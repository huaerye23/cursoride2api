# `step_completed` missing from Cursor's stream — and the watchdog workaround

Observed 2026-05-15 in the ratlc-pool against `*-thinking-fast` Cursor model
variants. This doc captures the problem, the workaround currently in
production, and what investigation would actually fix the root cause.

## Background — ratlc-pool architecture in one paragraph

`ratlc-pool` is an Anthropic-API-compatible proxy in front of Cursor's API.
It maintains a pool of N "channels," each holding a long-lived bidi stream
to Cursor and acting as a session for claude-code requests. Three processes:

- **`pool-manager.mjs`** — supervisor; forks N `bridge-worker.mjs` child
  processes
- **`bridge-worker.mjs`** — one per channel; owns one `startConversation()`
  instance from `cursor-agent.js` (or `cursor-agent-h1.js`) and bridges
  between Cursor's protocol and an IPC contract to pool-manager
- **`api-server.mjs`** — speaks Anthropic Messages API to claude-code;
  translates incoming requests through the pool socket; emits
  Anthropic-shaped SSE

## The protocol mismatch we have to bridge

Anthropic's Messages API uses `stop_reason: "tool_use"` to mean "the
assistant's response is complete and is waiting for tool results — execute
these tools and send the results back as the next user message."

Cursor's internal protocol uses a richer event stream over a long-lived BiDi
connection. The relevant signal is `interactionUpdate.stepCompleted` — fired
when the model has finished a "step" (which may include multiple parallel
tool calls) and is paused awaiting tool results. **This is the deterministic
boundary we need to convert into Anthropic's `stop_reason: "tool_use"`.**

## The bug

`cursor-agent.js` and `cursor-agent-h1.js` both have handlers for
`iuCase === 'stepCompleted'`:

```js
// src/cursor-agent-h1.js:615
if (iuCase === 'stepCompleted') {
  try { currentCallbacks.onStepCompleted && currentCallbacks.onStepCompleted(); }
  catch (e) { /* ... */ }
}
```

These handlers wire up to the `onStepCompleted` callback that bridge-worker
forwards to pool-manager → api-server, where the api-server uses it to
finalize the SSE response.

**Empirically observed 2026-05-15: zero `stepCompleted` events arrive from
Cursor across the `*-thinking-fast` model variants in production use.** Grep
across many turns:

```
step_completed firings in pool log:                       0
step_completed firings in api-server finalize handler:    0
watchdog firings as substitute:                         100%
```

So the deterministic signal we designed around simply doesn't exist on this
code path. We're flying blind on step boundaries.

## The workaround currently in production

`api-server.mjs` arms a "watchdog" timer after every `tool_use` event:

```js
// scaffolding/pool/api-server.mjs — armToolUseFinalizer
toolUseFinishTimer = setTimeout(() => {
  log('finalize tool_use turn (WATCHDOG @1000ms — step_completed never arrived)');
  stopReason = 'tool_use';
  finishMessage();
}, 1000);  // POOL_TOOL_USE_WATCHDOG_MS
```

Each new `tool_use` event resets the timer. If 1000ms passes with no new
tool_use and no `step_completed`, we **guess** the model is done emitting and
finalize. Default 1000ms because observed inter-tool_use gaps within a step
are 150–480ms in practice.

## Why this is risky

If the model takes >1000ms between tool_uses within a single step (e.g., a
thinking model deliberating on a complex problem), the watchdog fires
mid-step. The api-server sends `stop_reason="tool_use"` to claude-code with
only the early tool_uses. claude-code executes those, sends results. The
model then emits the LATE tool_uses, which:

1. Reach bridge-worker → stored in `pendingMcpInfo`
2. Forwarded to api-server as `tool_use` events
3. Dropped silently because `done === true` already

The model now sits waiting forever for `tool_result`s for those late
tool_uses that claude-code never knew existed. **Channel stuck busy.**
Mitigated by the busy-watchdog we shipped (commit `d1c5eee`): any channel
idle in `busy` state for 240s gets SIGTERM'd and respawned by the pool. So
the bounded damage is: 1 failed request + 1 channel recycle.

A reproducer was hit during this session — model emitted 4 parallel tool_uses
across ~10s with the then-active 250ms watchdog. Timer fired after the first
2 emissions, the remaining 2 orphaned, channel sat busy 113s+ until the
busy-watchdog cleaned it up.

## What we don't know — the actual question to answer

Why is `interactionUpdate.stepCompleted` not arriving? Three hypotheses,
ranked by prior:

1. **Cursor's `*-thinking-fast` variants don't emit `stepCompleted` over
   this stream at all.** Maybe they use a different envelope for step
   boundaries (`thinkingDelta` or some other case in the `interactionUpdate`
   oneof). High prior — these "fast" variants likely have a custom path that
   bypasses the legacy step semantics.
2. **The vendored proto schema in `src/proto/agent_pb.mjs` is out of date.**
   If Cursor renumbered the `interactionUpdate.stepCompleted` proto field,
   our decoder would silently drop it (falls through the `iuCase` switch
   with no log). Worth checking against fresh proto from Cursor's IDE.
3. **A bug in the decoder switch.** Lower prior — code is short and
   unchanged.

## How to investigate

Spawn a single channel with `CURSOR_AGENT_DEBUG=1`, send a request that
triggers a tool_use, and grep for what `interactionUpdate` cases actually
arrive:

```bash
ratlc down
CURSOR_AGENT_DEBUG=1 POOL_BRIDGE_PROTOCOL=h1 POOL_TOOL_MODE=translate \
  POOL_MODEL=claude-opus-4-7-thinking-max-fast \
  ratlc up 1
# wait for ready, send a tool-using request through the proxy
grep "interactionUpdate" /tmp/ratlc-pool.log | sort | uniq -c
```

- If `stepCompleted` shows up there: bug in our handler (low effort fix).
- If only other cases show up: Cursor doesn't emit it for these models, and
  the watchdog architecture is fundamentally correct, just needs tuning
  per-model (e.g., longer `POOL_TOOL_USE_WATCHDOG_MS` for thinking variants
  that take longer between tool_uses on complex prompts).

## Comparison of watchdog values

| Watchdog value | Per-turn latency | Orphan bug risk | Channel recycle freq |
|---:|---|---|---|
| 250ms (original) | imperceptible | high — fires within 250ms of any gap | frequent |
| **1000ms (current)** | **~1s per turn** | **low** — needs >1s mid-step gap | **rare** |
| 5000ms | ~5s per turn | very low | very rare |
| 30000ms (first attempted fix) | 30s per turn | essentially zero | never |
| ∞ (no watchdog) | hangs forever | zero | never (but everything stuck) |

## Relevant files / commits

- `scaffolding/pool/api-server.mjs` — watchdog logic
- `src/cursor-agent-h1.js:615`, `src/cursor-agent.js:1536` — where
  `stepCompleted` would fire
- `scaffolding/pool/bridge-worker.mjs:282` — `onStepCompleted` forwarding
  callback
- `cd34ecf` — initial 30s watchdog (broken-slow, reverted)
- `2a004f2` — current 1s watchdog
- `d1c5eee` — busy-watchdog safety net (240s) — bounds the worst case
