# RATLC pool — user guide

A four-process stack that serves the Anthropic Messages API by holding a
**pool of pre-warmed Cursor agent streams**. Pay the probabilistic
`unpaid_invoice` retry lottery once per channel; reap fast responses for
the rest of each channel's lifetime.

> **Relationship to `server.js` (repo root):** completely separate. The
> root `server.js` is the *original* per-request proxy and uses features
> like `CURSOR_REINJECT_THINKING`. The RATLC pool (this directory) is a
> different code path that maintains warm conversations. They share
> protobuf and HTTP helpers via `src/cursor-agent*.js` but the rest of
> the stack is independent.

## Processes

| Process | What it does | Restart cost |
|---|---|---|
| **`pool-manager.mjs`** | Forks N `bridge-worker` children, maintains the pool, pings idle channels every 20 min, auto-respawns dead ones. Listens on `/tmp/ratlc-pool.sock`. | High — restarting loses all warm channels |
| **`bridge-worker.mjs`** *(child)* | Owns one Cursor `RunSSE` stream + matching `BidiAppend` POSTs. Managed by the pool. | Auto-respawn |
| **`api-server.mjs`** | HTTP `:4242` serving `/v1/messages` (Anthropic format). Stateless. | **Free** — restart anytime, pool stays up |
| **`ratlc`** (unified CLI) | Single entry point: `up`, `down`, `status`, `watch`, `tui`, `ramp`, `restart`, `claude`, `tail`, `metrics`, `logs` | n/a |

## Recommended launch (for claude-code use)

```bash
POOL_BRIDGE_PROTOCOL=h1 \
POOL_TOOL_MODE=translate \
POOL_CONTEXT_MODE=full \
POOL_CONCURRENT_OPENS=5 \
  ./scaffolding/pool/ratlc up 10
```

Then in another shell:

```bash
./scaffolding/pool/ratlc claude
# or manually:
ANTHROPIC_BASE_URL=http://127.0.0.1:4242 \
  claude --dangerously-skip-permissions --effort max
```

The first launch takes 1–10 min (probabilistic gate). Subsequent restarts
are faster as long as you didn't kill `pool-manager.mjs`.

## Configuration (env vars)

All settings are env vars on the pool-manager (it forwards everything to
the children + api-server).

| Var | Values | Default | What it controls |
|---|---|---|---|
| `POOL_BRIDGE_PROTOCOL` | `h1` \| `h2` | `h2` | HTTP version of the Cursor bridge. **`h1` strongly recommended** — bypasses the per-account hard rate limit on `/Run` that bites HTTP/2 under concurrency. See `H1_RESULTS.md`. |
| `POOL_TOOL_MODE` | `translate` \| `contract` | `contract` | `translate` (recommended for claude-code): bridge auto-injects Cursor's native tools and we translate them to Anthropic names. `contract`: pool's tool list is locked to the first request's `tools` field. |
| `POOL_CONTEXT_MODE` | `full` \| `last` | `last` | How multi-turn conversations are forwarded. **`full` strongly recommended for claude-code.** See [§ Context modes](#context-modes-fullvslast) below. |
| `POOL_CONCURRENT_OPENS` | `1`–`5` | `1` | How many channels open in parallel. `1` is safe but slow; `5` is faster but more rate-limit pressure. **At `>=5`, H2 trips the per-account rate limit**; H1 is fine. |
| `POOL_SIZE` | integer | `1` (cli `up N` overrides) | Target channel count. `./scaffolding/pool/ratlc up N` is the easy way. |
| `POOL_MODEL` | model id | `claude-opus-4-7-thinking-max-fast` | Which Cursor model to drive. |
| `POOL_REINJECT_THINKING` | `0` \| `1` | `0` | Captures the model's `thinking_delta` per `convKey`; on the next turn for the same conversation, prepends `<thinking>…</thinking>` text into the outbound prompt. Pool-side symmetry with `server.js`'s `CURSOR_REINJECT_THINKING`. See [§ Thinking continuity](#thinking-continuity) below. |
| `POOL_REINJECT_THINKING_MAX_BYTES_PER_TURN` | int | `4096` | Cap on captured bytes per assistant turn (truncates further deltas in the same turn). Matches server.js's default. |
| `POOL_REINJECT_THINKING_MAX_TURNS` | int | `5` | Number of past assistant turns kept per `convKey`; FIFO-evicts older. |
| `POOL_REINJECT_THINKING_DEBUG` | `1` | unset | Exposes `/v1/_debug/thinking_buffer` and `/v1/_debug/render` for buffer inspection. Off in normal operation. |
| `CURSOR_AGENT_DEBUG` | `1` | unset | Per-line wire debug from cursor-agent (verbose) |
| `CURSOR_LOG_SERVER_MSG` | `1` | unset | Log every `AgentServerMessage` case received from Cursor |
| `CURSOR_LOG_NATIVE_EXEC` | `1` | unset | Log every native exec passthrough event |
| `LOG_REQUEST_TOOLS` | `1` | unset | api-server logs incoming tool list per request |
| `LOG_REQUEST_BODY` | n/a | n/a | Body summary (last-message role + content shape) is always on. |

## Context modes (`full` vs `last`)

This is the most important setting for claude-code multi-turn coherence.
Pool channels are picked **least-recently-used** for each new POST, so a
multi-turn conversation can hop between channels — which means model
context handling depends on this flag.

### `last` (default, backward-compatible)

- Only the **last user message text** is forwarded to the bridge.
- Channels accumulate per-conversation state **server-side** inside
  Cursor's model context.
- Multi-turn coherence requires every turn of one conversation to land
  on the **same channel** — but LRU rotation breaks this.
- ✅ Cheap (no token re-send overhead).
- ❌ Cross-channel drift on multi-turn: turn 2 lands on a different
  channel that has no memory of turn 1.

### `full` (recommended for claude-code)

- The **entire `messages[]` history** is rendered into one self-contained
  prompt and fed via `bajie_yield` on every POST.
- Channels are stateless carriers — each `bajie_yield` result is a
  complete request.
- ✅ Multi-turn coherence preserved across channel rotation.
- ❌ Quadratic token cost as conversations grow (each turn re-sends the
  full prior history). Fine for typical claude-code sessions (10-30
  turns); gets expensive at 100+.

## Thinking continuity

Enable with `POOL_REINJECT_THINKING=1`. Captures Cursor's `thinking_delta`
events into a per-`convKey` server-side buffer, then renders them as
`<thinking>…</thinking>` text into the next turn's outbound prompt for
that same conversation. Pool-side counterpart of `server.js`'s
`CURSOR_REINJECT_THINKING`; same mechanism, different process boundary.

### Why this exists

The model's *own* prior reasoning ordinarily lives in the server-side
Cursor model context for the open channel. Two situations break that:

- **`full` mode + LRU rotation** — turn 2 of a conversation may land on a
  different channel from turn 1. The new channel has no memory of what
  turn 1's model thought.
- **`last` mode + same-channel-different-conversation** — across truly
  unrelated conversations served by the same channel, prior thinking is
  noise rather than help (this case already works without reinjection;
  no change).

Either path, the captured-then-rendered `<thinking>…</thinking>` is the
only way to give the model a useful reasoning carry-over within the
existing wire constraints (Cursor's transport strips signed extended-
thinking blocks regardless of source — see `DEVLOG.md` "Proxy-side
thinking re-injection" for that constraint).

### How conversations are identified

This depends on the convKey identity fix that landed earlier in this
project: `extractClientSessionId(req)` pulls a stable per-conversation
UUID from either the `x-claude-code-session-id` header or
`body.metadata.user_id`'s `session_id` field. `deriveConversationKey`
hashes only `(modelId, sessionId)` when that UUID is present, producing
a `conv-v2:` key with zero collision risk across distinct conversations
even when prompts and tools are identical. Non-claude-code callers
fall back to the legacy circumstantial hash.

### Trade-off

- ✅ Survives both LRU rotation in `full` mode and channel reuse across
  conversations in `last` mode.
- ✅ Bounded: `MAX_BYTES_PER_TURN × MAX_TURNS` = 4 KB × 5 = 20 KB cap on
  injected thinking per `convKey`.
- ✅ Default off — opt-in symmetry with `CURSOR_REINJECT_THINKING`.
- ❌ Text-form continuity, NOT native signed extended-thinking. The model
  sees prior reasoning as inline `<thinking>` tags, treats it as
  reference context — does not run it through extended-thinking re-
  validation logic on Cursor's side. Same approximation `server.js`
  ships, same caveat.
- ❌ Some extra prompt bytes per continuation; bounded by the env caps.

### Verifying it works

```bash
node scaffolding/pool/thinking-buffer-test.mjs      # 34 unit assertions
node scaffolding/pool/reinject-thinking-test.mjs    # 2-turn E2E
```

The E2E test asserts on the **outbound prompt to the bridge** containing
a `<thinking>` block on turn 2 — model-output coherence is a separate
concern verified by `multi-turn-test.mjs`.

If `POOL_REINJECT_THINKING_DEBUG=1`, the api-server exposes:

| Endpoint | What it returns |
|---|---|
| `GET /v1/_debug/thinking_buffer` | Live buffer contents per convKey |
| `POST /v1/_debug/render` | Render a fake POST body through the same pipeline to inspect what would be sent |

### Verifying it works

```bash
# Single-turn round-trip (both modes)
node scaffolding/pool/tool-roundtrip-test.mjs

# Multi-turn coherence — "I have three apples" / "How many do I have?"
node scaffolding/pool/multi-turn-test.mjs

# Parallel tools — model calls 2 Bash tools at once
node scaffolding/pool/parallel-tools-test.mjs
```

All three should PASS in the recommended config (`h1 + translate + full`).

## What works (current state, 2026-05-14)

- ✅ **HTTP/1.1 transport** via `BidiAppend` + `RunSSE` pair. Bypasses
  the per-account rate-limit ceiling that capped H2 at ~2-3 channels.
- ✅ **Stateless full-context forwarding** (`POOL_CONTEXT_MODE=full`).
- ✅ **Parallel tool calls** — model can fire N tool_uses in one
  response; all N round-trip back correctly.
- ✅ **Anthropic SSE wire compliance** — claude-code parses our
  responses correctly (cache_creation_input_tokens et al., proper
  model id, empty `input_json_delta` preamble).
- ✅ **`ExecClientControlMessage(streamClose)`** sent after every tool
  result — matches Cursor IDE's bundle pattern.
- ✅ **Thinking continuity** (`POOL_REINJECT_THINKING=1`, opt-in) —
  captured per `convKey` (using `x-claude-code-session-id` for ironclad
  attribution), reinjected as `<thinking>…</thinking>` text on
  subsequent turns. Mirrors `server.js`'s `CURSOR_REINJECT_THINKING`.

See `H1_RESULTS.md` for the scale-test results (10 channels @ H1: 0
hard rate-limit hits vs 108 on H2). See `TOOL_USE_HANG_FINDINGS.md`
for the diagnosis trail that found the streamClose requirement.

## Bring it up — basics

```bash
# Default (size=1, contract mode, h2) — minimal but slow
./scaffolding/pool/ratlc up

# Recommended for claude-code (size=10, h1, translate, full context)
POOL_BRIDGE_PROTOCOL=h1 POOL_TOOL_MODE=translate POOL_CONTEXT_MODE=full \
  POOL_CONCURRENT_OPENS=5 ./scaffolding/pool/ratlc up 10
```

## Watch it warm up

```bash
./scaffolding/pool/ratlc watch       # status refresh every 2s
./scaffolding/pool/ratlc tui         # split-screen TUI with logs + status
./scaffolding/pool/ratlc status      # one-shot snapshot
```

Channels transition `spawning` → `opening` → `ready`. Many will retry
30-300 times each before clearing the probabilistic `unpaid_invoice`
gate. Expected.

## Use claude-code against it

```bash
./scaffolding/pool/ratlc claude
# auto-waits for ready≥1, then launches claude with the right env vars
```

Or manually:

```bash
ANTHROPIC_BASE_URL=http://127.0.0.1:4242 \
ANTHROPIC_API_KEY=ratlc-pool \
  claude --dangerously-skip-permissions --effort max
```

`./scaffolding/pool/ratlc claude` is the easier path — it adds the env
vars + waits for the pool to be ready before exec'ing claude.

## Scale at runtime

```bash
./scaffolding/pool/ratlc ramp +3        # add 3 channels
./scaffolding/pool/ratlc ramp -2        # remove 2 (idle first)
./scaffolding/pool/ratlc restart ch-0   # respawn one channel
```

## Restart the API server without losing the pool

```bash
pkill -f "node.*api-server.mjs"
LOG_REQUEST_TOOLS=1 nohup node scaffolding/pool/api-server.mjs >> /tmp/ratlc-api.log 2>&1 &
disown
```

The api-server holds no Cursor state. The pool keeps its warm channels.
Useful for picking up new env vars (e.g. switching `POOL_CONTEXT_MODE`)
without re-paying the probabilistic gate.

## Stop everything

```bash
./scaffolding/pool/ratlc down
```

## Logs

| File | What's in it |
|---|---|
| `/tmp/ratlc-pool.log` | Pool manager + worker stdout/stderr (state transitions, BidiAppend results, tool routing, native exec passthrough) |
| `/tmp/ratlc-api.log` | API server (HTTP requests, per-POST body summary, pool socket events) |

Live-tail both: `./scaffolding/pool/ratlc tail`

## Observability surface

| Layer | Logged automatically | Opt-in (env var) |
|---|---|---|
| api-server | POST URL, tool list, body summary (`role + content shape`), pool dispatch (`mode=full\|last`, requestId, bytes) | — |
| pool-manager | Channel state transitions, `route send_tool_result` outcomes, attempt counters | — |
| bridge-worker | IPC arrival (`type, requestId, state, pendingYield/pendingMcp`), BEFORE/AFTER around `bridge.sendToolResult` | — |
| cursor-agent-h1 | `sendToolResult` entry (id, execId, kind, size), BidiAppend OK/FAIL/EXCEPTION | `CURSOR_LOG_SERVER_MSG=1` for receive-side msgCase trace |
| metrics | Prometheus exposition at `:4242/metrics` | — |

## Health behaviors

- **Idle ping every 20 min** — any `ready` channel idle that long gets a
  tiny health-check round-trip. Reset on success; channel killed on
  failure.
- **Heartbeat every 30s** — workers report state to manager.
- **Auto-respawn on exit** — worker exit triggers replacement to maintain
  `currentTargetSize`.
- **Rate-limit handling** — soft "please wait" returns retry with
  exponential backoff (5s → 60s). Hard `ERROR_PRO_USER_RATE_LIMIT`
  doesn't fire on H1 path.

## Status fields

```
CHANNEL   ch-N         logical id, monotonically increasing
STATE     spawning     forked, not yet started open
          opening      running the probabilistic gate retry lottery
          ready        parked in bajie_yield, awaiting request
          busy         serving a request OR holding a tool_use
          dead         fatal error, will be respawned
PID                    OS pid of the worker process
ATTEMPTS               how many retry attempts the lottery has taken
AGE                    time since the channel opened (first lottery win)
IDLE                   time since the last activity
ROUNDS                 successful user-message→yield cycles served
CURRENT                request id currently in flight on this channel
ERROR                  most recent error message (if any)
```

## Common diagnostic paths

| Symptom | Where to look |
|---|---|
| claude-code hangs mid-conversation | `/tmp/ratlc-api.log` for "→ tool_use to client" then check `/tmp/ratlc-pool.log` for the matching `sendToolResult` and `BidiAppend OK seqno=…` |
| "API returned an empty or malformed response" | Likely parallel-tool-call bug if the model fires multiple in one turn. We support this now; if it surfaces, check `pendingMcpInfo` map state |
| Channel stuck `opening` forever | Probabilistic gate or hard rate-limit — log entries `stream-summary-h1 code=fail reason="…"` reveal which |
| Channel stuck `busy` with high `IDLE` | The bridge sent a tool result but Cursor's model isn't resuming. Likely the `streamClose` issue if pre-`72c60fd`, otherwise check the receive-side msgCase trace |

See `TOOL_USE_HANG_FINDINGS.md`, `H1_RESULTS.md`, and `FOCUS.md` for the
underlying RE work.
