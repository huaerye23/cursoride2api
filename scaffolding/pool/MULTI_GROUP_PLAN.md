# Multi-Group Model Pools — Plan

Status: **proposed**, awaiting decisions on the open questions in § 9.

Add support for **multiple model groups** in the RATLC pool, so a single
running stack can serve several Cursor models from disjoint channel
pools, with the per-request `model` field routing to the right group.
Today every channel is locked to the single `POOL_MODEL` env var at
pool-manager boot.

---

## 1. Goal

One `ratlc up` invocation should be able to host:

- A **default group** sized by `POOL_SIZE` of `POOL_MODEL` (today's
  behavior, unchanged for existing setups).
- Zero or more **named groups**, each pinned to a different Cursor
  model, with its own target size.

Per-request routing rule:

- If `req.body.model` exactly matches a known group → serve from that
  group.
- Otherwise → serve from the default group (annotated as a fallback).

Adding / removing / resizing groups must be possible **at runtime**
without losing warm channels in other groups.

---

## 2. Why now

`claude-code` lets you pick a model per session. Different models have
different cost/latency profiles (Opus thinking-max for hard reasoning,
Haiku for cheap throughput, Codex for code edits). Today you'd need to
run a separate `ratlc up` on a separate port for each — multiple
pool-managers, multiple ports, separate observability. One stack
serving many models is dramatically simpler.

---

## 3. Current shape (what we're changing)

| Layer | Today | After |
|---|---|---|
| `pool-manager.mjs` | One flat `channels[]`, every worker forked with `POOL_MODEL` | `groups: Map<modelId, { targetSize, channels[], openingInFlight }>` |
| `bridge-worker.mjs` | Receives model via env at fork | **Unchanged** — model is already a per-worker env var |
| `api-server.mjs` | Ignores `req.body.model`, asks pool for any LRU channel | Extracts `body.model`, passes it on the socket op |
| Socket protocol (`/tmp/ratlc-pool.sock`) | `{ type: 'requestChannel', requestId }` | `{ type: 'requestChannel', requestId, model }` |
| `ratlc` CLI | `up`, `down`, `ramp`, `restart`, ... | + `add-group`, `remove-group`, `groups`; `ramp --group=X` |
| TUI | 4 views (`split`/`api`/`pool`/`status`) | + `groups` view, + `GROUP` column in status, + hotkeys |
| `/health`, `/metrics` | Flat counts | + per-group breakdown |

The lucky bit: `bridge-worker.mjs` is already model-agnostic at code
level — the model is just an env var consumed at fork. We don't touch
worker internals, only how the pool groups and picks them.

---

## 4. Detailed design

### 4.1 Pool-manager — groups as first-class

```js
// Internal data
groups = new Map();  // modelId -> Group
defaultModel = process.env.POOL_MODEL;

Group = {
  model: string,          // cursor model id, also the key
  isDefault: boolean,     // true for POOL_MODEL group
  targetSize: number,
  channels: Channel[],    // owned by this group
  openingInFlight: number,// shared budget? see below
};

Channel = {
  ...existing fields,
  group: string,          // model id of owning group
};
```

- Each `Channel` gains a `group` field.
- LRU pick, idle-ping, auto-respawn, ramp — all become **per-group**
  operations dispatched off the requested model.
- `POOL_CONCURRENT_OPENS` stays a **global** gate across all groups
  (single rate-limit budget against Cursor's `/Run`).

### 4.2 Config: bootstrap groups via `POOL_GROUPS`

```bash
POOL_MODEL=claude-opus-4-7-thinking-max-fast POOL_SIZE=10 \
POOL_GROUPS="claude-haiku-4-5:3,gpt-5-codex:2" \
POOL_BRIDGE_PROTOCOL=h1 POOL_TOOL_MODE=translate POOL_CONTEXT_MODE=full \
POOL_CONCURRENT_OPENS=5 \
  ./scaffolding/pool/ratlc up
```

Grammar: `POOL_GROUPS` is a comma-separated list of `model:size` pairs.
Parsed once at pool-manager boot. After boot, groups are mutated via
CLI/socket (see § 4.4) so the env-only path is just a convenience —
declarative bring-up of an initial group set.

Default group is always defined by `POOL_MODEL` / `POOL_SIZE`. Existing
single-group setups keep working unchanged.

### 4.3 Routing in api-server

```js
// On every POST /v1/messages:
const model = req.body.model;           // may be undefined
const op = { type: 'requestChannel', requestId, model };
const { channelId, servedModel, fallback, fallbackReason } = await poolRequest(op);

res.setHeader('x-ratlc-routed-to', servedModel);
res.setHeader('x-ratlc-channel', channelId);
res.setHeader('x-ratlc-fallback', fallback ? '1' : '0');
if (fallback) res.setHeader('x-ratlc-fallback-reason', fallbackReason);
```

Pool-manager logic on `requestChannel`:

1. If `model` matches a known group → pick LRU channel from that group.
2. If group has zero `ready` channels but >=1 `opening` → **wait up to
   `POOL_GROUP_WAIT_MS` (default 5000ms)** then fall back (see § 9
   question 2).
3. If group is unknown → fall back to default group, log
   `routed-to-default model=<x> reason=unknown-model`, increment metric.

`body.model` in the response stays whatever the actual serving model
is, so OpenAI-SDK-style clients see truthful provenance. The
`x-ratlc-*` headers are the observability handle.

### 4.4 CLI additions

| Command | Behavior |
|---|---|
| `ratlc up [N]` | **Unchanged.** Sizes default group; brings up any groups in `POOL_GROUPS`. |
| `ratlc add-group <model> <size>` | **New.** IPC to pool-manager: register group, spawn `size` channels. Returns once accepted (channels still warming). |
| `ratlc remove-group <model>` | **New.** Drains (refuses new requests, lets in-flight finish), then kills channels and deletes the group. Refuses on default group. |
| `ratlc groups` | **New.** Prints per-group breakdown (one line per group). |
| `ratlc ramp ±N [--group=<model>]` | Adds `--group` flag, defaults to default group. |
| `ratlc status` | **Updated.** Adds `GROUP` column; header line shows group count + default. |
| `ratlc restart ch-N` | **Unchanged.** Channel ids stay globally monotonic. |
| `ratlc claude [--model X] [...]` | Adds `--model X` passthrough; sets `ANTHROPIC_MODEL=X` if not already set. |

### 4.5 Socket protocol additions

| Op | Payload | Response |
|---|---|---|
| `addGroup` | `{ type, model, targetSize }` | `{ ok, message }` |
| `removeGroup` | `{ type, model }` | `{ ok, message }` (refused if default) |
| `listGroups` | `{ type }` | `{ groups: [{ model, isDefault, target, ready, busy, opening, dead, rounds }] }` |
| `rampUp` / `rampDown` | now takes optional `{ group }` field | unchanged |
| `requestChannel` | now takes `{ model }` field | adds `servedModel`, `fallback`, `fallbackReason` |
| `status` | response gains `pool.groups[]` (same shape as `listGroups`) | (used by TUI) |

### 4.6 Channel lifecycle change: `draining`

Adding a state to support graceful `remove-group`:

```
spawning -> opening -> ready <-> busy -> (idle long enough) ping -> ready
                         |
                         v (group being removed)
                     draining -> killed
```

`draining` channels:
- refuse new `requestChannel` dispatches
- complete any in-flight request normally
- once their `currentRequestId` clears → killed
- a `draining` channel cannot transition back to `ready`

If the user prefers fire-and-forget remove, the state addition is
optional — see § 9 question 3.

---

## 5. TUI surface

### 5.1 New view: hotkey `5` -> groups

```
ratlc tui  10:21:14   views:  1:split  2:api  3:pool  4:status  [5:groups]
                      actions: [+]+1 [-]-1 [a] add [d] del [k] restart-stuck [:] cmd [q] quit
-----------------------------------------------------------------------------------------
GROUP                                       TARGET  READY  BUSY  OPEN  DEAD   ROUNDS
> claude-opus-4-7-thinking-max-fast (def)       10      8     1     1     0      247
  claude-haiku-4-5                                3      3     0     0     0       42
  gpt-5-codex                                     2      0     2     0     0       18

>> READY -- 11 channels across 3 groups (default has 8 ready)
```

- `>` is a cursor (move with `j`/`k`).
- Per-group hotkeys when focused: `+` / `-` ramp focused group;
  `d` removes group (refused on default, confirmation in `cmdResult`);
  `Enter` drills into a channels-of-this-group view.
- Global new hotkey: `a` → opens `:add-group ` pre-filled in the
  command bar (sets `cmdMode=true; cmdBuffer='add-group '`).

### 5.2 Updated `4:status` view — `GROUP` column

```
Pool  13/15 channels  ready=11  busy=2  opening=0  dead=0   pending=0  tool_use_held=0
Groups: 3 (default=claude-opus-4-7-thinking-max-fast)   parallel-opens=5

CHANNEL    STATE   GROUP                              PID    ATTEMPTS  AGE   IDLE  ROUNDS  CURRENT
ch-0       ready   claude-opus-4-7-thinking-max-fast  12345  3         42m   3s    34      -
ch-1       busy    claude-opus-4-7-thinking-max-fast  12346  1         42m   -     28      req-a1b2
ch-7       ready   claude-haiku-4-5                   12399  1         12m   8s    14      -
ch-8       busy    gpt-5-codex                        12401  5         8m    -     12      req-c9d8
```

`GROUP` column truncated/padded same as others. Optionally
group-color the row left-border for faster eye-tracking (nice-to-have,
not v1).

### 5.3 `:`-command bar wiring

Extend `executeCommand()` in `ratlc.mjs` (around line 294) to recognize:

```
:add-group <model> <size>
:remove-group <model>
:groups
:ramp ±N [group=<model>]
```

All shell out to the CLI via the existing `spawnRatlc` helper, so the
`OK ... -- <tail>` feedback line gets populated automatically.

Update the `:help` line accordingly.

### 5.4 Live log annotations

`api-server.mjs` log lines gain group info:
```
[10:21:13] POST /v1/messages  model=claude-haiku-4-5     -> ch-12 (group=claude-haiku-4-5)
[10:21:11] POST /v1/messages  model=gpt-9000 (unknown)   -> ch-3  (group=default, FALLBACK)
[10:21:09] POST /v1/messages  model=claude-opus-4-7...   -> ch-7
```

So routing decisions are visible in `3:pool` and `2:api` views without
extra TUI work.

---

## 6. API contract

The wire stays Anthropic's `/v1/messages`. Only behavior change: the
`model` field in the request now picks the group.

### 6.1 Exact match

```bash
curl -s http://127.0.0.1:4242/v1/messages \
  -H 'content-type: application/json' \
  -H 'anthropic-version: 2023-06-01' \
  -d '{
    "model": "claude-haiku-4-5",
    "max_tokens": 1024,
    "messages": [{"role":"user","content":"hi"}]
  }' -i

HTTP/1.1 200 OK
x-ratlc-routed-to: claude-haiku-4-5
x-ratlc-channel: ch-7
x-ratlc-fallback: 0
```

### 6.2 Unknown model -> silent fallback

```bash
HTTP/1.1 200 OK
x-ratlc-routed-to: claude-opus-4-7-thinking-max-fast
x-ratlc-channel: ch-0
x-ratlc-fallback: 1
x-ratlc-fallback-reason: unknown-model
```

(Pending § 9 question 1 — could change to 404 if loud-fail preferred.)

### 6.3 From claude-code

```bash
ANTHROPIC_BASE_URL=http://127.0.0.1:4242 \
ANTHROPIC_API_KEY=ratlc-pool \
ANTHROPIC_MODEL=claude-haiku-4-5 \
  claude --dangerously-skip-permissions
```

Or via the wrapper (after CLI change):
```bash
ratlc claude --model claude-haiku-4-5
```

### 6.4 `/health` and `/metrics` get per-group breakdown

```bash
curl http://127.0.0.1:4242/health | jq
{
  "pool": {
    "readyCount": 11, "busyCount": 2, "actualSize": 13, "configuredSize": 15,
    "groups": [
      { "model": "claude-opus-4-7-thinking-max-fast", "default": true,
        "target": 10, "ready": 8, "busy": 1, "open": 1, "dead": 0 },
      { "model": "claude-haiku-4-5",
        "target": 3,  "ready": 3, "busy": 0, "open": 0, "dead": 0 },
      { "model": "gpt-5-codex",
        "target": 2,  "ready": 0, "busy": 2, "open": 0, "dead": 0 }
    ]
  }
}
```

Prometheus exposition gains `group` label:
```
ratlc_pool_channels{group="claude-haiku-4-5",state="ready"} 3
ratlc_requests_total{group="default",fallback="1"} 17
```

---

## 7. Behaviors that don't change

| Concern | Why no change |
|---|---|
| **Thinking continuity** (`POOL_REINJECT_THINKING`) | `convKey` already hashes in `modelId` (see `pool-manager.mjs` `deriveConversationKey`). Per-group buffers naturally segregated. |
| **`bridge-worker.mjs` internals** | Model is a fork-time env var. Workers don't need to know about groups. |
| **`src/cursor-agent*.js`** | Transport layer is per-channel — no awareness of group identity needed. |
| **Token pool** | All groups share `token.json`. See § 8. |
| **Channel ids** | Stay globally monotonic (`ch-0`, `ch-1`, ...). Channels are addressable across the whole pool — only their `group` field changes. |

---

## 8. Explicitly out of scope

- **Per-group token isolation.** All groups share `token.json` and
  therefore share account quota / rate-limit pressure. If you want
  group A pinned to account-1 and group B to account-2, that's a
  separate (larger) change — token-pool partitioning. Flagging now so
  this PR stays focused.
- **Cross-group failover.** If group A is exhausted, we don't try
  group B; we just fall back to default. Smart failover is a v2 idea.
- **Per-group `POOL_CONTEXT_MODE` / `POOL_TOOL_MODE`.** All groups
  inherit pool-manager-level settings. Per-group overrides could be
  added later via `add-group --context-mode=full`, but v1 ties these
  to the pool.
- **Model alias / aliases.** `gpt-4o -> claude-haiku-4-5` style mapping.
  Out of scope; the OpenAI-style mapping in `src/config.js` is for the
  legacy `server.js` path, not the pool.

---

## 9. Open questions (need your decision)

1. **Fallback announcement style.**
   - (a) Silent `x-ratlc-fallback: 1` header, 200 OK. **(default)**
   - (b) Return `404 unknown model "X"` to fail loudly.
   - (c) Configurable: `POOL_UNKNOWN_MODEL=fallback|404`, default `fallback`.

2. **Group has 0 ready channels (still warming, or all busy).**
   - (a) Wait up to `POOL_GROUP_WAIT_MS` (default 5000ms) for ready,
     then fall back to default. **(default)**
   - (b) Fall back to default immediately.
   - (c) Wait indefinitely (like single-group does today).

3. **`ratlc remove-group <model>` semantics.**
   - (a) Drain: introduce `draining` state, let in-flight requests
     finish, refuse new ones, kill when idle. **(default)**
   - (b) Fire-and-forget: kill immediately, in-flight requests error
     out. Simpler — no state machine change.

4. **`ratlc claude --model X` for an unknown group.**
   - (a) Auto-create group with default size 2, then exec claude.
   - (b) Refuse and print "no group for X — run `ratlc add-group X N`
     first." **(default — keeps the user in control of resource cost)**
   - (c) Run anyway; fall back to default group per § 6.2 semantics.

5. **Group mutation surface.**
   - (a) Env-only (`POOL_GROUPS` at boot, no runtime change).
   - (b) CLI-only (no `POOL_GROUPS`, must `add-group` after `up`).
   - (c) Both. **(default — env declares initial set, CLI mutates)**

Default picks if no opinion: **1a, 2a, 3a, 4b, 5c**.

---

## 10. Phasing — what ships when

### v1 (this PR — minimum viable multi-group)

- Pool-manager: groups data structure, per-group operations.
- Socket: new ops + extended `requestChannel`.
- api-server: model extraction + routing + headers.
- CLI: `add-group`, `remove-group`, `groups`, `--group` flag on
  `ramp`, `--model` passthrough on `claude`.
- TUI: `GROUP` column in `4:status` view; new commands accepted in
  the `:`-bar; `:help` updated.
- `/health` + `/metrics` per-group breakdown.

This is enough to: run the stack with multiple groups, route
`claude-code` to the right one, observe what's happening.

### v1.5 (follow-up, after dogfooding)

- TUI `5:groups` dedicated view with `j`/`k` cursor and per-group
  hotkeys (`+`, `-`, `d`, `a`, `Enter`).
- `:add-group` pre-fill via `a` hotkey.
- Group-color row left-borders in the channel list.

### v2 (separate planning doc)

- Per-group token-pool partitioning.
- Cross-group failover policies.
- Per-group context / tool mode overrides.

---

## 11. Test plan

1. **Unit**
   - `pool-manager`: group add/remove/ramp; default group cannot be
     removed; channel `group` field set correctly on fork.
   - api-server: `model` extraction; unknown-model fallback emits
     correct headers + log; missing-model uses default.
2. **Integration** (new scripts in `scaffolding/pool/`)
   - `multi-group-routing-test.mjs` — boot 2 groups, fire 10 requests
     across both, assert each landed on the right group.
   - `add-remove-group-test.mjs` — runtime mutation: add, ramp, remove,
     status snapshot at each step.
3. **Existing tests must still pass**
   - `multi-turn-test.mjs` (single-group default-model conversation).
   - `parallel-tools-test.mjs` (parallel tool_use).
   - `reinject-thinking-test.mjs` (convKey scoping is unchanged).

---

## 12. Affected files (estimate)

| File | Lines changed (est) |
|---|---|
| `scaffolding/pool/pool-manager.mjs` | ~250 (largest delta — group data structure + per-group ops) |
| `scaffolding/pool/api-server.mjs` | ~60 (extract model, set headers, per-group logs) |
| `scaffolding/pool/ratlc.mjs` | ~150 (new subcommands, `GROUP` column, `--group` flag, `:`-bar) |
| `scaffolding/pool/IPC.md` | ~40 (new socket ops documented) |
| `scaffolding/pool/README.md` | ~60 (`POOL_GROUPS`, group CLI, routing section) |
| `scaffolding/pool/bridge-worker.mjs` | 0 (no change) |
| `src/cursor-agent*.js` | 0 (no change) |

New files:
- `scaffolding/pool/multi-group-routing-test.mjs` (~150)
- `scaffolding/pool/add-remove-group-test.mjs` (~150)

---

## 13. Decision log

The five open questions in § 9 were answered with the **default**
picks. Recorded here verbatim so future readers don't have to chase the
PR description for the rationale.

| Q | Decision | Why |
|---|---|---|
| 1. Fallback for unknown model | **(1a) Silent `x-ratlc-fallback: 1` header, 200 OK.** | claude-code clients keep working when the user switches models mid-session even if no group exists yet. Loud-fail (404) would break the session; the `x-ratlc-fallback-reason` header is enough for ops visibility. |
| 2. Group has 0 ready channels | **(2a) Wait `POOL_GROUP_WAIT_MS` (default 5000ms), then fall back to default.** | A group that's mid-warm shouldn't 503; a group that's permanently saturated shouldn't hang the request indefinitely. The 5s wait gives in-flight opens a fair chance, then prefers serving from default over blocking. |
| 3. `remove-group` semantics | **(3a) Drain — introduce `draining` state.** | In-flight requests finish without errors; new requests fall back to default with `fallbackReason=group-draining`. Avoids a thundering herd of 503s when an operator decommissions a group. |
| 4. `ratlc claude --model X` unknown | **(4b) Refuse with helpful hint** (`no group for X — run \`ratlc add-group X N\` first`). | Auto-creating groups would silently burn quota; falling back to default would make `--model` a no-op. Better to keep the operator in control of resource cost. |
| 5. Group mutation surface | **(5c) Both — `POOL_GROUPS` env at boot + CLI ops at runtime.** | Declarative bring-up via env is convenient for service managers (systemd, launchd, ratlc up). CLI ops are necessary for runtime adjustment without restarting (and losing warm channels). |

### Additional decisions made during implementation

- **Mock-channel test harness.** Added `POOL_TEST_MOCK_CHANNELS=1` +
  `mock-worker.mjs` so the routing and add-remove-group tests can
  exercise pool-manager logic without paying the Cursor `/Run` retry
  lottery. The env flag is undocumented in README (test-internal).
- **Bootstrapping `POOL_GROUPS` entries that name the default model**
  are folded into the default group (their size adds to `POOL_SIZE`).
  Same for entries that name an already-listed group — sizes sum. This
  is friendlier than rejecting `POOL_GROUPS="POOL_MODEL_NAME:N"` as an
  accidental misuse.
- **`route_decision` is the first event on the wire.** The api-server
  needs `servedModel` + `channelId` + `fallback` + `fallbackReason`
  BEFORE it writes the HTTP response headers (to stamp the `x-ratlc-*`
  fields). The pool-manager emits `route_decision` synchronously inside
  `routeRequest()` before forwarding the actual `text` to the worker,
  so the api-server gets it as the first message on the request stream.
- **`send_tool_result(s)` ignores the request's `model` field.** The
  routing is forced to the channel that emitted the original
  `tool_use` (sticky tool-round-trip invariant). The `model` field on
  these requests is informational only — the response's
  `x-ratlc-routed-to` header reflects the channel's group, not the
  request's `model`.
