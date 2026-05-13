# IPC contracts for the RATLC pool stack

Three processes, two protocols.

```
┌─────────────────────────────────────┐
│  Pool Manager (parent process)      │           PROTOCOL B
│  - /tmp/ratlc-pool.sock (listen)    │ ←──────── Unix socket, line-delimited
│  - forks N bridge-worker children   │           JSON
│                                     │              │
│   ┌──────────┐  ┌──────────┐  ...   │              │
│   │worker 0  │  │worker 1  │        │              │
│   └──────────┘  └──────────┘        │              │
│       ↑              ↑              │              │
│       └── PROTOCOL A ┘              │              │
│           process.send                              │
└─────────────────────────────────────┘              │
                                                     │
                                  ┌──────────────────┴────────────────┐
                                  │                                   │
                          ┌───────┴────────┐               ┌──────────┴──────────┐
                          │  api-server    │               │  ratlc-ctl          │
                          │  HTTP :4242    │               │  (CLI)              │
                          └────────────────┘               └─────────────────────┘
```

## Protocol A — Pool Manager ↔ Bridge Worker (via `process.send` IPC, JSON objects)

### Manager → Worker

```jsonc
{ "type": "open", "model": "claude-opus-4-7-thinking-max-fast",
  "tools": [{"name":"Read", "input_schema":{...}}, ...], "system": "<caller system>" }

{ "type": "send_user_message", "requestId": "req-abc", "text": "What is 2+2?" }

{ "type": "send_tool_result", "requestId": "req-abc", "execId": "<from previous tool_use>", "content": "..." }

{ "type": "ping", "requestId": "ping-1234" }       // a no-op user message just for keepalive

{ "type": "shutdown" }                              // graceful close
```

### Worker → Manager

```jsonc
// State updates — sent on every transition.
{ "type": "state", "state": "spawning"|"opening"|"ready"|"busy"|"dead",
  "openAttempts": 231, "openedAt": 1730000000000, "lastActivityAt": 1730000000000,
  "model": "...", "error": "..." }

// Streaming response events — keyed to requestId from the matching send_*.
{ "type": "text_delta", "requestId": "req-abc", "text": "Hello" }

{ "type": "tool_use", "requestId": "req-abc", "execId": "<cursor exec id>",
  "name": "Read", "args": {"file_path":"/foo"} }

{ "type": "yield", "requestId": "req-abc" }        // round complete (model called bajie_yield)

{ "type": "error", "requestId": "req-abc"|null, "message": "..." }

// Heartbeat (sent every 30s so manager can detect hung workers)
{ "type": "heartbeat", "now": 1730000000000 }
```

### Worker lifecycle

```
spawning → opening (retry lottery) → ready → busy → ready → ... → dead
```

- `spawning` — process forked, hasn't yet called `startConversation`
- `opening` — running the retry-until-success lottery
- `ready` — bridge open, parked in `bajie_yield`, awaiting user message
- `busy` — currently servicing a request OR holding a tool_use waiting for tool_result
- `dead` — fatal error; process should exit so manager can respawn

## Protocol B — Pool Manager ↔ Clients (api-server, ratlc-ctl) over Unix socket

Listen path: `/tmp/ratlc-pool.sock` (override with `POOL_SOCK`).

Wire format: newline-delimited JSON. One line = one message.

### Client → Manager — request types

```jsonc
// Open a request — manager picks a ready channel.
// For new conversation turn:
{ "type": "request", "requestId": "req-abc",
  "action": "send_user_message", "text": "...",
  "system": "<caller system>", "tools": [...] }   // first request's tools become the pool's tools

// For tool_result follow-up:
{ "type": "request", "requestId": "req-abc",
  "action": "send_tool_result",
  "anthropic_tool_use_id": "toolu_xyz", "content": "..." }

// Snapshot pool state.
{ "type": "status" }

// Scale pool.
{ "type": "ramp_up", "count": 3 }
{ "type": "ramp_down", "count": 2 }

// Force-restart a specific channel.
{ "type": "restart_channel", "channelId": "ch-3" }

// Shutdown the whole pool gracefully.
{ "type": "shutdown" }
```

### Manager → Client — streamed events on a request

```jsonc
// Routed to whichever channel won the request.
{ "type": "text_delta", "requestId": "req-abc", "text": "Hello" }

// Tool_use events — manager mints anthropic_id and remembers (channelId, execId).
{ "type": "tool_use", "requestId": "req-abc", "anthropic_id": "toolu_abc...",
  "name": "Read", "args": {"file_path":"/foo"} }

// Round complete — channel goes back to `ready`.
{ "type": "yield", "requestId": "req-abc" }

// Errors — manager may downgrade the channel to dead and pick another.
{ "type": "error", "requestId": "req-abc"|null, "message": "..." }
```

### Status response shape

```jsonc
{
  "type": "status",
  "pool": {
    "configuredSize": 5,
    "channels": [
      { "id": "ch-0", "state": "ready",   "openedAt": ...,
        "openAttempts": 87, "lastActivityAt": ..., "idleMs": 240000,
        "roundsServed": 14, "pid": 12345 },
      { "id": "ch-1", "state": "opening", "openAttempts": 123, "pid": 12346 },
      { "id": "ch-2", "state": "busy",    "currentRequestId": "req-abc",
        "openAttempts": 92, "pid": 12347, "roundsServed": 7 },
      ...
    ],
    "readyCount": 3,
    "busyCount": 1,
    "openingCount": 1,
    "deadCount": 0,
    "pendingRequests": 0
  },
  "config": {
    "model": "claude-opus-4-7-thinking-max-fast",
    "idlePingMs": 1200000,    // 20 min
    "openRetryMs": 300,
    "openRetryMax": 300
  }
}
```

## Key invariants

1. **Tool-round-trip stickiness.** When a worker emits a `tool_use`, the manager records
   `(anthropic_id) → (channelId, execId)`. The next request's `send_tool_result` MUST be
   routed back to that exact channel — otherwise the inner agent has no pending tool to
   respond to and the round breaks.

2. **One request per channel.** Each channel services at most one request at a time
   (a request being either a `send_user_message` or `send_tool_result` cycle that ends
   with `yield`). Channels in `busy` state are skipped by the round-robin picker.

3. **Tool list changes trigger a full reopen.** If a request comes in with a tool list
   that differs from what the pool was opened with, the manager closes and reopens
   ALL workers (because they need to be in sync). For MVP this is acceptable; in
   practice claude-code's tool list is stable per session.

4. **Channel respawn is automatic.** When a worker exits (`process.on('exit')`) or
   emits a `dead` state, the manager replaces it with a fresh fork. The new worker
   starts in `spawning` → `opening` and joins the rotation when `ready`.

5. **Idle ping.** A `ready` channel that has been idle ≥ `idlePingMs` (default 20 min)
   gets a `ping` message (round-trip through `bajie_yield` with content like
   `[health-check] reply with exactly OK`). If the round doesn't complete within
   30s OR returns an error, the channel is marked dead and respawned.

6. **API server is stateless.** All persistent state lives in the pool manager.
   Restarting api-server.mjs reconnects to the same pool, loses no warm channels.
