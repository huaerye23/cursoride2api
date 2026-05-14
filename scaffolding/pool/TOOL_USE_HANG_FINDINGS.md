# Tool-use round-trip hang — root cause findings

Captured 2026-05-14 after end-to-end instrumentation of api-server,
pool-manager, bridge-worker, and cursor-agent-h1. Test reproducer:
`scaffolding/pool/tool-roundtrip-test.mjs`.

## TL;DR

The hang is **not** an H1 transport issue and **not** a routing issue.
The chain works perfectly up to: bridge sends `ExecClientMessage(shell_stream{stdout})`
+ `shell_stream{exit}` to Cursor over `BidiAppend`. **Cursor's model
restarts** generation (we see `interactionUpdate:textDelta`,
`partialToolCall`, `toolCallStarted`, even a new `execServerMessage:mcpArgs`),
**then stalls mid-second-tool-call** — last visible event is one
`interactionUpdate:toolCallDelta` followed by `conversationCheckpointUpdate`,
then only heartbeats forever.

The bug is downstream of our payload — either:
1. The shell-stream proto encoding is subtly wrong (Cursor's model gets
   confused enough to keep producing for ~1 turn, then locks up), or
2. Cursor's server has a state machine that requires a specific control
   message we're not sending (e.g. user-confirmation, output-location, etc.).

Both H1 (`cursor-agent-h1.js`) and H2 (`cursor-agent.js`) reproduce the
same hang — proven by switching `POOL_BRIDGE_PROTOCOL` and rerunning
the test. So the bug is in the proto payload, not the transport.

## What works (verified end-to-end)

| Step | Mechanism | Status |
|---|---|---|
| HTTP/1.1 `BidiAppend` POST | unary, 200 OK with empty `{}` body | ✅ |
| HTTP/1.1 `RunSSE` stream | Connect-Web envelopes over chunked | ✅ |
| Channel open via `runRequest` | Initial AgentClientMessage | ✅ |
| `bajie_yield` mcp result round-trip | sendToolResult(yield_id, prompt_text) | ✅ (step 1) |
| Bridge → pool-manager → api-server tool_use plumbing | both H1 and H2 | ✅ |
| Anthropic SSE wire format | claude-code reads the events correctly | ✅ |
| Pool routing of incoming tool_result | toolUseIndex lookup, channel mapping | ✅ |
| Bridge.sendToolResult on shellStream | builds ShellStream{stdout}+{exit} | ✅ (correctly invoked) |
| `sendBinaryFrame` over BidiAppend | proto bytes hex-encoded, 200 OK | ✅ |

## What's broken

After the bridge sends `shell_stream{stdout, exit}`:

```
[cursor-agent-h1 RX] msgCase=kvServerMessage          ← server processing
[cursor-agent-h1 RX] msgCase=kvServerMessage
[cursor-agent-h1 RX] msgCase=interactionUpdate:toolCallDelta  ← model started
[cursor-agent-h1 RX] msgCase=conversationCheckpointUpdate
[only heartbeats from this point — stream alive but model stalled]
```

The model BEGAN generating its next tool call (we see `toolCallDelta`),
then stopped emitting deltas. No `toolCallStarted`, no `mcpArgs`, no
`toolCallCompleted`, no `textDelta`. Cursor's server-side state is
stuck waiting for something we're not providing.

## What we tried (didn't help)

1. **Adding `ShellStream{event: start}` before stdout** — protocol-level
   the canonical order is start → stdout → exit. Adding it didn't
   change the symptom.
2. **Adding `ConversationAction(shellCommandAction, exec_id)` before
   the stream events** — hypothesis: Cursor's server requires explicit
   user-confirmation of the shell command. The IDE does this in the
   normal flow. Adding it didn't help.

Both are committed-out (reverted). The diagnostic logging is kept.

## Strong next-step hypotheses (untested)

In priority order:

1. **Set `output_location` on the exit event.** Looking at the proto:
   `ShellStreamExit { code, cwd, output_location, aborted, abort_reason }`.
   Cursor likely uses `output_location` to know where the stdout/stderr
   actually got written (file path or in-memory ID). Without it, the
   model may be waiting for "where do I read the actual output from?"
   to decide its next action.
2. **Use `ShellResult` not `ShellStream` even for shellStreamArgs.**
   Cross the wires — model called shellStreamArgs but we respond with
   shell_result (1-shot, simpler schema). May or may not be tolerated.
3. **Force MCP path instead of native passthrough.** Set
   `RATLC_PASSTHROUGH_NATIVE=0` for the worker spawn env. Then native
   shell calls get rejected and the model has to use the MCP `Bash`
   tool we registered. The mcpResult round-trip is proven to work
   (step 1 succeeded via mcpResult). **Caveat**: requires bridge to
   know about caller's tools, not just the placeholder.
4. **Pull and grep the Cursor IDE bundle** for the exact construction
   of the shell-stream response when the user clicks "Run" in the UI.
   The bundle has the answer to "what makes Cursor's server tick when
   processing shell_stream events." This is the surest fix but the
   highest cost — the bundle is ~50MB minified.

## How to repro

```bash
cd /Users/juncwang/Downloads/new_mcp_study/cursoride2api
./scaffolding/pool/ratlc down
POOL_BRIDGE_PROTOCOL=h1 POOL_TOOL_MODE=translate POOL_CONCURRENT_OPENS=1 \
  CURSOR_LOG_SERVER_MSG=1 LOG_REQUEST_TOOLS=1 \
  ./scaffolding/pool/ratlc up 1
# Wait for ready=1
node scaffolding/pool/tool-roundtrip-test.mjs
# Step 1 passes within ~10s. Step 2 hangs forever.
# All diagnostic logs land in /tmp/ratlc-pool.log and /tmp/ratlc-api.log
```

## Diagnostic surface added

| File | Log line |
|---|---|
| `api-server.mjs` | per-POST `body: lastMsg.role=... content=[...] msgCount=N` |
| `api-server.mjs` | per-dispatch `→ pool send_user_message requestId=... tools=N` / `→ pool send_tool_result requestId=... tool_use_id=... bytes=N` |
| `pool-manager.mjs` | `route send_tool_result ... found=true/false indexSize=N` + `✅ routing to ch-X execId=...` |
| `bridge-worker.mjs` | per-IPC `IPC type=... requestId=... state=... pendingYield=bool pendingMcp=...` + BEFORE/AFTER/EXCEPTION around `bridge.sendToolResult` |
| `cursor-agent-h1.js` | `sendToolResult id=... execId=... kind=... contentSize=...` |
| `cursor-agent-h1.js` | always-on `BidiAppend OK seqno=N body={...}` + always-on `BidiAppend FAIL/EXCEPTION` |
| `cursor-agent-h1.js` | opt-in `[cursor-agent-h1 RX] msgCase=...` (via `CURSOR_LOG_SERVER_MSG=1`) |
