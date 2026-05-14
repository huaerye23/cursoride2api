# RATLC — current focus & next-session plan

Captured 2026-05-14 after a long debugging arc. Used to compact context
and hand off to fresh sessions / multi-agent teams.

## Current state of the world

- **Architecture is working end-to-end for text + the common tool paths.**
  - Pool of N channels (default 2), translate mode + contract mode both
    operational, sequential or parallel opens (`POOL_CONCURRENT_OPENS=K`),
    automatic respawn, idle ping at 20 min, /metrics endpoint, `ratlc` CLI
    with split-screen TUI and `:`-prefixed command bar.
  - Translate mode passes Cursor-native tools (Shell/Read/Write/Grep/
    Fetch/shellStream/backgroundShellSpawn) through `onMcpCall` with
    Anthropic name translation; sendToolResult dispatches by native_kind.
  - claude-code can talk to the pool over `ANTHROPIC_BASE_URL`.
- **Two real blockers remain** — both documented below.

## Open blockers

### Priority 1: HTTP/1.1 retry loop (Bajie pattern)

**Why we want it:**

Cursor's `agent.v1.AgentService/Run` is HTTP/2-only at the ELB layer
(verified — returns HTTP 464 on HTTP/1.1). When the account hits the
per-account rate limit (`ERROR_PRO_USER_RATE_LIMIT_EXCEEDED`), the only
path forward today is exponential backoff (5 s → 60 s cap), which makes
N-channel parallel bring-up infeasible.

**Bajie observation:** the UI exposes an "HTTP/1.1" toggle. When set,
Cursor's IDE behavior shifts. We hypothesized — but did NOT prove —
that this toggle routes Cursor's IDE to a different endpoint family
(SSE / Poll variants) that may have different rate-limit behavior.

**What's known:**

- `Cursor API 端点大全.md:840` says HTTP/2 mandatory.
- `Cursor IDE API 逆向工程文档.md:1015-1016` confirms target `api2.cursor.sh`
  port 443, HTTP/2 mandatory.
- But the RE catalog lists SSE/Poll variants:
    - `agent.v1.AgentService/RunSSE` — server-streaming, requires
      `x-idempotent-encryption-key`
    - `agent.v1.AgentService/RunPoll` — long-poll variant
    - `aiserver.v1.ChatService/StreamUnifiedChatWithToolsSSE` — needs key
    - `aiserver.v1.ChatService/StreamUnifiedChatWithToolsPoll` — needs key
- These are conceptually HTTP/1.1-friendly (text/event-stream over
  chunked transfer encoding).
- The encryption key is described as an **AES-GCM 256-bit JWK** generated
  client-side via `crypto.subtle.generateKey('AES-GCM', 256)` (see
  `Cursor IDE API 逆向工程文档.md:1009-1011`).
- We never probed these endpoints. The proxy's existing code only uses
  `Run` (BiDi over HTTP/2).

**Concrete next-session prompt (single agent, RE-heavy):**

> Read `cursoride2api/Cursor API 端点大全.md` and
> `cursoride2api/Cursor IDE API 逆向工程文档.md` end-to-end. Then read
> `REFERENCES.md` and the relevant entries under "What we actually
> consumed from" / "Cross-checked but did not adopt".
>
> Goal: produce a concrete plan to invoke
> `agent.v1.AgentService/RunSSE` (or `…/RunPoll`, or
> `…/StreamUnifiedChatWithToolsSSE`) directly. Specifically answer:
>
> 1. How is `x-idempotent-encryption-key` derived? Is it just a random
>    AES-GCM JWK each request, or is there a server-side handshake?
>    Locate the relevant code in Cursor's `workbench.desktop.main.js`
>    (the user has Cursor installed; ask for the path or grep the
>    bundle).
> 2. What does the request body look like? Same as `Run` but framed
>    differently for server-streaming? Or a different proto?
> 3. Does HTTP/1.1 actually reach these endpoints? Test with curl +
>    minimal headers. If 464, the design is wrong.
> 4. Empirically: does the SSE/Poll path have **different rate-limit
>    behavior** than `Run`? Run a comparison: 10 parallel requests via
>    `Run` (expected to trip) vs 10 parallel via `RunSSE` (TBD).
>
> Out: `scaffolding/pool/RUNSSE.md` documenting the protocol +
> empirical findings, and (if viable) a working client in
> `scaffolding/pool/run-sse-probe.mjs`.

**Multi-agent parallelism for P1** (if RE work is too big for one):

- Agent A: Find encryption-key derivation in `workbench.desktop.main.js`
- Agent B: Find request-body shape and framing for `RunSSE`
- Agent C: Survey other RE projects (REFERENCES.md catalog) for prior
  art on these endpoints

### Priority 2: Tool-use round-trip hang (E2E debug)

**The symptom:**

In translate mode, claude-code makes a tool_use call (e.g. Read). Our
proxy emits `content_block(tool_use, name=Read)` SSE to claude-code.
claude-code **never POSTs back a tool_result**. The pool channel
permanently holds the tool_use_id, stays `busy` forever.

**What's confirmed:**

- claude-code receives the tool_use (the api-server logs `→ tool_use to
  client` and the SSE is well-formed by curl-replay).
- claude-code is alive (process still running, listed in `ps`).
- claude-code's per-request HTTP socket may or may not still be open —
  we haven't checked.
- The file path in the failing case (`central_docs/.../email_provider_…md`)
  doesn't exist from claude-code's CWD; but **a missing file should
  return a tool_result with an ENOENT error**, not silence.

**What's not yet known:**

1. Is our SSE format subtly wrong compared to real Anthropic API?
   - Missing event? Wrong field name? Mis-ordered events?
   - claude-code might silently drop responses that don't match its
     internal schema.
2. Is claude-code holding the SSE socket waiting for more events?
   - Maybe message_delta/message_stop weren't sent in some path.
3. Is claude-code POSTing back to a wrong endpoint?

**Tooling we have:**

- `scaffolding/pool/tap.mjs` (new, this session, uncommitted) — drop-in
  HTTP capture proxy. Sits in front of api-server, dumps every request
  body + every SSE event to per-request JSONL files under
  `/tmp/ratlc-tap/`.

**Concrete next-session prompt (single agent, debugging):**

> Read `cursoride2api/scaffolding/pool/FOCUS.md` then
> `cursoride2api/scaffolding/pool/tap.mjs`. Goal: capture a full
> round-trip from claude-code through the pool when the tool-use hang
> reproduces, and find the bug.
>
> Steps:
>
> 1. `./scaffolding/pool/ratlc down` then bring up with translate mode
>    and `POOL_CONCURRENT_OPENS=1` (avoid rate-limit thrash). Pool size 1.
> 2. Wait for ready=1.
> 3. Run tap in foreground: `TAP_PORT=4343 TAP_TARGET=http://127.0.0.1:4242
>    node scaffolding/pool/tap.mjs`
> 4. In another terminal: `ANTHROPIC_BASE_URL=http://127.0.0.1:4343
>    claude --dangerously-skip-permissions --effort max`
> 5. Inside claude: enter "check central_docs/epics/06/
>    [OpsEng]cme_rollover_email_reminder/email_provider_research.md
>    what does it say" — the exact failing prompt.
> 6. Observe: claude-code will receive a tool_use and (per the bug) not
>    POST back. Wait 30-60 s for symptoms.
> 7. Read every JSONL under `/tmp/ratlc-tap/`. Analyze:
>    - Did we emit `message_delta` + `message_stop` after the tool_use?
>    - Are the SSE event field names exactly what Anthropic emits?
>      Compare to the official Anthropic SSE spec
>      (https://docs.anthropic.com/en/api/messages-streaming).
>    - Is `client_disconnect` recorded in the capture? If yes,
>      claude-code dropped the connection — find when, see what was
>      the last event we sent before it closed.
> 8. **If claude-code DID post back, but our pool didn't route it:**
>    the bug is in pool-manager's `toolUseIndex` lookup. Trace
>    `anthropic_tool_use_id` from emission to lookup.
> 9. Out: `scaffolding/pool/TOOL_USE_DEBUG.md` with capture excerpts +
>    the identified root cause + the fix.

**Things to check that I suspect:**

- Anthropic spec requires `event: message_start` then a sequence of
  `content_block_*` then `event: message_delta` with stop_reason then
  `event: message_stop`. Ordering matters.
- `input_json_delta` for tool_use has a specific structure
  (`{"type":"input_json_delta","partial_json":"..."}`); ours emits the
  whole JSON in one delta. Spec might require multiple deltas or final
  empty delta? **Test.**
- claude-code may parse `content_block_start.content_block.input` as
  the initial state; we set `input: {}` and then send the actual args
  in `input_json_delta`. If claude-code expects the input directly in
  `content_block_start`, it'd be confused.

## What's NOT a current focus

- **More e2e claude-code scenarios** — until Priority 2 is resolved,
  iterating on different prompts will just hit the same hang.
- **Auto-release-on-disconnect** for stuck channels — known issue, but
  Priority 2 is the root cause.
- **/v1/chat/completions OpenAI compatibility** — out of scope until
  the Anthropic path is solid.
- **Multi-user / multi-token** — single account, single pool, until P1.

## Files / commits

Branch: `feat/ratlc-mvp` on
https://github.com/lovinrain/cursoride2api/tree/feat/ratlc-mvp

Latest commits:

- `72f9b4c` feat(ratlc tui): vim-style command input bar
- `a8e3ab0` feat(ratlc tui): split-screen + READY indicator
- `a07ac95` feat: unified ratlc CLI + parallel opens + /metrics
- `34399c2` fix: passthrough shellStreamArgs + backgroundShellSpawnArgs
- `02b0798` fix: translate v2 — round-trip end-to-end
- `c3b05e5` feat: translate-mode v2 native passthrough in cursor-agent
- `a44690c` feat: POOL_TOOL_MODE=translate scaffolding
- (older — see git log)

Uncommitted:

- `scaffolding/pool/tap.mjs` (capture proxy, needs commit)
- `scaffolding/pool/FOCUS.md` (this file, needs commit)

## Quick test environment reset

When rate-limit is fresh / quota is open:

```bash
cd /Users/juncwang/Downloads/new_mcp_study/cursoride2api
./scaffolding/pool/ratlc down
POOL_TOOL_MODE=translate POOL_CONCURRENT_OPENS=1 ./scaffolding/pool/ratlc up 2
# wait for ready=1 (~30-300 s depending on rate-limit state)
./scaffolding/pool/ratlc tui
```

When the account is hot-throttled, stop and wait 5-15 min before retrying.
The `RATE_LIMIT_EXCEEDED` budget appears to be per-account-per-rolling-window;
heavy concurrent use ages it out.

## Reading order for a fresh session

1. This file (`scaffolding/pool/FOCUS.md`)
2. `scaffolding/pool/IPC.md` — wire protocol between processes
3. `scaffolding/pool/FINDINGS.md` — what worked, what didn't
4. `scaffolding/pool/SURVEY.md` — HTTP/1.1 vs HTTP/2 empirical comparison
5. `REFERENCES.md` (root) — for P1 RE research only
6. `DEVLOG.md` (root) — long-form engineering notebook from the proxy
   project (predates RATLC)
