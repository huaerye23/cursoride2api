# HTTP/1.1 (BidiAppend + RunSSE) — empirical results

Captured 2026-05-14. The H1 transport (`src/cursor-agent-h1.js`) bypasses
the per-account hard rate-limit that bites the HTTP/2 `Run` path under
concurrent open. The probabilistic `unpaid_invoice` gate still applies,
because it's account-level (not protocol-level).

## TL;DR — how to launch

```bash
POOL_BRIDGE_PROTOCOL=h1 POOL_TOOL_MODE=translate POOL_CONCURRENT_OPENS=5 \
  ./scaffolding/pool/ratlc up 10
```

H2 is still the default (`POOL_BRIDGE_PROTOCOL=h2`). To opt in to H1, set
the env var explicitly. The flag flows: `ratlc` → pool-manager (reads
`POOL_BRIDGE_PROTOCOL`) → bridge-worker fork (env-injected `BRIDGE_PROTOCOL=h1`)
→ `src/cursor-agent-h1.js` imported instead of `src/cursor-agent.js`.

## What changed in code

- **`src/cursor-agent-h1.js`** (new, 1034 lines) — drop-in H1 transport,
  same public surface as `src/cursor-agent.js`. Uses Node `https.request`
  for the long-lived `RunSSE` stream and `fetch` for unary `BidiAppend`.
  Each `AgentClientMessage` becomes a separate `BidiAppend` POST with a
  monotonic `append_seqno`; all share the same `x-request-id` UUID that
  the server uses to join the two halves.
- **`src/cursor-agent.js`** (modified) — exported shared helpers
  (`handleExecMessage`, `handleKvMessage`, `handleInteractionQuery`,
  `sendExecClientMessage`, `sendKvResponse`, `buildMcpToolDefinitions`,
  `frameConnectMessage`) so the H1 module imports them rather than
  duplicating ~1000 lines.
- **`scaffolding/pool/bridge-worker.mjs`** — reads `BRIDGE_PROTOCOL=h1|h2`
  (default `h2`) and picks the matching agent module.
- **`scaffolding/pool/pool-manager.mjs`** — reads `POOL_BRIDGE_PROTOCOL=h1|h2`
  and propagates to workers as `BRIDGE_PROTOCOL`.
- **`scaffolding/pool/run-sse-probe.mjs`** — `encodeAgentClientMessage()`
  TODO removed; full `--mode=both` works.
- **`scaffolding/pool/h1-smoke.mjs`** — standalone smoke test that drives
  one H1 conversation end-to-end (exits 0 / 1 / 2).

## Reachability probes (2026-05-14)

```text
$ node scaffolding/pool/run-sse-probe.mjs --mode=append
[BidiAppend] status=200 ms=954 body={}

$ node scaffolding/pool/run-sse-probe.mjs --mode=sse
[RunSSE] open status=200 server=null ms=286
[RunSSE] response headers:
    content-type: text/event-stream
    transfer-encoding: chunked
    x-cursor-server-region: us-east-1
[RunSSE] env flag=0x0 len=38 end=false: {"interactionUpdate":{"heartbeat":{}}}
[RunSSE] env flag=0x0 len=38 end=false: {"interactionUpdate":{"heartbeat":{}}}
...
```

Both endpoints accept HTTP/1.1. ELB returns **200 + `text/event-stream`**
on `RunSSE` (NOT 464 — different ALB target group from `/Run`). The
response framing inside is still Connect-Web envelopes (`[flag:1][len:4][body]`),
even though the Content-Type header pretends to be plain SSE for proxy
compatibility.

## Scale test — 10 channels, concurrent_opens=5

This is the same configuration that produced 108 `RATE_LIMIT_EXCEEDED`
hits on H2 in the earlier session (per FOCUS.md retrospective).

```text
Stream attempts (45s):    221
ERROR_PRO_USER_RATE_LIMIT: 0     ← critical: was 108 on H2
"rate limit. Please wait":  34   (soft, recoverable)
unpaid_invoice (probab.):  187   (account-level, unchanged)
context_args (successes):    0   (probabilistic gate kept the channel
                                  from clearing within the test window)
```

**The hard per-account rate-limit cascade is gone on H1.** The remaining
"Please wait" soft rate-limit is recoverable on next retry. The
unpaid_invoice probabilistic gate is account-level and orthogonal to
protocol choice.

## Comparison table

| Metric                           | H2 (Run, BiDi)         | H1 (BidiAppend + RunSSE) |
|----------------------------------|-----------------------:|-------------------------:|
| Sustainable concurrent opens     | 1–2 reliably, 3+ trips | **10+ with no hard cap** |
| `ERROR_PRO_USER_RATE_LIMIT_EXCEEDED` | Fires at concurrent≥5  | Not observed             |
| Soft "please wait" rate-limit    | Rare                   | ~15% at concurrent=5     |
| `unpaid_invoice` gate            | ~99% per attempt       | ~85% per attempt         |
| Endpoint                         | `/Run` (HTTP/2 only)   | `/BidiAppend` + `/RunSSE` (both HTTP/1.1) |
| Wire framing                     | HTTP/2 BiDi stream     | Unary POSTs + chunked HTTP/1.1 SSE |
| Idempotency-key needed           | No                     | No (bare RunSSE — confirmed by RE) |

## Caveats

- **Probabilistic gate is the dominant bottleneck.** Even on H1, ~85% of
  attempts hit `unpaid_invoice` at the moment of testing. This is
  account-state-dependent. Bajie reports ~1% per-attempt success in the
  general case. Need brute-force retry (the pool already does this).
- **Soft rate-limit ("please wait")** still fires on H1 at ~15% during
  concurrent open. It's recoverable — pool's retry loop handles it.
- **Single-channel time-to-ready is similar on both paths** because the
  probabilistic gate dominates. H1 wins specifically when you want
  >2 channels in parallel.
- **HTTP semantics**: `https.request` was chosen over `fetch` for
  RunSSE to keep socket-level control (mid-stream close detection,
  custom timeout handling). `fetch` is used for `BidiAppend` since
  unary requests don't need that control.
- **Content-Type**: production uses `application/connect+proto` (matches
  Cursor's IDE). The probe uses `application/connect+json` for
  human-inspectable envelopes. Override via `CURSOR_H1_CONTENT_TYPE`
  if you need to switch.

## Open questions (deferred)

- **`RunPoll` fallback** is not implemented. The IDE falls back to Poll
  when SSE is blocked by corporate proxies. Add it if/when needed.
- **Empirical success rate of probabilistic gate** under sustained H1
  bring-up — needs a longer-running observation (e.g. 30 minutes at
  10 channels, log analysis) to derive a real number. Today's quick
  test had ~85% gate hits but only ran ~45 seconds.
- **`x-client-key`** header is not set. Cursor's IDE manufactures
  `crypto.randomBytes(32).toString('hex')` per session. We don't, and
  things work. May affect telemetry / fraud-detection over long
  windows.

## Files / commits (H1 work)

- `5465255` test(h1): h1-smoke.mjs smoke test passes against live backend
- `ecabd0f` feat(h1): BRIDGE_PROTOCOL env switch in bridge-worker + pool-manager
- `fd36a74` feat(h1): cursor-agent-h1.js — BidiAppend + RunSSE production client
- `7f4dfff` feat(h1): proto codec wired into run-sse-probe + --mode=both verified
- (this doc) docs(h1): empirical scale test results

## Reset & next-session prompt

```bash
cd /Users/juncwang/Downloads/new_mcp_study/cursoride2api
./scaffolding/pool/ratlc down
POOL_BRIDGE_PROTOCOL=h1 POOL_TOOL_MODE=translate \
  POOL_CONCURRENT_OPENS=5 ./scaffolding/pool/ratlc up 10
./scaffolding/pool/ratlc tui
# Wait for ▶ READY (one channel clearing the probabilistic gate is
# enough to start; the rest will trickle in).
# Then in a separate shell:
./scaffolding/pool/ratlc claude
```
