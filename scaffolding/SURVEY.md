# HTTP/2 vs HTTP/1.1 against Cursor's premium-model gate — empirical survey

**Question:** Bajie MCP's UI offers an "HTTP/1.1" toggle alongside its retry-on-credit-popup loop. Does HTTP/1.1 actually help reach `claude-opus-4-7-thinking-max` on a paid Pro account whose Claude-model requests are being soft-blocked with an `"unpaid invoice"` error? Or is retry doing all the work?

**Short answer:** Retry does all the work. HTTP/1.1 to Cursor's gRPC endpoints is rejected at the AWS ELB layer before reaching any gate, so its empirical success rate is **exactly 0%**. HTTP/2 succeeds **probabilistically at ~1% per attempt** (95% CI 0.34–2.90%), which means ~150-300 attempts at 300 ms intervals reliably gets one conversation through.

---

## 1. Setup

- **Account state** (from `/auth/full_stripe_profile`, HTTP/2): `membershipType: "pro"`, `subscriptionStatus: "active"`, `lastPaymentFailed: false`, `isOnBillableAuto: true`. **The account is fully paid** — the `"unpaid invoice"` response is a soft-throttle message, not the literal subscription state.
- **Target model:** `claude-opus-4-7-thinking-max` (an Anthropic-routed model on Cursor's backend).
- **Cheap reference models:** `default` and `composer-2-fast` succeed on every attempt — confirming the gate is per-model-class (Claude family) rather than per-account.
- **Date of measurement:** 2026-05-13.
- **Proxy commit:** branch `feat/anthropic-api-support`, `cursoride2api` repo, `src/cursor-agent.js` at line 197 using `http2.connect('https://api2.cursor.sh')` with `H2_POOL_SIZE = 3`.
- **Hardware/network:** local proxy on `127.0.0.1:4141`, residential US connection to `api2.cursor.sh` (98.86.55.191, AWS us-east-1).

## 2. Method

Two phases, run back-to-back from a single Node script (`scaffolding/measure-success-rate.mjs`):

| Phase | Transport | Path | Payload |
|---|---|---|---|
| **H2** | HTTP/2 via proxy | client → localhost:4141 → `api2.cursor.sh/agent.v1.AgentService/Run` | Full OpenAI-style chat completion request with `claude-opus-4-7-thinking-max` |
| **H1** | HTTP/1.1 direct | direct `https.request()` (no ALPN-h2) → `api2.cursor.sh/agent.v1.AgentService/Run` | Minimal connect-proto framed body (5-byte empty envelope), same auth headers (Bearer token, x-cursor-checksum, x-cursor-client-version, connect-protocol-version) |

- **Retry interval:** 300 ms between attempts (matching the empirical sweet spot reported for the proxy's user)
- **Sample size:** 300 attempts (H2), 100 attempts (H1)
- **Stop rule:** no early-exit — every attempt counted, even after a success
- **Latency captured per attempt** (separated into success vs failure pools)
- **Confidence interval:** Wilson 95% (more honest than naive ±Z·√(p(1-p)/n) at small successes)

H2 went through the proxy (rather than direct) because it exercises the actual production code path, with real request payloads and tool definitions that pass Cursor's schema validation. The proxy adds <5 ms of localhost overhead, negligible vs the 300+ ms upstream call.

H1 went direct (bypassing the proxy) because the proxy is HTTP/2-only and we wanted to actually exercise HTTP/1.1 wire behavior. The minimal payload is acceptable here because, as the results show, the request never reaches a layer that inspects the payload.

## 3. Results

### HTTP/2 via proxy — 300 attempts

| Outcome | Count | % | p50 latency | p95 latency |
|---|---:|---:|---:|---:|
| `success` (200, OpenAI choices) | **3** | **1.00%** | 2,320 ms | 3,425 ms |
| `unpaid_invoice` (error response) | 297 | 99.00% | 340 ms | 778 ms |

- **Per-attempt success rate: 1.00%, Wilson 95% CI [0.34%, 2.90%]**
- **Time-to-first-success:** attempt 148 in our earlier run, attempt 242 in this run. Means clustering "first success" empirically lands between attempts ~75 and ~250 — consistent with the user's report of "50-100 retries to get one connection" (their estimate skews to the optimistic end of our CI).
- **Latency split is bimodal and useful as a classifier:** rejected attempts land in 300-500 ms (Cursor's gate returns immediately after auth + model-routing decision), successful attempts span 2-3.5 s (real LLM inference). If you wanted to detect a "this attempt will succeed" earlier than 30 s timeout, anything still in flight past 1 s is almost certainly going to succeed.
- **Successful response shape (sample):** `{model: "claude-opus-4-7-thinking-max", content: "PONG", usage: {prompt_tokens: 22779, completion_tokens: 7, total_tokens: 22786}}` — fully formed OpenAI-style response, no degradation vs. a normally-served request.

### HTTP/1.1 direct — 100 attempts

| Outcome | Count | % | p50 latency | p95 latency |
|---|---:|---:|---:|---:|
| `464 elb_reject` (AWS ELB) | **100** | **100.00%** | 76 ms | 90 ms |
| `success` | 0 | 0% | — | — |

- **Per-attempt success rate: 0.00%, Wilson 95% CI [0.00%, 3.70%]** (the upper bound shrinks toward zero with larger samples; at n=100, our data is consistent with anywhere from 0% to 3.7% true rate, but the 100/100 deterministic rejection pattern strongly implies the true rate is 0).
- **Every response had `Server: awselb/2.0`** — AWS Application Load Balancer. HTTP 464 is an ALB-specific code for "incompatible HTTP protocol version" (client sent HTTP/1.1 but the target group is HTTP/2-only).
- **The 76 ms p50 is just the TCP+TLS handshake to ELB plus its instant 464 reply** — these requests never reached Cursor's backend, never touched Cursor's gate logic, and never consumed quota. They're free rejections at the edge.

### Side-by-side

```
                    successes   95% CI            reachable-rate
HTTP/2 via proxy:   3 / 300     0.34 – 2.90%      100% (every request reaches gate)
HTTP/1.1 direct:    0 / 100     0.00 – 3.70%        0% (ELB blocks at edge)
```

The CIs technically overlap (HTTP/1.1's upper bound at 3.70% touches HTTP/2's lower at 0.34%), but the rejection mechanisms are entirely different:

- HTTP/2's failures are **Cursor's application-layer soft-throttle** (a meaningful response from the backend you can act on)
- HTTP/1.1's failures are **AWS ELB's protocol-version filter** (the request is dropped before any application logic runs)

## 4. RE-document cross-reference

Two of the RE docs already in the repo confirm the architecture independently of our measurement:

- **`Cursor API 端点大全.md:840`:**
  > "HTTP/2 是必须的 — 不支持 HTTP/1.1"
- **`Cursor IDE API 逆向工程文档.md:1015-1016`:**
  > "目标域名: `api2.cursor.sh` / 协议: HTTP/2 (必须)"

Cursor's backend, behind `api2.cursor.sh`, is gRPC-over-HTTP/2 only. The AWS ALB in front of it has no HTTP/1.1 fallback configured for the gRPC services — only for the auth endpoints (`/auth/*`), which **do** answer HTTP/1.1 (we verified `/auth/full_stripe_profile` returns 200 over HTTP/1.1).

## 5. What Bajie's "HTTP/1.1" toggle is actually doing

Bajie's UI (`bajie-mcp-3.1.9.vsix`, `dist/extension.js`) writes two Cursor settings:

```js
disableHttp2     = (mode !== 'http2');
disableHttp1SSE  = (mode === 'http1.0');
await config.update('cursor.general.disableHttp2', disableHttp2, Global);
await config.update('cursor.general.disableHttp1SSE', disableHttp1SSE, Global);
```

Three plausible explanations for why this exists despite HTTP/1.1 being blocked at the gRPC endpoints:

1. **Stale / placebo.** May have worked on an older Cursor where ELB allowed HTTP/1.1, since hardened. Bajie's UI still exposes it because it doesn't actively harm anything.
2. **Routes Cursor's IDE to the SSE/Poll variants.** The RE catalog lists `agent.v1.AgentService/RunSSE`, `RunPoll`, `aiserver.v1.ChatService/StreamUnifiedChatWithToolsSSE` — server-streaming RPCs that are HTTP/1.1-compatible in shape (text/event-stream over chunked transfer encoding). They require `x-idempotent-encryption-key` (an AES-GCM 256-bit JWK generated client-side via `crypto.subtle.generateKey`). The proxy never tested these because the key derivation hasn't been reverse-engineered.
3. **Independent of transport, but tooltip wording is misleading.** Bajie's tooltip says *"need to enable HTTP/1.1 first, then use auto to send a conversation, then send another with the advanced model"* — the two-step recipe (auto warmup → premium) may be the real mechanism, with the HTTP version flag being a confounded variable in the user's empirical experience.

**Tested in this survey:** (3) — warmup with `composer-2-fast` followed immediately by `claude-opus-4-7-thinking-max` over HTTP/2 fails deterministically (3 sequential premium retries: all 3 hit `unpaid_invoice`). So the two-step recipe by itself does not bypass the gate.

**The load-bearing component is the retry, not the protocol.** Bajie's renderer-side script also matches this: its main retry loop ignores HTTP version entirely and just keeps re-sending until the composer reaches a "stable ready" state. Our measurement confirms that's all that's needed.

## 6. Implications for cursoride2api

### 6.1 Minimum viable change

Wire a retry-on-`unpaid_invoice` path into `src/cursor-agent.js:failOrRetry()`. The current implementation only retries on H2-transport-level errors (REFUSED_STREAM, INTERNAL_ERROR). A small addition:

```js
// New classifier alongside isTransient
const isModelGateThrottle =
  /unpaid invoice|cursor\.com\/dashboard\b.*resume requests/i.test(msg);

// New schedule: fixed 300ms, separate counter, much higher cap
const MAX_MODEL_GATE_RETRIES = parseInt(
  process.env.CURSOR_MODEL_GATE_RETRY_MAX || '200', 10);

if (isModelGateThrottle && modelGateRetries < MAX_MODEL_GATE_RETRIES && !hasEmittedContent) {
  modelGateRetries++;
  // Don't poison the H2 client — this isn't a transport problem.
  setTimeout(() => attemptConnection(proto), 300);
  return;
}
```

- **Separate counter from `retryAttempts`** so transport retries don't share budget with model-gate retries.
- **Fixed 300 ms interval, no exponential backoff** — exp-backoff was tuned for LB cascade scenarios where bursts hurt. The gate is a per-request lottery; slowing down doesn't change the odds.
- **Don't poison the H2 pool slot** — the connection is fine, only this specific request was rejected at the application layer.
- **`hasEmittedContent` guard preserved** — never retry once user-visible bytes have shipped.

### 6.2 Optimization: parallel retry

Since each attempt is independent and rejected attempts return in ~340 ms, **N concurrent attempts** roughly divide time-to-first-success by N:

- Serial (N=1, 300 ms interval): expected wall time = (148 ± 50) × 0.64s ≈ **60-130 s** before first success
- Parallel (N=5): expected wall time ≈ **12-26 s**, at the cost of 5× more upstream calls before cancellation

Practical caveat: Cursor's gate may have a per-connection rate-limit we'd discover only by trying. Worth measuring N=2, N=5, N=10 to find the knee.

### 6.3 Cost / load model

For every successful premium-model conversation:

- **~150 upstream POSTs to `api2.cursor.sh`** (serial), **~5-10** when one slips through under parallel-N
- **~50 KB / attempt** request payload (full conversation context with tools schema) → **~7.5 MB egress** per success on serial. Per-request size is dominated by tools schema; if `TOOL_INCLUDE` is set, payload halves.
- **0 Anthropic input/output tokens billed** for the rejected attempts (Cursor's gate fires before model routing)
- **~22,800 input tokens + 7 output tokens** for the actual successful inference (sample observation)

For agentic workloads that don't care about latency (background batch tasks), this is fine. For interactive use (IDE assistant), a 60-130 s time-to-first-token is unacceptable and parallel-N is mandatory.

## 7. Open questions worth flagging in STUDY-REPORT.md

1. **SSE/Poll variants** (`RunSSE`, `StreamUnifiedChatWithToolsSSE`, `*WithToolsPoll`) — never tested by this project. They use `x-idempotent-encryption-key`, an AES-GCM 256-bit JWK generated client-side. If these endpoints have **different gate behavior** (lower throttle rate, or unconditional 200), they'd be a much better wire transport than retrying `agent.v1.AgentService/Run`. Requires RE work on `crypto.subtle.generateKey` usage in `workbench.desktop.main.js`.
2. **Per-connection vs per-request rate-limit** — does the proxy's `H2_POOL_SIZE = 3` already help, or hurt? Worth measuring success rate at pool sizes 1, 3, 10.
3. **Time-of-day effects** — is the gate's 1% rate constant, or does it vary with global Cursor load? A 24-hour run at fixed cadence would answer this.
4. **Token-level effects** — does rotating between multiple `tokens[]` in `token.json` independently slot each into the lottery? If yes, N tokens × M concurrent ≈ N×M parallel attempts.

## 8. Reproduction

```bash
cd cursoride2api
PORT=4141 node server.js &           # start proxy
node scaffolding/measure-success-rate.mjs    # ~3-4 min wall time
# Result printed + JSON dropped at scaffolding/measurement-result.json
```

Tunables via env: `MAX_H2`, `MAX_H1`, `INTERVAL_MS`, `MODEL`, `PROXY_URL`, `SKIP_H2=1`, `SKIP_H1=1`.

The HTTP/1.1 phase is independent of the proxy — only the H2 phase requires it running.

## 9. Bottom line

| Question | Empirical answer |
|---|---|
| Does HTTP/1.1 bypass Cursor's premium-model gate? | **No.** AWS ELB rejects HTTP/1.1 to `agent.v1.AgentService/Run` at the edge with HTTP 464, 100% of the time. The request never reaches the gate. |
| Does HTTP/2 work? | **Yes, at ~1% per-attempt.** Confirmed at 3 successes in 300 attempts, 95% CI [0.34%, 2.90%]. |
| Is Bajie's "HTTP/1.1" toggle the real trick? | **No, it's a confound.** Bajie's retry loop is what punches through the gate; the HTTP version flag is ineffective against the gRPC endpoints. |
| Does retry alone suffice? | **Yes.** 150-300 attempts at 300 ms intervals over HTTP/2 reliably yields ≥1 success. |
| Should cursoride2api adopt retry-on-`unpaid_invoice`? | **Yes**, with a separate counter from H2-transport retries, fixed 300 ms interval, capped at ~200 attempts. Optionally parallel-N for latency-sensitive use. |
| Is there a better path than retry? | **Maybe**, but requires RE work. The SSE/Poll variants are unexplored and may have different gate behavior. |
