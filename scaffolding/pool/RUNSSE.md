# RunSSE / BidiAppend — protocol-level RE notes

Reverse-engineered from the locally-installed Cursor 2.6.20 bundle on
2026-05-13. **Scope: Priority 1, protocol design only — no live probes.**

## TL;DR

Cursor's HTTP/1.1-compatible "BiDi" is **not** a separate body shape or a
JWK-keyed handshake — it is a **client-side emulator** that splits a BiDi
RPC into:

- N × **`POST /aiserver.v1.BidiService/BidiAppend`** (unary, one per
  client→server frame), and
- 1 × **`POST /agent.v1.AgentService/RunSSE`** (server-streaming,
  text/event-stream over HTTP/1.1 chunked transfer), or
- 1 × **repeated `POST /agent.v1.AgentService/RunPoll`** if SSE is also
  blocked (corporate-proxy fallback).

All three share a single `request_id` (proto type
`aiserver.v1.BidiRequestId { string request_id = 1; }`). The `request_id`
**is just the `x-request-id` UUID header value** — there is **no AES-GCM
JWK** involved in the basic `RunSSE`/`RunPoll` path in this Cursor build.

The RE catalog's claim that `RunSSE` needs `x-idempotent-encryption-key`
is **wrong for the bare `RunSSE` method** — that header is only mentioned
in the proto for `StreamUnifiedChatWithToolsIdempotent*` variants, none of
which are wired into the IDE's transport map for `agent.v1.AgentService.Run`.

## Source-of-truth files (local)

- `/Applications/Cursor.app/Contents/Resources/app/out/vs/workbench/workbench.desktop.main.js`
  — proto registration, transport-selection wiring
- `/Applications/Cursor.app/Contents/Resources/app/extensions/cursor-always-local/dist/main.js`
  — `cursor-always-local` extension, the **actual** transport-provider
  implementation; contains the BiDi-emulator classes
- `/Users/juncwang/Downloads/new_mcp_study/cursoride2api/src/proto/agent_pb.mjs`
  — base64-encoded `agent.v1` `FileDescriptor` already in this repo;
  contains `BidiRequestId` schema and the `RunSSE`/`RunPoll` service
  method registration

All byte offsets below refer to the two locally-installed bundle files;
they will drift between Cursor releases, so the offsets are sketched as
landmarks, not as stable anchors.

## 1. Encryption-key derivation — there is no encryption key

**Finding (definitive for `RunSSE`/`RunPoll`/`BidiAppend`):**

In Cursor 2.6.20's `workbench.desktop.main.js`, the only key Cursor
manufactures on the IDE side is `clientKey`:

```js
// extensions/cursor-always-local/dist/main.js, offset ~3467593
this.clientKey = Ky.randomBytes(32);   // Ky = require('node:crypto')
// ...later, in every outbound header:
this.clientKey.toString('hex')         // → "x-client-key"
```

This is **32 raw random bytes hex-encoded to a 64-char string**, sent on
the `x-client-key` header. It is NOT an AES-GCM JWK. It is opaque to the
server in any way we can observe; the existing `cursoride2api`
implementation does **not** set this header today, and we have empirical
evidence that `Run` works without it, so it is not load-bearing for
`agent.v1.AgentService/Run` either.

**Where `x-idempotent-encryption-key` appears:**

Grep across the bundle finds zero hits for the literal header name
`x-idempotent-encryption-key`. The token `idempotent` appears only in the
proto-derived method names `StreamUnifiedChatWithToolsIdempotent[SSE|Poll]`.
Those endpoints exist in the proto registration but are **not** wired
into the BiDi-to-SSE method map (see §3) for the agent-chat path.

The original RE catalog (`Cursor API 端点大全.md:511,844-846` and `Cursor
IDE API 逆向工程文档.md:1009-1011`) describes the header as
"`AES-GCM 256-bit JWK generated client-side via
`crypto.subtle.generateKey('AES-GCM', 256)`". In this Cursor build:

1. Neither `cursor-always-local/dist/main.js` nor
   `workbench.desktop.main.js` calls
   `subtle.generateKey('AES-GCM', 256)` followed by `exportKey('jwk')`.
   Their `crypto.subtle` hits are unrelated (SHA-256 hashing, ed25519
   verification, etc.).
2. The only randomness fed into a Cursor backend header is the 32-byte
   `clientKey` for `x-client-key`, which is plain hex, not JWK.

Working hypothesis (cannot be confirmed without a working probe and a
build that actually mounts the `Idempotent` variants): the
`x-idempotent-encryption-key` is for the
`StreamUnifiedChatWithToolsIdempotent*` family only, where the client
posts an encrypted body the server can replay-deduplicate by holding
ciphertext-as-idempotency-token. **For `RunSSE`, it is not needed.**

## 2. Request body shape & framing

### Proto (decoded from `src/proto/agent_pb.mjs` and the Cursor bundle)

```protobuf
// aiserver.v1.BidiRequestId
message BidiRequestId {
  string request_id = 1;
}

// aiserver.v1.BidiAppendRequest
message BidiAppendRequest {
  string data = 1;             // hex-encoded protobuf bytes
  BidiRequestId request_id = 2;
  int64 append_seqno = 3;
}

// aiserver.v1.BidiAppendResponse — empty/unspecified payload

// aiserver.v1.BidiService
service BidiService {
  rpc BidiAppend(BidiAppendRequest) returns (BidiAppendResponse);  // Unary
}

// agent.v1.AgentService (subset)
service AgentService {
  rpc Run(stream AgentClientMessage) returns (stream AgentServerMessage);
  rpc RunSSE(BidiRequestId) returns (stream AgentServerMessage);
  rpc RunPoll(BidiPollRequest) returns (stream BidiPollResponse);
}
```

`BidiPollRequest`/`BidiPollResponse` are not in `agent_pb.mjs` proto
descriptor but they're in `aiserver.v1` namespace per the Cursor bundle.
Field shape is irrelevant to a first probe — Cursor goes for `RunSSE`
unless the `cursor.general.disableHttp1SSE` setting is true.

### How the IDE emulates BiDi over HTTP/1.1 — annotated pseudocode

From `cursor-always-local/dist/main.js` (renamed for clarity):

```js
class BidiEmulatorBase {
  constructor(bidiEndpointMap, bidiServiceClient, fallbackTransport) { ... }

  async stream(service, method, signal, timeoutMs, headers, clientMsgIter, opts) {
    // 1. request_id := headers['x-request-id'] (creates new UUID if absent)
    const requestId = ensureRequestIdHeader(headers);

    // 2. If the method is `Run` or `StreamUnifiedChatWithTools` etc.,
    //    swap to its SSE-variant method-descriptor.
    if (this.bidiEndpointMap[method.name]) {
      method = this.bidiEndpointMap[method.name];  // → RunSSE / RunPoll
    }

    // 3. Open the server-streaming response (SSE or Poll-based).
    const responsePromise = this.connectForStartOfStream(
      requestId, service, method, signal, timeoutMs, headers, opts);

    // 4. In parallel, drain client→server messages by writing each one
    //    to BidiAppend.
    this.startYieldingInputsToTheServer(
      requestId, clientMsgIter, timeoutMs, signal, headers, ...);

    return await responsePromise;
  }

  async startYieldingInputsToTheServer(requestId, iter, ...) {
    let seqno = 0n;
    for await (const clientMsg of iter) {
      const dataHex = Buffer.from(clientMsg.toBinary()).toString('hex');
      await this.bidiClient.bidiAppend({
        requestId: { requestId },
        appendSeqno: seqno,
        data: dataHex,
      }, { headers });
      seqno += 1n;
    }
  }
}

class SseEmulator extends BidiEmulatorBase {           // qPe
  async connectForStartOfStream(requestId, service, method, ...) {
    // method here is already swapped → RunSSE, kind=ServerStreaming.
    // Single client-side BidiRequestId frame, server streams responses.
    const body = framedSingleton(new BidiRequestId({ requestId }));
    return this.transport.stream(service, method, ..., body, opts);
  }
}

class PollEmulator extends BidiEmulatorBase {          // PPe
  async connectForStartOfStream(requestId, service, method, ...) {
    // Poll variant: server returns BidiPollResponse {data, seqno, eof}
    // After each batch the client re-POSTs to /RunPoll for more.
    // O is overridden to BidiPollResponse; client wraps and decodes.
    ...
  }
}
```

(Class names in the bundle: base = `bPe`, SSE = `qPe`, Poll = `PPe`,
factory = `LPe`. The mapping initializer is at `_bidiEndpointToSSEMethodMap`
and `_bidiEndpointToPollMethodMap`.)

### Mapping table (verbatim from bundle, ~offset 3465931)

```js
_bidiEndpointToSSEMethodMap = {
  'StreamUnifiedChatWithTools':            streamUnifiedChatWithToolsSSE,
  'StreamUnifiedChatWithToolsIdempotent':  streamUnifiedChatWithToolsIdempotentSSE,
  'StreamBidi':                            streamBidiSSE,
  'StreamStt':                             streamSttSSE,
  'StreamBugBotAgentic':                   streamBugBotAgenticSSE,
  'StreamUiBestOfNJudge':                  streamUiBestOfNJudgeSSE,
  'Run':                                   runSSE,                  // ← the one we care about
}
```

So when Cursor wants a `Run` BiDi stream over HTTP/1.1, it opens a single
`POST /agent.v1.AgentService/RunSSE` with body
`BidiRequestId{request_id: <uuid>}`, and pumps every `AgentClientMessage`
that *would* have gone into the BiDi stream as a separate unary
`POST /aiserver.v1.BidiService/BidiAppend` with `data` = the hex of that
message's binary protobuf encoding.

### Wire format on the SSE side

`RunSSE`'s `kind: ServerStreaming` plus ConnectRPC's Connect-Web protocol
means: the server sends Connect-framed envelopes inside a chunked
`Content-Type: application/connect+proto` (or `+json`) response. **Not
literally `text/event-stream`** — Connect-Web's server-streaming is
chunked envelopes, not SSE-format. The repository's existing `Run`
client (`src/cursor-agent.js`) already speaks this framing for BiDi; the
RunSSE response stream is the same envelope sequence minus the bidi half.

For Connect-Web framing details see existing
`Cursor IDE API 逆向工程文档.md:98-144`.

## 3. Headers — required & optional

Same set as `Run` today, plus:

| Header | Source | Required? |
|---|---|---|
| `authorization: Bearer <token>` | from `token.json.accessToken` | yes |
| `x-cursor-checksum: <base64ts><machineId>/<macMachineId>` | `generateChecksum()` from `src/cursor-client.js` | yes |
| `x-cursor-client-version: 2.6.20` | env / config | yes |
| `x-request-id: <uuid>` | per call (this is **also** the `request_id` carried in the body) | yes |
| `content-type: application/connect+proto` *or* `application/connect+json` | depends on framing | yes |
| `connect-protocol-version: 1` | constant | yes |
| `x-cursor-streaming: true` | Cursor extension sets on every outbound | observed; effect unverified |
| `x-client-key: <32-byte-random-hex>` | per-session random buffer | observed; probably for sandbox/telemetry |
| `x-session-id: <uuid>` | per channel | observed |
| `x-cursor-timezone`, `x-cursor-client-{os,arch,type,device-type}`, `x-cursor-canary` | fingerprint padding | optional |
| `x-amzn-trace-id: Root=<uuid>` | per call | optional |
| `x-idempotent-encryption-key` | **NOT NEEDED for `RunSSE`** | no (see §1) |

The **same `x-request-id` UUID must appear on**: the `RunSSE` open, every
`BidiAppend` call for that conversation, AND any `RunPoll` retry-poll for
the same exchange. That UUID, not any encryption key, is what joins the
three RPCs into one logical BiDi stream on the server side.

## 4. HTTP/1.1 viability — protocol-level analysis

**Yes, the design is HTTP/1.1-native** by construction:

- `BidiAppend` is **Unary** Connect-RPC. Unary Connect maps to a normal
  HTTP/1.1 request with a request body and a single response. No
  multiplexing, no streaming. Compatible with HTTP/1.1, HTTP/1.0 in
  theory.
- `RunSSE` is **Server Streaming**, which in Connect-Web is sent as
  HTTP/1.1 `Transfer-Encoding: chunked` plus the Connect envelope frames
  in the response body. This is the same shape as plain SSE in terms of
  hop-by-hop semantics — it requires only that intermediaries not
  buffer the response body. Hence the user-facing setting
  `cursor.general.disableHttp1SSE: "Disable HTTP/1.1 SSE for agent chat.
  This increases resource utilization and latency, but is useful if
  you're behind a corporate proxy that does not support HTTP/1.1 SSE
  streaming responses."` (workbench.desktop.main.js, offset ~44888094).
- `RunPoll` is **Server Streaming** but its emulator (`PollEmulator`)
  reissues fresh HTTP/1.1 requests after each batch — that's the
  "Poll-on-no-SSE" fallback for proxies that can't even pass chunked
  bodies through.

**ELB layer**: `Cursor API 端点大全.md:840` ("HTTP/2 是必须的 — 不支持
HTTP/1.1") and the existing SURVEY measurement
(`scaffolding/SURVEY.md:53-60`) describe ELB returning HTTP 464 to
HTTP/1.1 requests against `/agent.v1.AgentService/Run`. **That ELB
target group serves `Run` only**; the empirical 464 was specifically on
the BiDi path. The new `RunSSE` and `BidiAppend` endpoints are
**different gRPC methods** living on the same `api2.cursor.sh` ALB, and
the ALB's per-target-group HTTP-version policy applies per
method-path-prefix, not per host. **It is plausible (but unverified)
that `/aiserver.v1.BidiService/BidiAppend` and
`/agent.v1.AgentService/RunSSE` are configured to accept HTTP/1.1.**

The strongest indirect evidence that they accept HTTP/1.1 is the user
setting being shipped at all: shipping a "Disable HTTP/1.1 SSE" toggle
implies the default — HTTP/1.1 SSE for agent chat — is expected to
work on Cursor's backend.

**Caveat**: we have NOT run a live probe. The ALB may still reject
HTTP/1.1 specifically on these endpoint paths. The probe in
`run-sse-probe.mjs` exists to test exactly that.

## 5. Rate-limit hypothesis

No direct RE evidence either way in the bundle. Indirect signals:

1. **Separate proto paths**: `Run` lives at `/agent.v1.AgentService/Run`,
   `RunSSE` at `/agent.v1.AgentService/RunSSE`, `BidiAppend` at
   `/aiserver.v1.BidiService/BidiAppend`. ALB target groups can have
   independent rate-limit rules per path. There is no proof Cursor has
   set them differently, but the architecture allows it.
2. **Per-session vs per-request budgets**: The `Run` path's
   `ERROR_PRO_USER_RATE_LIMIT_EXCEEDED` is documented as
   per-account-per-rolling-window (`scaffolding/pool/FOCUS.md:218`).
   `RunSSE` is logically the same "one user message → one agent turn"
   workload, so a backend-side per-account quota would naturally count
   them together. But if the limiter is in front of the gRPC handler and
   keyed on method name (a common Envoy/Linkerd pattern), they'd have
   separate buckets.
3. **`x-cursor-streaming: true` header**: the IDE sets this on every
   outbound request. The server side might use it as a hint to apply a
   "streaming" quota that's separate from the BiDi-only quota. Pure
   speculation.
4. **Feature gates** observed in the bundle: `bidi_append_fix` (default
   false), `retry_interceptor_disabled` (default false),
   `retry_interceptor_enabled_for_streaming` (default true),
   `http2_disable_pings`, `http1_keepalive_disabled`. None directly
   reveal rate-limit policy.

**Net**: the architecture *could* expose a different rate-limit bucket,
but we have **zero proof from the bundle that Cursor's backend
actually splits them**. This is the single most valuable thing to test
with a probe.

## 6. Cross-references to prior art

This repo's `REFERENCES.md` was promised in the brief but does not exist
on `feat/ratlc-mvp`. Other Cursor RE projects we are aware of from the
existing `cursoride2api` proto / docs:

- The `agent.proto` descriptor embedded in `src/proto/agent_pb.mjs`
  itself is the canonical RE artifact — it includes the full
  `BidiRequestId` / `RunSSE` / `RunPoll` schema in its base64 file
  descriptor.
- `Cursor IDE API 逆向工程文档.md:1056-1058` lists `Eye`,`X$e`,`eqe`
  as `BidiRequestId`/`BidiPollRequest`/`BidiPollResponse` (older minifier
  IDs from a different Cursor build).
- `cursor-api-go`, `cursor-reverse-engineering`, etc., were
  cross-referenced in prior versions of `REFERENCES.md`; they have not
  been (re-)verified during this session.

If/when those references are checked: any project that already invokes
`RunSSE` will be the highest-value source, since they will already have
solved the `BidiAppend` + `RunSSE` pairing in code.

## 7. What to probe next (priority order)

1. **HTTP/1.1 reachability of `BidiAppend`** — single unary POST, no
   stream needed. Most likely to surface a hard ELB-layer block (464)
   if one exists. Cheap test, ~80 ms per attempt.
2. **HTTP/1.1 reachability of `RunSSE`** — open a single SSE stream
   with a random `request_id`, no `BidiAppend`s, expect either the
   server to wait forever (good, means SSE accepted, just no run was
   started) or a fast 4xx/5xx.
3. **End-to-end one-shot conversation via `BidiAppend`+`RunSSE`** —
   write a `runRequest` to `BidiAppend`, immediately open `RunSSE`,
   reply to `requestContextArgs` with another `BidiAppend`, observe
   `textDelta`/`turnEnded`. If it works, do the same with HTTP/1.1 to
   confirm the protocol layer.
4. **Rate-limit comparison (out of scope of this RE doc)** — fire N
   parallel `Run` requests vs N parallel `RunSSE` requests after a
   recovery window. Different success rates → different rate-limit
   buckets.
5. **Idempotent variant probe** — if `RunSSE` shares `Run`'s rate-limit
   bucket, the only remaining lever is the `StreamUnifiedChatWithToolsIdempotent*`
   family, which is the one that *does* mention
   `x-idempotent-encryption-key` in the proto / header registry. That
   would require RE of `IDempotency` key derivation; it has not been
   found in this build (see §1).

## 8. Things this document is NOT confident about

- **Connect-Web vs `text/event-stream` framing on the wire**. Connect's
  spec defines a streaming response that is wire-compatible with HTTP/1.1
  chunked transfer, but the response framing inside is Connect envelopes,
  not SSE `event:` lines. The probe should verify this empirically.
- **Whether `BidiAppend`'s `data` field is a *hex string* of protobuf
  bytes, or base64**. The bundle uses `Buffer.from(...).toString('hex')`,
  but there's some plausibility that the server also accepts base64; we
  use hex in the probe stub to match the bundle exactly.
- **Whether `appendSeqno` must start at 0 and be strictly monotonically
  increasing**, or whether the server tolerates gaps/reorder. The
  bundle uses `BigInt(0)` start with `++`, suggesting strict order
  matters.

---

End of document.
