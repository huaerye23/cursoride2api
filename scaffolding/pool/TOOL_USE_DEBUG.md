# TOOL_USE_DEBUG — Why claude-code never POSTs back tool_result

Written 2026-05-13 from a static read of `api-server.mjs`, `pool-manager.mjs`,
`tool-translator.mjs`, `bridge-worker.mjs`, the Anthropic Messages-streaming
spec (https://platform.claude.com/docs/en/build-with-claude/streaming), the
@anthropic-ai/sdk TS source (`MessageStream.ts` and `messages.ts` Usage
types), and a working reference proxy (`1rgs/claude-code-proxy`).

P2 scope only — HTTP/1.1 / SSE-vs-Poll is not touched here.

---

## TL;DR

The most likely silent-drop cause is **the wrong `model` field on
`message_start`**. Our code emits `'claude-opus-4-7-thinking-max-fast'` (the
internal Cursor-side model id) into `message_start.message.model` when the
request body had no `body.model`. claude-code asks for an Anthropic-shaped
model id (e.g. `claude-opus-4-7-...`) and may validate or canonicalize this.

Close second: **missing `cache_creation_input_tokens` / `cache_read_input_tokens`
on `message_start.message.usage`**. The Anthropic SDK type for `Usage` declares
both as required (nullable) properties — claude-code is built on `@anthropic-ai/sdk`
and reads these eagerly; their absence has been observed to break sibling proxies.

Third: a stale `pendingMcpInfo` / `currentRequestId` problem in
`bridge-worker.mjs`: after we emit `tool_use` to claude-code and call
`finishMessage()` (which fully closes the HTTP response and deletes the
reqHandler), the **worker still has** `pendingMcpInfo` set and is sitting
busy. When claude-code POSTs `/v1/messages` with `[tool_result]`, the
api-server creates a NEW request, the pool routes to the same worker, and the
worker happily resumes — UNLESS the new POST is being parsed wrong (e.g.
`findToolResult` only grabs the first tool_result; multi-tool turns lose
data).

Best single first thing to try: fix the model echo + add the four usage
fields, see if claude-code starts POSTing back.

---

## 1. Spec deviations in our api-server, ranked

Refs:
- Spec stream example (tool_use turn): see fetched copy lines 1042–1121 of
  `https://platform.claude.com/docs/en/build-with-claude/streaming`.
- @anthropic-ai/sdk `Usage` type (TS): all six fields are declared required
  as properties; `cache_*` and `server_tool_use` are nullable but must be
  present.

### D1 (LIKELY-FATAL) — Wrong model on message_start

`api-server.mjs:154`

```js
content: [], model: model || 'claude-opus-4-7-thinking-max-fast',
```

- `model` here is destructured from `body` on line 114. Most claude-code
  requests *do* include `model`, but some (token-count probes, system-reminder
  pings) may not. The fallback string is the *Cursor-internal* model id, not
  an Anthropic public id.
- Spec example (line 1042 of cached page) always echoes back the model the
  client requested verbatim: `"model":"claude-opus-4-7"`.
- More importantly: if `body.model` is `claude-opus-4-7-20260101` we echo
  that fine; but if absent, the fallback string can be interpreted by
  claude-code as "not the model I asked for" → it may discard the response.

### D2 (LIKELY-FATAL) — Missing usage fields on message_start

`api-server.mjs:156`

```js
usage: { input_tokens: extractTextFromContent(lastMsg.content).length / 4 | 0, output_tokens: 0 },
```

Spec example (cached line 1623):

```json
"usage":{"input_tokens":2679,"cache_creation_input_tokens":0,"cache_read_input_tokens":0,"output_tokens":3}
```

@anthropic-ai/sdk `Usage` declares **six required properties** (most
nullable but present): `input_tokens`, `output_tokens`,
`cache_creation_input_tokens`, `cache_read_input_tokens`, `server_tool_use`,
`service_tier`. We send only the first two.

The reference proxy `1rgs/claude-code-proxy/server.py:849-855` explicitly
sends all four token fields on `message_start`:
```python
'usage': {
    'input_tokens': 0,
    'cache_creation_input_tokens': 0,
    'cache_read_input_tokens': 0,
    'output_tokens': 0
}
```

If claude-code reads `usage.cache_read_input_tokens` and gets `undefined`,
the access pattern `cache_read_input_tokens + ...` produces `NaN`, which can
trip downstream guards (cost accounting, telemetry POST) and break the
processing chain before tool_use is handed to the executor.

### D3 (LIKELY-FATAL) — Missing usage fields on message_delta

`api-server.mjs:208`

```js
usage: { input_tokens: 0, output_tokens: outputTokens },
```

Spec example (cached line 1696):

```json
"usage":{"input_tokens":10682,"cache_creation_input_tokens":0,"cache_read_input_tokens":0,"output_tokens":510,"server_tool_use":{"web_search_requests":1}}
```

The `MessageDeltaUsage` type also declares all four token fields + 
`server_tool_use` as required. Even though `input_tokens` is conventionally
zero/null on the delta, the **field must be present**. We at least include
`input_tokens: 0`. We don't include the cache fields or server_tool_use.

Note: the spec warning on the page says "The token counts shown in the
`usage` field of the `message_delta` event are *cumulative*." That's an
informational note for SDK accumulators; doesn't change the shape, but does
hint that claude-code is reading these fields and accumulating them.

### D4 (PROBABLE) — Empty content block on tool_use

`api-server.mjs:186-199`

```js
function emitToolUseBlock(anthropicId, toolName, args) {
  stopTextBlock();
  blockIdx++;
  sseWrite(res, 'content_block_start', {
    type: 'content_block_start', index: blockIdx,
    content_block: { type: 'tool_use', id: anthropicId, name: toolName, input: {} },
  });
  sseWrite(res, 'content_block_delta', {
    type: 'content_block_delta', index: blockIdx,
    delta: { type: 'input_json_delta', partial_json: JSON.stringify(args || {}) },
  });
  sseWrite(res, 'content_block_stop', { type: 'content_block_stop', index: blockIdx });
  toolUseEmitted = true;
}
```

Real Anthropic sends:
1. `input_json_delta` with `partial_json: ""` (empty preamble; cached
   line 1096 and 1647 — present in BOTH the tool-use and web-search examples).
2. Then one or more deltas with chunked JSON.

We send a single delta with the entire JSON. Per the spec accumulator and
the SDK source (`MessageStream.ts`), this is *accepted* because the
partialParse function handles complete JSON fine. **But** if claude-code's
input-streaming UX relies on the empty-preamble delta to "open" the tool
input pane and then expects further deltas, our single-delta path may not
match the state machine. Lower confidence — likely not a hard fail, but
worth fixing.

Also: with no text block before the tool_use, blockIdx starts at 0 (first
ever block). Spec example always shows the tool_use at `index: 1+` after
some text. This is *probably* fine — there's no rule against it — but the
spec example never demonstrates "tool_use at index 0 with no preceding text".

### D5 (POSSIBLE) — Missing initial text block / "skipping content"

`api-server.mjs:215` calls `startMsg()` then immediately returns. The first
content_block_start is only emitted lazily by `emitTextDelta` or
`emitToolUseBlock`.

Spec basic example (cached lines 647-672) and reference proxy
(`1rgs/claude-code-proxy/server.py:860`) both open an empty text block at
index 0 *immediately* after message_start. claude-code may not depend on
this — but every working reference does it.

### D6 (POSSIBLE) — `service_tier` field

`Usage` type requires `service_tier`. Not in our message_start. Same
nullable-but-present rule applies as D2.

### D7 (UNLIKELY-FATAL but worth fixing) — message_stop has no extras

`api-server.mjs:210` sends `{ type: 'message_stop' }`. Spec matches exactly
(cached line 670 and 1120). OK.

### D8 (POOL_BUG, not SSE) — `findToolResult` returns only first match

`api-server.mjs:88-98`:

```js
function findToolResult(content) {
  if (!Array.isArray(content)) return null;
  for (const c of content) {
    if (c.type === 'tool_result') {
      const text = ...;
      return { tool_use_id: c.tool_use_id, text };
    }
  }
  return null;
}
```

If claude-code ever batches multiple tool_results into a single user message
(it does in practice when parallel tool_use happens), we only deliver one
and silently swallow the rest. Not the cause of the current single-tool hang
but will bite later.

### D9 (POOL_BUG) — `currentRequestId` cleared too late

`pool-manager.mjs:283` (after writing tool_use to client) leaves
`ch.currentRequestId` set. Then the api-server's request handler runs
`finishMessage()` and `reqHandlers.delete(requestId)` — the **HTTP socket
closes**. But pool-manager still holds the mapping in `requestClient`. When
claude-code POSTs back with a *new* requestId, that's fine, but if anything
on the original requestId fires (a late error, a heartbeat), it goes to a
dead client and is silently dropped.

This is unlikely to cause the described silence — the bug is that
claude-code *never* POSTs at all — but it's a latent leak.

---

## 2. Top-3 candidate root causes (ranked)

1. **D1 — Wrong fallback `model` echo on `message_start`** (severity:
   likely-fatal when claude-code POSTs without `body.model`, e.g. a tools-only
   probe or its first turn). Cursor's internal model id is not a recognized
   Anthropic id; claude-code may abort silently.

2. **D2 + D3 — Missing required usage fields** (severity: likely-fatal).
   `cache_creation_input_tokens`, `cache_read_input_tokens`, `server_tool_use`,
   `service_tier` are all declared required on the SDK's `Usage` type. Any
   downstream `+`-arithmetic on `undefined` yields `NaN`, which may trip cost
   guards in claude-code and short-circuit response handling before the
   `tool_use` block is dispatched to the executor.

3. **D4 — Single-delta input_json** (severity: probable). Possibly safe per
   SDK accumulator but doesn't match the empirical wire shape; combined with
   D5 (no preceding text block), claude-code may treat the response as
   malformed.

---

## 3. Concrete fix diffs

All Edit-tool-ready. Apply in this order — D2/D3 first (single, additive
fix), D1 second, D4 third.

### Fix A — Usage fields on message_start (D2) and message_delta (D3)

Edit `scaffolding/pool/api-server.mjs`:

**old_string** (the entire `startMsg` body — must include line breaks
exactly):
```js
  function startMsg() {
    sseWrite(res, 'message_start', {
      type: 'message_start',
      message: {
        id: messageId, type: 'message', role: 'assistant',
        content: [], model: model || 'claude-opus-4-7-thinking-max-fast',
        stop_reason: null, stop_sequence: null,
        usage: { input_tokens: extractTextFromContent(lastMsg.content).length / 4 | 0, output_tokens: 0 },
      },
    });
    sseWrite(res, 'ping', { type: 'ping' });
  }
```

**new_string**:
```js
  function startMsg() {
    sseWrite(res, 'message_start', {
      type: 'message_start',
      message: {
        id: messageId, type: 'message', role: 'assistant',
        content: [], model: model || 'claude-opus-4-7',
        stop_reason: null, stop_sequence: null,
        usage: {
          input_tokens: extractTextFromContent(lastMsg.content).length / 4 | 0,
          output_tokens: 0,
          cache_creation_input_tokens: 0,
          cache_read_input_tokens: 0,
          server_tool_use: null,
          service_tier: 'standard',
        },
      },
    });
    sseWrite(res, 'ping', { type: 'ping' });
  }
```

Notes:
- Default model id changed to a public-shape Anthropic id (`claude-opus-4-7`),
  not the Cursor-internal one. claude-code echoes back the model it asked for
  via `body.model` in 99% of paths, but probes without `model` now get a
  sensible default. (See D1.)
- All four `Usage` extras set to `0` / `null` / `'standard'` as the SDK
  default sentinels.

### Fix B — Usage fields on message_delta (D3)

**old_string**:
```js
    sseWrite(res, 'message_delta', {
      type: 'message_delta',
      delta: { stop_reason: stopReason, stop_sequence: null },
      usage: { input_tokens: 0, output_tokens: outputTokens },
    });
```

**new_string**:
```js
    sseWrite(res, 'message_delta', {
      type: 'message_delta',
      delta: { stop_reason: stopReason, stop_sequence: null },
      usage: {
        input_tokens: 0,
        output_tokens: outputTokens,
        cache_creation_input_tokens: 0,
        cache_read_input_tokens: 0,
        server_tool_use: null,
      },
    });
```

Note: `service_tier` is not on `MessageDeltaUsage` — intentionally omitted.

### Fix C — Empty preamble + split JSON on tool_use (D4)

Replaces the single-delta with a 2-delta sequence: empty preamble, then full
JSON. This matches the real wire shape without committing to per-token
chunking.

**old_string**:
```js
  function emitToolUseBlock(anthropicId, toolName, args) {
    stopTextBlock();
    blockIdx++;
    sseWrite(res, 'content_block_start', {
      type: 'content_block_start', index: blockIdx,
      content_block: { type: 'tool_use', id: anthropicId, name: toolName, input: {} },
    });
    sseWrite(res, 'content_block_delta', {
      type: 'content_block_delta', index: blockIdx,
      delta: { type: 'input_json_delta', partial_json: JSON.stringify(args || {}) },
    });
    sseWrite(res, 'content_block_stop', { type: 'content_block_stop', index: blockIdx });
    toolUseEmitted = true;
  }
```

**new_string**:
```js
  function emitToolUseBlock(anthropicId, toolName, args) {
    stopTextBlock();
    blockIdx++;
    sseWrite(res, 'content_block_start', {
      type: 'content_block_start', index: blockIdx,
      content_block: { type: 'tool_use', id: anthropicId, name: toolName, input: {} },
    });
    // Empty preamble — matches the real Anthropic wire shape (spec
    // example: input_json_delta with partial_json: "" appears first).
    sseWrite(res, 'content_block_delta', {
      type: 'content_block_delta', index: blockIdx,
      delta: { type: 'input_json_delta', partial_json: '' },
    });
    sseWrite(res, 'content_block_delta', {
      type: 'content_block_delta', index: blockIdx,
      delta: { type: 'input_json_delta', partial_json: JSON.stringify(args || {}) },
    });
    sseWrite(res, 'content_block_stop', { type: 'content_block_stop', index: blockIdx });
    toolUseEmitted = true;
  }
```

### Fix D (optional, lower priority) — Open text block immediately after message_start (D5)

If A/B/C don't resolve it, also emit an empty text content_block_start +
content_block_stop *before* the tool_use, so every message has at least one
text block at index 0. This mirrors both the spec example and 1rgs's proxy.

Most surgical: in `startMsg`, after `sseWrite(res, 'ping', ...)`, add:

```js
    // Open empty text block at index 0 — matches the canonical wire shape.
    blockIdx = 0;
    sseWrite(res, 'content_block_start', {
      type: 'content_block_start', index: 0,
      content_block: { type: 'text', text: '' },
    });
    textBlockOpen = true;
```

Then `startTextBlock` should detect we already have one (the existing
`textBlockOpen` flag handles this — verify the `blockIdx++` accounting).
Lower confidence this matters; do not apply unless A/B/C fail.

### Fix E (out of scope but worth flagging) — D8 multi-tool_result

`findToolResult` returns only the first tool_result. If claude-code ever
batches results from parallel tool_use, we drop the rest. Not the cause of
the current hang (single tool_use), but file an issue.

---

## 4. Verification plan via `tap.mjs`

Bring up the stack in translate mode with the tap in front:

```bash
cd /Users/juncwang/Downloads/new_mcp_study/cursoride2api
./scaffolding/pool/ratlc down
POOL_TOOL_MODE=translate POOL_CONCURRENT_OPENS=1 ./scaffolding/pool/ratlc up 1
# wait for ready=1
TAP_PORT=4343 TAP_TARGET=http://127.0.0.1:4242 node scaffolding/pool/tap.mjs &
ANTHROPIC_BASE_URL=http://127.0.0.1:4343 claude --dangerously-skip-permissions --effort max
```

Inside claude, run the failing prompt: ask it to Read a path that doesn't
exist from its CWD.

### Bytes to look for in `/tmp/ratlc-tap/<stamp>-<id>.jsonl`

**Pre-fix capture (current buggy behavior):**

- `sse_event` with `event=message_start` whose `data.message.usage` has
  only `input_tokens` + `output_tokens`. **No** `cache_*`.
- `sse_event` with `event=content_block_start` for `tool_use`.
- `sse_event` with `event=content_block_delta` carrying entire JSON in one
  `partial_json`.
- `sse_event` with `event=content_block_stop`.
- `sse_event` with `event=message_delta` and `message_stop`.
- `response_end` recorded.
- **No follow-up POST** in any later capture file — i.e. the next file in
  `/tmp/ratlc-tap/` is from the user's next prompt, not from claude-code
  posting back a tool_result. If a `client_disconnect` line appears between
  `sse_event` events, the issue is even earlier (claude-code aborted
  mid-stream).

**Post-fix capture (success):**

- `sse_event` `message_start.message.usage` contains all of `input_tokens`,
  `output_tokens`, `cache_creation_input_tokens`, `cache_read_input_tokens`,
  `server_tool_use`, `service_tier`.
- For tool_use: two `content_block_delta` events with `input_json_delta`,
  first with `partial_json:""`, second with the JSON body.
- `message_delta.usage` has all four token fields + `server_tool_use:null`.
- Within ~5 s of `response_end`, a **new** capture file appears containing:
  - `request_body.messages[-1].content` is an array including
    `{type:"tool_result", tool_use_id:"toolu_...", content:"..."}`.
  - That tool_use_id matches the one we minted in the previous capture's
    tool_use `content_block_start`.

### Behavior change that confirms the fix

- The pool's `toolUseIndex` (visible via `/metrics`) returns to 0 after the
  tool_result POST is processed.
- claude-code shows the file-read result (or an ENOENT error) in its UI.
- The channel returns to `state: ready` instead of stuck `busy`.

### If post-fix capture still hangs

- Look for `client_disconnect` line. If present, claude-code is bailing on
  the SSE stream before reading the tool_use — re-check D5 (open empty text
  block first; apply Fix D).
- Look for `response_headers.status != 200` or
  `response_headers.content-type` missing `text/event-stream`.
- Compare every field of `message_start.message` byte-for-byte against the
  spec example (cached fetch lines 1042). Particularly check that
  `stop_reason: null` is literal `null`, not the string `"null"`. (Our
  `JSON.stringify` handles this fine; just verify in the capture.)
- Inspect the next request that claude-code DOES make. If it's a
  `/v1/messages` POST without a `tool_result` (a fresh user turn), then
  claude-code accepted the SSE but its tool-executor refused to run the
  tool — possibly because the *name* it received (`Read`) is one claude-code
  has marked unsafe in non-interactive mode (e.g. requires a pre-approved
  permission file).

---

## 5. Anti-patterns to avoid

- **Don't add `data: [DONE]` after `message_stop`.** That's the
  OpenAI-style terminator. Anthropic SDKs treat unknown `data:` lines as
  parse errors. (1rgs's proxy does this and gets away with it because
  litellm-OpenAI clients tolerate it; claude-code may not.)
- **Don't change `Connection: keep-alive` to `Connection: close`.**
  claude-code expects a long-lived SSE stream within the request lifecycle.
  Closing the underlying socket after `message_stop` is fine; the
  *application-level* end is `message_stop`, and `res.end()` from Node
  closes the connection naturally on HTTP/1.1.
- **Don't add `anthropic-version` to response headers.** It's a request
  header. Spurious response headers may trip clients that snapshot request
  metadata.
- **Don't synthesize a fake `content_block_start` with `type: "text"` and
  some non-empty text** to "warm up the stream". claude-code will then
  expect that text in the final assembled message and may render it.
- **Don't compute output_tokens from delta string length and forget about
  tool_use tokens.** Our running counter `outputTokens` only counts
  text_delta bytes; tool_use args aren't added. This is fine for now (cost
  accounting only), but if a future debugging session sees `output_tokens:0`
  on a tool_use turn and panics, that's expected.
- **Don't try to "fix" by reordering events to send `content_block_stop`
  for the tool_use AFTER `message_delta`.** Spec requires per-block
  start/stop pairing strictly before message_delta.
- **Don't go probe Cursor's `RunSSE` endpoint as part of P2.** That's P1
  scope; separate agent.
- **Don't trust the @anthropic-ai/sdk being lenient to mean claude-code is
  lenient.** claude-code is built on the SDK but adds its own validation
  layers (cost accounting, transcript writing, tool-execution policy)
  before tool_use is dispatched. The SDK accumulator accepting a malformed
  message ≠ claude-code dispatching a tool from it.

---

## Source references used

- Cached Anthropic spec (full body):
  `~/.claude/projects/-Users-juncwang-Downloads-new-mcp-study/4fad31c9-797c-440c-a333-e299baef100d/tool-results/toolu_01G8hWfXwDZyCp3bEAet9UXU.txt`
  — see lines 647-672 (basic stream), 1042-1121 (tool_use stream), 1623-1700
  (web_search stream w/ cache fields).
- @anthropic-ai/sdk TS `MessageStream.ts` (parser tolerance for partial JSON
  and block ordering): fetched live during this session.
- @anthropic-ai/sdk TS `messages.ts` `Usage` type (six-field requirement
  with nullables): fetched live during this session.
- Reference working proxy: `1rgs/claude-code-proxy/server.py` lines
  834-1093 — note the explicit four-field `usage` on message_start, the
  immediate empty text block at index 0, and the multi-delta tool_use
  pattern.
- Bug-side code: all line numbers above refer to
  `/Users/juncwang/Downloads/new_mcp_study/cursoride2api/scaffolding/pool/api-server.mjs`
  at the current `feat/ratlc-mvp` HEAD as read during this session.
