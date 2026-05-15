# WebSearch / WebFetch translation review

**Scope:** Native interaction-query handling for `webSearchRequestQuery` and `webFetchRequestQuery`, plus the `fetchArgs` passthrough path. Post-commit `f6cc478` ("approve WebSearch when passthroughNativeTools is on").
**TL;DR:** One architectural asymmetry, one real footgun (already observed in the wild), three smaller issues.

---

## Context

Two distinct paths exist for native web tools:

1. **`InteractionQuery`** path -- Cursor's backend asks for permission via `webSearchRequestQuery` / `webFetchRequestQuery`. If we approve, Cursor runs the call **server-side** and streams results back via `interactionUpdate.tool_call_started/Delta/Completed`. The client never sees a tool_use.
2. **`ExecServerMessage`** path -- Cursor's backend emits `fetchArgs` (and `shellArgs`, `readArgs`, etc.). In passthrough mode these become MCP-shape `tool_use` events forwarded to the client; the client returns a `tool_result` that we adapt to the matching native result schema.

`f6cc478` added the approve branch for `webSearchRequestQuery` only. Everything else in `handleInteractionQuery` still rejects.

## Issue 1 -- WebSearch passthrough is invisible to the client (architectural)

**Files:** `src/cursor-agent.js:854-870` (approve), `src/cursor-agent.js:1571-1574` (drop), `scaffolding/pool/api-server.mjs:423, :493` (telemetry).

Flow when the model emits WebSearch with `passthroughNative=true`:

```
model -> webSearchRequestQuery -> handleInteractionQuery (APPROVE)
       -> Cursor backend runs search
       -> interactionUpdate.tool_call_started/Delta/Completed
       -> dropped at cursor-agent.js:1571-1574 ("Misc updates we don't render")
       -> model context now contains search results, client transcript does not
```

Consequences:

- claude-code never sees a `tool_use` block for the search; the assistant turn looks like the model "just knew" the answer.
- No audit trail, no transcript line, no cache key on the client side.
- `usage.server_tool_use` stays `null` in the SSE we emit -- Anthropic-side telemetry under-counts.
- Hard to reproduce or debug downstream model behavior that was actually driven by an invisible search result.

**Suggested fix:** in the `interactionUpdate` handler in `cursor-agent.js`, intercept the `toolCallStarted/Delta/Completed` cases when the inner tool is `webSearchToolCall`. Synthesize a fake MCP tool_use into the existing `onMcpCall` callback so the api-server emits a real `tool_use` content block to the client, and emit a matching synthetic `send_tool_result` back to Cursor (no-op since Cursor already has the result -- but the round-trip stays observable from the client side).

If that's too invasive, at minimum log every approved WebSearch with the search term and result-byte-count behind `CURSOR_LOG_INTERACTION=1`, and increment a counter exposed via `/metrics`.

## Issue 2 -- WebSearch results reference paths on the wrong filesystem (real footgun)

**Empirically observed; not yet captured in any review note.**

Cursor's backend WebSearch implementation writes scraped page contents to files on **Cursor's backend FS** (under a relative path like `agent-tools/<uuid>.txt`) and returns text into the model context that reads:

```
Title: ...
URL: ...
Content: Full page text written to file: agent-tools/<uuid>.txt
Size: 36.1 KB, 11933 lines
Use shell / grep / read_file on this path to inspect the page; no follow-up fetch is needed.
```

When the model trusts that text and issues `Read` or `Bash ls agent-tools/<uuid>.txt` on the client side, the file does not exist -- or exists as 0 bytes if the model previously created it via `Write`. The model cannot distinguish the failure from a transient FS issue and proceeds to hallucinate from the in-context summary.

This is not a hypothetical: I reproduced it directly in a recent session -- `ls -la agent-tools/` showed every UUID-named file at 0 bytes, despite WebSearch claiming multi-KB writes. The model then confabulated leaderboard numbers because it had no way to verify the underlying source.

**Suggested fix:** the cleanest option is to intercept `toolCallCompleted` for the WebSearch case (depends on Issue 1's hook) and rewrite the result text -- strip the "written to file: <path>" line, or replace it with an inlined excerpt of the body. Without the hook, document this in CLAUDE.md / system prompt so the model knows not to trust those paths.

**Severity:** silent correctness failure -- model output looks confident but is uncorroborated. Worse than a loud failure.

## Issue 3 -- WebFetch passthrough hardcodes the prompt (lossy)

**File:** `src/cursor-agent.js:610-619`.

```javascript
if (msgCase === 'fetchArgs') {
  nativeExecKinds.set(execId, { kind: 'fetch', url: msgValue?.url || '' });
  onMcpCall({
    id, execId,
    toolCallId: `native-fetch-${execId.slice(0, 8)}`,
    toolName: 'WebFetch',
    args: { url: msgValue?.url || '', prompt: 'Summarize this content.' },
  });
  return 'fetch-passthrough';
}
```

claude-code's `WebFetch` takes `{url, prompt}` where `prompt` drives the secondary-model extraction. We discard whatever question the model wanted answered and hardcode `"Summarize this content."` -- the model gets a summary regardless of whether it asked for a price, a date, or a structured field.

Three options, in increasing order of effort:

- **Pass empty prompt** (`prompt: ''`) and let claude-code's WebFetch default kick in. Less misleading than the current behavior.
- **Forward whatever model-supplied query field Cursor sends in `fetchArgs`** if one exists; the current code looks at only `url`. Worth a proto inspection to confirm.
- **Use the last few hundred chars of the assistant's pre-fetch text/thinking deltas as a heuristic prompt** -- captures intent at the cost of complexity.

## Issue 4 -- `webFetchRequestQuery` branch is dead code with regression risk

**File:** `src/cursor-agent.js:872-884`.

The comment correctly notes the branch is unreachable because `WebFetchRequest*` schemas are not in the vendored proto. But the rejection logic is symmetric with the pre-`f6cc478` WebSearch handling. When the proto is regenerated and `webFetchRequestQuery` becomes a real case, this code will silently start rejecting WebFetch calls **even in passthrough mode** -- reintroducing exactly the "tool not available" leak that motivated `f6cc478`.

**Suggested fix:** either delete the branch entirely (the `default` case falls through to `abandon`, which makes the model fall back to the MCP-prefixed `mcp_WebFetch` -- same fallback path WebSearch used pre-fix), or pre-emptively wire an approve path gated on `passthroughNative`. Whichever -- don't leave a future-armed footgun in the switch.

## Issue 5 -- Stale comment in the passthrough header

**File:** `src/cursor-agent.js:551-552`.

```
// When passthroughNativeTools is enabled, instead of rejecting native tool
// calls (shellArgs / readArgs / writeArgs / fetchArgs), translate them to
```

Drift: `shellStreamArgs`, `backgroundShellSpawnArgs`, and `grepArgs` are also handled below the comment but unlisted. One-line cleanup.

## Nice-to-have -- ExaSearch / ExaFetch

`src/cursor-agent.js:880-887` unconditionally rejects ExaSearch / ExaFetch even with passthrough on. The unit test (`test-interaction-query-websearch.mjs:84`) treats this as deliberate ("we only approve WebSearch"). If that's intentional (paid-tier carve-out, different result handling), no action. If it's an oversight, the same approve pattern as WebSearch applies -- and the same Issue 1 invisibility caveat would too.

## Severity summary

| # | Title | Severity |
|---|-------|----------|
| 1 | WebSearch invisible to client | Medium (architecture / observability) |
| 2 | Wrong-FS path footgun | High (silent correctness failure) |
| 3 | Hardcoded WebFetch prompt | Medium (lossy, but obvious in diff) |
| 4 | Dead WebFetch rejection branch | Low now, High after next proto regen |
| 5 | Stale passthrough comment | Trivial |
| -- | ExaSearch/ExaFetch parity | Unknown -- needs intent confirmation |

## Files referenced

- `src/cursor-agent.js:540-650` -- passthrough native-exec dispatch (`fetchArgs`, `readArgs`, etc.)
- `src/cursor-agent.js:806-925` -- `handleInteractionQuery` (WebSearch approve / others reject)
- `src/cursor-agent.js:1495-1575` -- `interactionUpdate` handler (where tool_call_started/Completed get dropped)
- `src/cursor-agent.js:462-468` -- `CURSOR_NATIVE_TOOL_NAMES` set (drives `mcp_` prefixing)
- `scaffolding/pool/tool-translator.mjs` -- sibling name/arg translator (does NOT touch WebSearch/WebFetch; they're handled in cursor-agent.js's native-exec/interaction paths)
- `scaffolding/pool/test-interaction-query-websearch.mjs` -- unit test for the approve/reject branches
- Related: `WATCHDOG_REARM_REVIEW.md`, `STEP_COMPLETED_INVESTIGATION.md`
- Relevant commit: `f6cc478`
