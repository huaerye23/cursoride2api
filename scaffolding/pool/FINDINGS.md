# Findings from end-to-end translate-mode investigation

This document records what we learned trying to make a single warm pool
serve any claude-code session with no tool-list recycle.

## What works (delivered)

**Contract mode** (default, `POOL_TOOL_MODE=contract`):

- Pool channels open with the **first client request's** tool list as the
  contract.
- Subsequent requests with the **same tool names** reuse warm channels.
  Schema field drift is absorbed silently (signature is name-only by
  default, controlled by `POOL_SIG_MODE=name|schema`).
- Different tool names trigger a full pool recycle (all workers shut
  down + sequential respawn with the new contract).
- Verified end-to-end: text-only requests + tool-using requests + recall
  across multiple rounds — all in one channel session, ~1-2 s per warm
  round, ~150-300 s amortized for the initial lottery.

## What we tried and where it broke

**Translate mode v1** (`POOL_TOOL_MODE=translate`):

The plan was to open the pool with a minimal placeholder tool list so
Cursor's backend would auto-expose its full default agent toolset
(`Shell`, `Read`, `Write`, `Grep`, `Glob`, `StrReplace`, `EditNotebook`,
`TodoWrite`, `WebFetch`, `WebSearch`, `Task`, etc.), then translate the
Cursor names + arg shapes to claude-code's (`Bash`, `Edit`,
`NotebookEdit`, …) on the wire. **The empirical pieces all checked out**:

1. With non-empty `tools` field, Cursor injects its 18-tool default
   agent toolset into the model's prompt — confirmed by asking the inner
   agent to list its available tools.
2. The schemas are stable across rounds — empirically verified by
   instrumenting `requestContextArgs` and observing it fires once per
   stream.
3. `tool-translator.mjs` correctly maps `Shell→Bash`,
   `StrReplace→Edit`, etc., with best-effort arg adapters.

**Where it breaks**: `src/cursor-agent.js` **deliberately rejects** all
Cursor-native tool invocations (`shellArgs`, `readArgs`, `writeArgs`,
`grepArgs`, `lsArgs`, `deleteArgs`, `fetchArgs`, `diagnosticsArgs`) with
a structured `*Rejected` result so the model falls back to MCP-style
tools. Comment in the source:

```
// ── Reject native Cursor tools so the model falls back to MCP ──
```

This rejection happens *before* any callback the pool worker could hook.
The native tool call never reaches `onMcpCall`, so the translator never
sees it. The model interprets the rejection as "tool unavailable" and
either stops or tries another approach.

## What it would take to make translate mode actually work

A non-trivial modification to `src/cursor-agent.js`:

1. **Add `passthroughNativeTools` option** to `startConversation`.
2. **For each native exec case** (`shellArgs`, `readArgs`, …), when the
   option is set, instead of building a `*Rejected` result, translate
   the native args to MCP-style `{toolName, args}` and call `onMcpCall`
   the same way `mcpArgs` does.
3. **Track `execId → native_kind`** in a side map so when
   `sendToolResult` is later called, we build the right native result
   schema (`ReadResult`, `ShellResult`, …) instead of `McpResult`.
4. **Build the native result** from the caller's tool_result content —
   probably wrapping it in the success variant of the right schema.

Estimated ~150-200 lines in `cursor-agent.js` plus careful schema
inspection. Leaving it as documented future work rather than ship a
half-built version.

## Operational caveats discovered

1. **First-claude-code-call timeout**. Default claude-code timeout
   appears to be ~60 s. The initial lottery often runs 30-300 s. First
   call therefore frequently fails with "API returned an empty or
   malformed response (HTTP 200)". Mitigations:

   - Pre-warm the pool before pointing claude-code at it. Use a probe
     request from `curl` first; once the pool reports `ready ≥ 1`, run
     claude-code.
   - Or pre-configure the contract with a known tool set via
     `POOL_DEFAULT_TOOLS_FILE` (proposed feature, not yet implemented).

2. **Sequential opens vs parallel opens**. Opening multiple channels in
   parallel trips Cursor's per-account `ERROR_PRO_USER_RATE_LIMIT_EXCEEDED`
   limiter. We open one channel at a time (after the prior reaches
   `ready`), staggered. Bring-up time scales linearly with pool size:
   ~3-10 minutes for size 5 in the worst case.

3. **Idle ping at 20 min** keeps channels alive but doesn't reset
   conversation context. After many rounds the inner stream's context
   bloats; eventually responses degrade or hit upstream context limit.
   No automatic recycle for context — operator's responsibility.

## Verified working today

| Path | Wall time |
|---|---|
| Pool startup → first channel ready (lottery) | 30-300 s, p50 ~60 s |
| Subsequent rounds on warm channel | 1-3 s, dominated by LLM inference |
| Recycle on tool-name change | 30-180 s for new lottery |
| Native curl smoke test through pool | works |
| Tool round-trip in contract mode | works (verified with `frobulate_xyz`) |
| Recall across rounds | works (model quoted earlier messages verbatim) |
