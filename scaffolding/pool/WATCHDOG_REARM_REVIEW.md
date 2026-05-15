# tool_use watchdog -- re-arm gap review

**Scope:** `scaffolding/pool/api-server.mjs` tool_use turn finalization (post-commit `2a004f2`).
**TL;DR:** One real correctness bug (orphan tool_use risk worsened by the 1s default), three cleanup items.

---

## Context

Recent commits changed the tool_use finalization model:

- `cd34ecf` -- replaced the 250ms debounce with a 30s watchdog (semantics: watchdog only fires when `step_completed` never arrives).
- `2a004f2` -- dropped the watchdog default from 30s to 1s after observing that `*-thinking-fast` Cursor variants emit **zero** `step_completed` events in practice. See `STEP_COMPLETED_INVESTIGATION.md`.

Net result: the watchdog is now the **de-facto primary** finalize signal, with a 1s budget.

## Issue 1 -- Watchdog only re-arms on `tool_use`, not on `text_delta`/`thinking_delta` (real bug)

**File:** `scaffolding/pool/api-server.mjs:506-620` (the `reqHandlers.set(requestId, { onEvent })` block).

**Sequence that breaks:**

```
t=0     tool_use #1     -> armToolUseFinalizer()  (fires at t=1000)
t=200   thinking_delta  -> NOT re-armed
t=400   thinking_delta  -> NOT re-armed
t=600   text_delta      -> NOT re-armed
t=1000  WATCHDOG fires  -> finishMessage(); done=true; res.end()
t=1200  tool_use #2     -> emitToolUseBlock() writes to closed res
                        -> AND the worker has already added it to
                           pendingMcpInfo
```

**Why it matters:** when tool_use #2 arrives after `done=true`, two things go wrong:

1. `emitToolUseBlock` calls `sseWrite` on a closed response (swallowed by sseWrite's try/catch -- silent but wasted work).
2. **The real problem:** bridge-worker stored execId for tool_use #2 in `pendingMcpInfo` (bridge-worker.mjs:351). When claude-code POSTs back `tool_result` blocks for the tool_uses it *did* see (#1 only), the next `send_tool_results` will not include #2's execId. Worker dispatches what it has, the model keeps waiting for the missing result, and the channel stays busy until pool-manager's 240s busy-watchdog kills it.

**Why the 150-480ms gap measurement doesn't cover this:** those numbers were observed between consecutive tool_uses in the same step. With thinking-fast models, a thinking_delta chunk *between* two tool_uses in the same step is normal -- and on slower turns can easily exceed 1s.

**Fix:** re-arm on any model-originated stream activity, not just tool_use. The watchdog then measures "model went silent" rather than "no more tool_uses".

```javascript
} else if (msg.type === 'text_delta') {
  if (done) return;
  emitTextDelta(msg.text);
  if (toolUseEmitted) armToolUseFinalizer();
} else if (msg.type === 'thinking_delta') {
  if (done) return;
  if (POOL_REINJECT_THINKING) thinkingBuffer.append(convKey, msg.text || '');
  if (toolUseEmitted) armToolUseFinalizer();
}
```

After this fix, 1s is a fine default.

## Issue 2 -- Stale inline comment

**File:** `scaffolding/pool/api-server.mjs:542`.

```
//   (b) the watchdog (default 30 s) fires -- last-resort fallback
```

Should read `default 1 s`. The surrounding block-comment (363-385) was updated in `2a004f2`; this one was missed.

## Issue 3 -- `tool_use` handler doesn't guard on `done`

**File:** `scaffolding/pool/api-server.mjs:534-591`.

Late tool_use after watchdog/disconnect runs the full translate-or-passthrough path, calls `emitToolUseBlock` (writes to closed `res`), and arms another watchdog timer. The arm-after-done is harmless because of the `if (done) return` inside the timer body, but the work is wasted.

Add `if (done) return;` at the top of the `tool_use` branch. Same guard belongs on `text_delta` and `thinking_delta`. (`step_completed` already has it implicitly at line 598.)

## Issue 4 -- `finishMessage()` doesn't disarm the timer

**File:** `scaffolding/pool/api-server.mjs:474-499`.

All explicit exit paths (`step_completed`, `yield`, `error`, client disconnect) call `disarmToolUseFinalizer()` before `finishMessage()`. The watchdog-triggered exit doesn't, because the timer just fired. Result: harmless dangling timeout reference, but the bookkeeping is asymmetric.

Add `disarmToolUseFinalizer()` at the top of `finishMessage()` and the explicit disarms at the call sites become redundant (can be removed for clarity).

## Stop-gap until Issue 1 is patched

Bump `POOL_TOOL_USE_WATCHDOG_MS=5000` in prod env. Watch the log for `finalize tool_use turn (WATCHDOG @...)` lines -- each one is potentially a turn that orphaned a downstream tool_use.

## Files referenced

- `scaffolding/pool/api-server.mjs` -- finalizer arm/disarm + onEvent dispatcher
- `scaffolding/pool/bridge-worker.mjs:103, :351, :433` -- `pendingMcpInfo` map ownership
- `scaffolding/pool/STEP_COMPLETED_INVESTIGATION.md` -- why the watchdog is primary, not fallback
- Recent commits: `2a004f2`, `cd34ecf`
