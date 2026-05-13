#!/usr/bin/env node
// RATLC MVP — Retry-based Agentic Tool-Looping Conversation
//
// Open ONE Cursor agent stream with retry-until-success, then drive N text
// round-trips through it via a single `bajie_yield` tool. Measures whether
// one paid-for stream can serve many API requests.
//
// Usage:
//   MODEL=claude-opus-4-7-thinking-max-fast node scaffolding/ratlc-mvp.mjs
//   MODEL=claude-4.6-opus-max-thinking-fast  node scaffolding/ratlc-mvp.mjs
//
// Env:
//   MODEL              target Cursor model (must support maxMode)
//   ROUNDS             number of round-trips to attempt (default 20)
//   OPEN_RETRY_MAX     retry cap on initial stream open (default 250)
//   OPEN_RETRY_MS      interval between open attempts (default 300)
//   TURN_TIMEOUT_MS    per-round timeout waiting for yield (default 120000)

import { createRequire } from 'node:module';
import fs from 'node:fs';

const require = createRequire(import.meta.url);
const { startConversation } = require('../src/cursor-agent.js');

const MODEL = process.env.MODEL || 'claude-opus-4-7-thinking-max-fast';
const ROUNDS = parseInt(process.env.ROUNDS || '20', 10);
const OPEN_RETRY_MAX = parseInt(process.env.OPEN_RETRY_MAX || '250', 10);
const OPEN_RETRY_MS = parseInt(process.env.OPEN_RETRY_MS || '300', 10);
const TURN_TIMEOUT_MS = parseInt(process.env.TURN_TIMEOUT_MS || '120000', 10);

const tokenFile = JSON.parse(fs.readFileSync(new URL('../token.json', import.meta.url), 'utf8'));
const token = tokenFile.tokens[0];

const YIELD_TOOL_NAME = 'bajie_yield';

// Tool definition handed to Cursor's agent.
const yieldToolDef = {
  name: YIELD_TOOL_NAME,
  toolName: YIELD_TOOL_NAME,
  description:
    'Call this tool when you have finished your reply and want to wait for the next user message. ' +
    'The tool result string will be the next user message. ' +
    'You MUST call this tool at the end of every response. Never end your turn without calling it.',
  providerIdentifier: 'cursoride2api-ratlc',
  jsonSchema: { type: 'object', properties: {}, required: [] },
};

// Sequence of test messages to drive through one stream. Mix of easy + recall
// to detect quality drift across rounds.
const TEST_MESSAGES = [
  'Hello — to confirm the connection, reply with exactly "READY" and nothing else, then yield.',
  'What is 2 + 2? One-word answer.',
  'What is 7 times 8? One-word answer.',
  'Capital of France? One word.',
  'How many planets in the solar system? One number.',
  'What was my first question in this conversation? Quote it briefly.',
  'Spell the word "cat" backwards.',
  'What is the chemical symbol for gold?',
  'Square root of 144?',
  'Name a color that starts with M.',
  'How many letters in the word "scaffolding"?',
  'Is Tokyo north or south of the equator?',
  'What did I just ask before the Tokyo question?',
  'Name a programming language that starts with "P".',
  'Translate "hello" to Spanish.',
  'How many sides does a hexagon have?',
  'What gas do plants release during photosynthesis?',
  'Pi rounded to two decimal places?',
  'How many rounds have we done so far? Estimate is fine.',
  'Final round. Reply with exactly "DONE" and yield.',
];

// ── instrumentation ───────────────────────────────────────────────────────
const stats = {
  model: MODEL,
  openedAt: null,
  rounds: [],
  failures: [],
  openAttempts: 0,
  streamOpenedAt: null,
};

function logRound(idx, send, recvText, recvMs, ttftMs, yieldExecId) {
  stats.rounds.push({ idx, send, recvText, recvMs, ttftMs, yieldExecId });
  const preview = recvText.replace(/\s+/g, ' ').slice(0, 80);
  console.log(`[round ${String(idx).padStart(2)}/${ROUNDS}]  ttft=${ttftMs}ms  total=${recvMs}ms  → ${JSON.stringify(preview)}`);
}

// ── open with retry on unpaid_invoice ─────────────────────────────────────
function openWithRetry(initialPrompt) {
  return new Promise(async (resolveOpen, rejectOpen) => {
    let opened = false;

    const tryOnce = (attempt) => new Promise((resolve) => {
      const t0 = Date.now();
      let textBuf = '';
      let firstYieldInfo = null;
      let firstTextDeltaAt = null;
      let bridge = null;

      const settle = (outcome) => {
        // outcome: { kind: 'opened'|'unpaid'|'other_error', ... }
        if (outcome.kind !== 'opened' && bridge) {
          try { bridge.close(); } catch { /* ignore */ }
        }
        resolve(outcome);
      };

      bridge = startConversation(token, {
        prompt: initialPrompt,
        modelId: MODEL,
        tools: [yieldToolDef],
        maxMode: true,
        onTextDelta: (t) => {
          if (firstTextDeltaAt == null) firstTextDeltaAt = Date.now();
          textBuf += t;
        },
        onThinkingDelta: () => { /* ignored — we only care about visible text */ },
        onMcpCall: (info) => {
          if (info.toolName === YIELD_TOOL_NAME && !firstYieldInfo) {
            firstYieldInfo = info;
            // Success — the stream is alive AND the model followed the yield
            // contract on its first turn. Hand the bridge back.
            settle({
              kind: 'opened',
              bridge,
              firstYieldInfo: info,
              firstYieldAt: Date.now(),
              firstTextDeltaAt,
              attemptStartedAt: t0,
              textBuf,
            });
          }
        },
        onStepCompleted: () => { /* ignored */ },
        onTurnEnded: () => {
          // turnEnded BEFORE yield means the model finished a turn without
          // calling yield — treat as a soft failure on this open attempt
          // so we retry. (The model didn't follow the contract.)
          if (!firstYieldInfo) {
            settle({ kind: 'no_yield', textBuf, elapsed: Date.now() - t0 });
          }
        },
        onError: (err) => {
          const msg = String(err?.message || err || '');
          if (/unpaid invoice|cursor\.com\/dashboard/i.test(msg)) {
            settle({ kind: 'unpaid', elapsed: Date.now() - t0 });
          } else {
            settle({ kind: 'other_error', msg, elapsed: Date.now() - t0 });
          }
        },
      });
    });

    for (let attempt = 1; attempt <= OPEN_RETRY_MAX; attempt++) {
      stats.openAttempts = attempt;
      const result = await tryOnce(attempt);
      if (result.kind === 'opened') {
        stats.streamOpenedAt = Date.now();
        console.log(`\n✅ stream opened on attempt ${attempt}/${OPEN_RETRY_MAX} (${Date.now() - result.attemptStartedAt}ms attempt)`);
        console.log(`   first yield received; pre-yield text: ${JSON.stringify(result.textBuf.slice(0, 120))}`);
        return resolveOpen(result);
      }
      if (result.kind === 'unpaid') {
        process.stdout.write(attempt % 10 === 0 ? `[${attempt}] ` : '.');
        await new Promise((r) => setTimeout(r, OPEN_RETRY_MS));
        continue;
      }
      // no_yield or other_error — log and retry
      console.log(`\n  attempt ${attempt} non-retryable kind=${result.kind}: ${result.msg || result.textBuf?.slice(0, 200) || ''}`);
      if (result.kind === 'no_yield') {
        // Model finished a turn without yielding. Retry with stronger prompt? For
        // now just retry the same prompt; if it's persistent we'll see a pattern.
        await new Promise((r) => setTimeout(r, OPEN_RETRY_MS));
        continue;
      }
      // hard failure
      return rejectOpen(new Error(`open failed on attempt ${attempt}: ${result.msg}`));
    }
    rejectOpen(new Error(`open failed after ${OPEN_RETRY_MAX} attempts`));
  });
}

// ── run rounds ────────────────────────────────────────────────────────────
async function runRound(bridge, pendingYield, userMessage, idx) {
  return new Promise((resolve) => {
    let textBuf = '';
    let firstTextAt = null;
    const t0 = Date.now();
    let timer = setTimeout(() => {
      bridge.setCallbacks({ onTextDelta: () => {}, onMcpCall: () => {}, onTurnEnded: () => {} });
      resolve({ ok: false, reason: 'timeout', textBuf, elapsed: Date.now() - t0 });
    }, TURN_TIMEOUT_MS);

    bridge.setCallbacks({
      onTextDelta: (t) => {
        if (firstTextAt == null) firstTextAt = Date.now();
        textBuf += t;
      },
      onThinkingDelta: () => {},
      onMcpCall: (info) => {
        if (info.toolName === YIELD_TOOL_NAME) {
          clearTimeout(timer);
          resolve({
            ok: true,
            textBuf,
            elapsed: Date.now() - t0,
            ttft: firstTextAt != null ? firstTextAt - t0 : null,
            yieldInfo: info,
          });
        }
      },
      onStepCompleted: () => {},
      onTurnEnded: () => {
        // Turn ended without yield = model broke the contract.
        clearTimeout(timer);
        resolve({ ok: false, reason: 'no_yield_on_turn_end', textBuf, elapsed: Date.now() - t0 });
      },
      onError: (err) => {
        clearTimeout(timer);
        const msg = String(err?.message || err || '');
        resolve({ ok: false, reason: 'error', error: msg, textBuf, elapsed: Date.now() - t0 });
      },
    });

    // Feed the next user message as the tool result for the previous yield.
    bridge.sendToolResult(pendingYield.id, pendingYield.execId, userMessage);
  });
}

// ── main ──────────────────────────────────────────────────────────────────
async function main() {
  console.log('═══ RATLC MVP ═══');
  console.log(`model:          ${MODEL}  (maxMode=true)`);
  console.log(`rounds target:  ${ROUNDS}`);
  console.log(`open retry:     up to ${OPEN_RETRY_MAX} attempts @ ${OPEN_RETRY_MS}ms`);
  console.log(`turn timeout:   ${TURN_TIMEOUT_MS}ms\n`);

  // Initial prompt does two things at once: (1) primes the relay behavior,
  // (2) carries the first real user question so we don't waste a round on
  // a no-op acknowledgment. The model should respond to (2) AND call yield.
  const initialPrompt =
    'You are operating in RELAY mode for an automated test. ' +
    'After every reply, you MUST call the `bajie_yield` tool to wait for the next user message. ' +
    'The tool result will be the next user message verbatim. ' +
    'Always end your turn by calling `bajie_yield`. Do not call any other tool. ' +
    'Keep replies short.\n\n' +
    'User: ' + TEST_MESSAGES[0];

  console.log('opening stream (this may take 30-300s while we win the retry lottery)...');
  stats.openedAt = Date.now();
  const opened = await openWithRetry(initialPrompt);
  const bridge = opened.bridge;

  // Round 0 is the initial prompt → first yield. Record it.
  const r0Text = opened.textBuf;
  const r0Elapsed = opened.firstYieldAt - opened.attemptStartedAt;
  const r0Ttft = opened.firstTextDeltaAt ? opened.firstTextDeltaAt - opened.attemptStartedAt : null;
  logRound(1, TEST_MESSAGES[0], r0Text, r0Elapsed, r0Ttft, opened.firstYieldInfo.execId);

  let pendingYield = opened.firstYieldInfo;

  // Subsequent rounds: feed messages 2..N as tool results.
  for (let i = 1; i < Math.min(ROUNDS, TEST_MESSAGES.length); i++) {
    const userMsg = TEST_MESSAGES[i];
    const result = await runRound(bridge, pendingYield, userMsg, i + 1);
    if (!result.ok) {
      stats.failures.push({ round: i + 1, reason: result.reason, error: result.error, partialText: result.textBuf });
      console.log(`\n❌ round ${i + 1} failed: ${result.reason} ${result.error || ''}`);
      console.log(`   partial text: ${JSON.stringify(result.textBuf.slice(0, 200))}`);
      break;
    }
    logRound(i + 1, userMsg, result.textBuf, result.elapsed, result.ttft, result.yieldInfo.execId);
    pendingYield = result.yieldInfo;
  }

  // ── summary ──────────────────────────────────────────────────────────────
  const wallMs = Date.now() - stats.openedAt;
  const streamLifetimeMs = stats.streamOpenedAt ? Date.now() - stats.streamOpenedAt : 0;

  console.log('\n\n═══ summary ═══');
  console.log(`model:               ${MODEL}`);
  console.log(`open attempts:       ${stats.openAttempts}`);
  console.log(`rounds completed:    ${stats.rounds.length} / ${ROUNDS}`);
  console.log(`stream lifetime:     ${(streamLifetimeMs / 1000).toFixed(1)}s`);
  console.log(`total wall time:     ${(wallMs / 1000).toFixed(1)}s`);

  if (stats.rounds.length > 0) {
    const lats = stats.rounds.map((r) => r.recvMs).sort((a, b) => a - b);
    const ttfts = stats.rounds.filter((r) => r.ttftMs != null).map((r) => r.ttftMs).sort((a, b) => a - b);
    const p = (arr, q) => arr[Math.min(arr.length - 1, Math.floor(q * arr.length))];
    console.log(`per-round latency:   p50=${p(lats, 0.5)}ms  p95=${p(lats, 0.95)}ms  min=${lats[0]}ms  max=${lats[lats.length - 1]}ms`);
    if (ttfts.length) console.log(`time-to-first-token: p50=${p(ttfts, 0.5)}ms  p95=${p(ttfts, 0.95)}ms`);
  }
  if (stats.failures.length) {
    console.log(`\nfailures:`);
    for (const f of stats.failures) {
      console.log(`  round ${f.round}: ${f.reason}  ${(f.error || '').slice(0, 200)}`);
    }
  }

  // dump JSON for the survey/follow-up doc
  const outPath = new URL(`./ratlc-result-${MODEL}.json`, import.meta.url);
  fs.writeFileSync(outPath, JSON.stringify({ ...stats, wallMs, streamLifetimeMs, completedRounds: stats.rounds.length }, null, 2));
  console.log(`\nresult written to ${outPath.pathname}`);

  try { bridge.close(); } catch { /* ignore */ }
  // Give the close a beat, then exit cleanly.
  setTimeout(() => process.exit(stats.failures.length ? 1 : 0), 500);
}

main().catch((e) => {
  console.error('\nfatal:', e);
  process.exit(2);
});
