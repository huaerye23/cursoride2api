#!/usr/bin/env node
// RATLC + caller-tools MVP
//
// Extends ratlc-mvp.mjs to validate that an inner Cursor agent can call
// CALLER-defined tools (e.g. Read, Bash, get_current_time) within the
// yield-loop session, with the proxy round-tripping tool_result back into
// the same stream, and the model still respecting the yield contract.
//
// If this works, RATLC can serve claude-code in production. If it doesn't,
// we either drop RATLC for tool-heavy workloads or fall back to embedding
// caller tools inside the yield text payload (option B from the survey).

import { createRequire } from 'node:module';
import fs from 'node:fs';

const require = createRequire(import.meta.url);
const { startConversation } = require('../src/cursor-agent.js');

const MODEL = process.env.MODEL || 'claude-opus-4-7-thinking-max-fast';
const ROUNDS = parseInt(process.env.ROUNDS || '10', 10);
const OPEN_RETRY_MAX = parseInt(process.env.OPEN_RETRY_MAX || '250', 10);
const OPEN_RETRY_MS = parseInt(process.env.OPEN_RETRY_MS || '300', 10);
const TURN_TIMEOUT_MS = parseInt(process.env.TURN_TIMEOUT_MS || '180000', 10);

const tokenFile = JSON.parse(fs.readFileSync(new URL('../token.json', import.meta.url), 'utf8'));
const token = tokenFile.tokens[0];

const YIELD_TOOL_NAME = 'bajie_yield';

// ── Tool definitions handed to Cursor's inner agent ───────────────────────
//
// All four tools registered up-front when the stream opens. They're
// "caller-defined" in the sense that bajie_yield is the only one for RATLC's
// own bookkeeping; the other three simulate what claude-code or another
// Anthropic client would expose.
//
// We register exec-side mock implementations: when the inner agent calls
// these, the scaffolding generates a plausible result and feeds it back.

const tools = [
  {
    name: YIELD_TOOL_NAME,
    description:
      'Call this tool when you have finished your reply and want to wait for the next user message. ' +
      'The tool result string will be the next user message. ' +
      'You MUST call this tool at the END of every response, AFTER any other tool calls. Never end your turn without it.',
    jsonSchema: { type: 'object', properties: {}, required: [] },
  },
  {
    name: 'get_current_time',
    description: 'Returns the current ISO timestamp as a string.',
    jsonSchema: { type: 'object', properties: {}, required: [] },
  },
  {
    name: 'get_random_number',
    description: 'Returns a random integer in the inclusive range [min, max].',
    jsonSchema: {
      type: 'object',
      properties: {
        min: { type: 'integer', description: 'lower bound, inclusive' },
        max: { type: 'integer', description: 'upper bound, inclusive' },
      },
      required: ['min', 'max'],
    },
  },
  {
    name: 'calculate',
    description: 'Evaluates a simple arithmetic expression and returns the result as a string. Supports + - * / ( ) and integers.',
    jsonSchema: {
      type: 'object',
      properties: {
        expression: { type: 'string', description: 'arithmetic expression, e.g. "(17+4)*3"' },
      },
      required: ['expression'],
    },
  },
].map((t) => ({ ...t, toolName: t.name, providerIdentifier: 'cursoride2api-ratlc-tools' }));

// ── Mock tool executors ───────────────────────────────────────────────────
function execTool(toolName, args) {
  switch (toolName) {
    case 'get_current_time':
      return new Date().toISOString();
    case 'get_random_number': {
      const min = parseInt(args?.min, 10);
      const max = parseInt(args?.max, 10);
      if (!Number.isFinite(min) || !Number.isFinite(max) || max < min) {
        return { error: `invalid bounds: min=${args?.min} max=${args?.max}` };
      }
      return String(Math.floor(Math.random() * (max - min + 1)) + min);
    }
    case 'calculate': {
      const expr = String(args?.expression || '');
      // Strict character whitelist — refuse anything that isn't pure arithmetic
      if (!/^[\d+\-*/().\s]+$/.test(expr)) {
        return { error: `disallowed character in expression: ${JSON.stringify(expr).slice(0, 80)}` };
      }
      try {
        // eslint-disable-next-line no-new-func
        const v = Function(`"use strict"; return (${expr});`)();
        return String(v);
      } catch (e) {
        return { error: `eval failed: ${e.message}` };
      }
    }
    default:
      return { error: `unknown tool: ${toolName}` };
  }
}

// ── Test prompts (designed to force specific tool usage patterns) ─────────
const TEST_MESSAGES = [
  // Round 1: priming + first task, requires get_current_time
  "What's the current time? Call get_current_time, then give me a one-sentence answer with the time, then yield.",
  // Round 2: simple text, no tool needed — tests that tools aren't over-used
  'Now without using any tool, just tell me: what color is grass? One word.',
  // Round 3: math via calculate tool
  'Compute (17 + 4) * 3 using the calculate tool, then state the result.',
  // Round 4: random number with explicit bounds
  'Roll a random number between 1 and 100 using get_random_number, and tell me what you rolled.',
  // Round 5: two tools chained
  'Use get_current_time once, then use calculate to compute 8 * 7, then give a one-sentence summary.',
  // Round 6: recall after several tool calls
  'What did I ask you in my very first request? Quote it briefly. No tools needed.',
  // Round 7: ambiguous — model decides whether to use a tool
  'How much is 2 + 2? Answer however you like.',
  // Round 8: explicit no-tool
  'Without calling any tool, what is the capital of Japan?',
  // Round 9: multiple random numbers
  'Roll two random numbers, one between 1-10 and one between 50-60, then tell me both.',
  // Round 10: closing
  'Final round. Reply with exactly "DONE" and yield. No tools.',
];

// ── instrumentation ───────────────────────────────────────────────────────
const stats = { model: MODEL, openAttempts: 0, rounds: [], failures: [], streamOpenedAt: null };

// ── open with retry on unpaid_invoice ─────────────────────────────────────
async function openWithRetry(initialPrompt) {
  for (let attempt = 1; attempt <= OPEN_RETRY_MAX; attempt++) {
    stats.openAttempts = attempt;
    const result = await openAttemptOnce(initialPrompt, attempt);
    if (result.kind === 'opened') {
      stats.streamOpenedAt = Date.now();
      console.log(`\n✅ stream opened on attempt ${attempt}/${OPEN_RETRY_MAX}`);
      console.log(`   pre-yield text: ${JSON.stringify(result.textBuf.slice(0, 120))}`);
      console.log(`   pre-yield tool calls: ${result.toolCallsMade.length} (${result.toolCallsMade.map((c) => c.toolName).join(', ') || 'none'})`);
      return result;
    }
    if (result.kind === 'unpaid') {
      process.stdout.write(attempt % 10 === 0 ? `[${attempt}] ` : '.');
      await new Promise((r) => setTimeout(r, OPEN_RETRY_MS));
      continue;
    }
    console.log(`\n  attempt ${attempt} non-retryable kind=${result.kind}: ${result.msg || result.textBuf?.slice(0, 200) || ''}`);
    if (result.kind === 'no_yield') {
      await new Promise((r) => setTimeout(r, OPEN_RETRY_MS));
      continue;
    }
    throw new Error(`open failed: ${result.msg}`);
  }
  throw new Error(`open failed after ${OPEN_RETRY_MAX} attempts`);
}

function openAttemptOnce(initialPrompt, attemptIdx) {
  return new Promise((resolve) => {
    let textBuf = '';
    let firstTextDeltaAt = null;
    let yieldInfo = null;
    let bridge = null;
    const toolCallsMade = [];
    const t0 = Date.now();

    const settle = (outcome) => {
      if (outcome.kind !== 'opened' && bridge) {
        try { bridge.close(); } catch { /* ignore */ }
      }
      resolve(outcome);
    };

    bridge = startConversation(token, {
      prompt: initialPrompt,
      modelId: MODEL,
      tools,
      maxMode: true,
      onTextDelta: (t) => {
        if (firstTextDeltaAt == null) firstTextDeltaAt = Date.now();
        textBuf += t;
      },
      onThinkingDelta: () => {},
      onMcpCall: (info) => {
        if (info.toolName === YIELD_TOOL_NAME) {
          yieldInfo = info;
          settle({
            kind: 'opened', bridge, yieldInfo, textBuf, toolCallsMade,
            firstTextDeltaAt, attemptStartedAt: t0,
          });
        } else {
          // Caller-tool call BEFORE yield — execute and feed result, keep listening
          const result = execTool(info.toolName, info.args);
          toolCallsMade.push({ toolName: info.toolName, args: info.args, result });
          console.log(`\n  tool→ ${info.toolName}(${JSON.stringify(info.args)}) = ${JSON.stringify(result).slice(0, 80)}`);
          bridge.sendToolResult(info.id, info.execId, result);
        }
      },
      onStepCompleted: () => {},
      onTurnEnded: () => {
        if (!yieldInfo) settle({ kind: 'no_yield', textBuf, elapsed: Date.now() - t0 });
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
}

// ── run a round, handling intra-round tool calls ──────────────────────────
function runRound(bridge, pendingYield, userMessage, roundIdx) {
  return new Promise((resolve) => {
    let textBuf = '';
    let firstTextAt = null;
    const toolCallsThisRound = [];
    const t0 = Date.now();
    const timer = setTimeout(() => {
      bridge.setCallbacks({ onTextDelta: () => {}, onMcpCall: () => {}, onTurnEnded: () => {} });
      resolve({ ok: false, reason: 'timeout', textBuf, elapsed: Date.now() - t0, toolCallsThisRound });
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
            ok: true, textBuf, elapsed: Date.now() - t0,
            ttft: firstTextAt != null ? firstTextAt - t0 : null,
            yieldInfo: info, toolCallsThisRound,
          });
        } else {
          // Caller tool: execute and feed result, stay in this round
          const result = execTool(info.toolName, info.args);
          toolCallsThisRound.push({ toolName: info.toolName, args: info.args, result });
          console.log(`  tool→ ${info.toolName}(${JSON.stringify(info.args)}) = ${JSON.stringify(result).slice(0, 80)}`);
          bridge.sendToolResult(info.id, info.execId, result);
        }
      },
      onStepCompleted: () => {},
      onTurnEnded: () => {
        clearTimeout(timer);
        resolve({ ok: false, reason: 'no_yield_on_turn_end', textBuf, elapsed: Date.now() - t0, toolCallsThisRound });
      },
      onError: (err) => {
        clearTimeout(timer);
        const msg = String(err?.message || err || '');
        resolve({ ok: false, reason: 'error', error: msg, textBuf, elapsed: Date.now() - t0, toolCallsThisRound });
      },
    });

    bridge.sendToolResult(pendingYield.id, pendingYield.execId, userMessage);
  });
}

function logRound(idx, sendMsg, textBuf, elapsed, ttft, toolCalls) {
  stats.rounds.push({ idx, sendMsg, textBuf, elapsed, ttft, toolCalls });
  const preview = textBuf.replace(/\s+/g, ' ').slice(0, 100);
  const toolSummary = toolCalls.length
    ? ` tools=[${toolCalls.map((c) => c.toolName).join(',')}]`
    : '';
  console.log(`[round ${String(idx).padStart(2)}/${ROUNDS}]  ttft=${ttft || '–'}ms  total=${elapsed}ms${toolSummary}  → ${JSON.stringify(preview)}`);
}

// ── main ──────────────────────────────────────────────────────────────────
async function main() {
  console.log('═══ RATLC + caller tools MVP ═══');
  console.log(`model:          ${MODEL}  (maxMode=true)`);
  console.log(`tools:          ${tools.map((t) => t.name).join(', ')}`);
  console.log(`rounds target:  ${ROUNDS}`);
  console.log(`open retry:     up to ${OPEN_RETRY_MAX} @ ${OPEN_RETRY_MS}ms\n`);

  const initialPrompt =
    'You are operating in RELAY mode for an automated test. ' +
    'You have these tools: get_current_time, get_random_number, calculate, and bajie_yield. ' +
    'When a user asks something that needs a tool, call the appropriate tool first, then write a brief answer. ' +
    'When a user asks something that does NOT need a tool, just answer directly. ' +
    'At the END of EVERY response (after any other tool calls), you MUST call `bajie_yield` to wait for the next user message. ' +
    'The bajie_yield tool result will be the next user message verbatim. ' +
    'Never end your turn without calling bajie_yield. Keep replies short.\n\n' +
    'User: ' + TEST_MESSAGES[0];

  console.log('opening stream...');
  const openedAt = Date.now();
  const opened = await openWithRetry(initialPrompt);
  const bridge = opened.bridge;

  // Round 1 = initial open
  logRound(
    1,
    TEST_MESSAGES[0],
    opened.textBuf,
    opened.yieldInfo
      ? (opened.yieldInfo._receivedAt = Date.now()) - opened.attemptStartedAt
      : 0,
    opened.firstTextDeltaAt ? opened.firstTextDeltaAt - opened.attemptStartedAt : null,
    opened.toolCallsMade,
  );

  let pendingYield = opened.yieldInfo;

  for (let i = 1; i < Math.min(ROUNDS, TEST_MESSAGES.length); i++) {
    const userMsg = TEST_MESSAGES[i];
    const result = await runRound(bridge, pendingYield, userMsg, i + 1);
    if (!result.ok) {
      stats.failures.push({
        round: i + 1, reason: result.reason, error: result.error,
        partialText: result.textBuf, toolCallsThisRound: result.toolCallsThisRound,
      });
      console.log(`\n❌ round ${i + 1} failed: ${result.reason} ${result.error || ''}`);
      console.log(`   partial text: ${JSON.stringify(result.textBuf.slice(0, 200))}`);
      console.log(`   tool calls made before failure: ${result.toolCallsThisRound.length}`);
      break;
    }
    logRound(i + 1, userMsg, result.textBuf, result.elapsed, result.ttft, result.toolCallsThisRound);
    pendingYield = result.yieldInfo;
  }

  const wallMs = Date.now() - openedAt;
  const lifetimeMs = stats.streamOpenedAt ? Date.now() - stats.streamOpenedAt : 0;
  const totalToolCalls = stats.rounds.reduce((s, r) => s + r.toolCalls.length, 0);
  const roundsWithTools = stats.rounds.filter((r) => r.toolCalls.length > 0).length;

  console.log('\n\n═══ summary ═══');
  console.log(`model:               ${MODEL}`);
  console.log(`open attempts:       ${stats.openAttempts}`);
  console.log(`rounds completed:    ${stats.rounds.length} / ${ROUNDS}`);
  console.log(`stream lifetime:     ${(lifetimeMs / 1000).toFixed(1)}s`);
  console.log(`total wall time:     ${(wallMs / 1000).toFixed(1)}s`);
  console.log(`tool calls:          ${totalToolCalls} across ${roundsWithTools}/${stats.rounds.length} rounds with tools`);

  if (stats.rounds.length > 0) {
    const lats = stats.rounds.map((r) => r.elapsed).sort((a, b) => a - b);
    const p = (a, q) => a[Math.min(a.length - 1, Math.floor(q * a.length))];
    console.log(`per-round latency:   p50=${p(lats, 0.5)}ms  p95=${p(lats, 0.95)}ms  min=${lats[0]}ms  max=${lats[lats.length - 1]}ms`);

    // Per-tool breakdown
    const byTool = {};
    for (const r of stats.rounds) {
      for (const c of r.toolCalls) {
        (byTool[c.toolName] || (byTool[c.toolName] = [])).push(c);
      }
    }
    if (Object.keys(byTool).length) {
      console.log(`tool-call breakdown:`);
      for (const [name, calls] of Object.entries(byTool)) {
        console.log(`  ${name.padEnd(20)} count=${calls.length}`);
      }
    }
  }
  if (stats.failures.length) {
    console.log(`\nfailures:`);
    for (const f of stats.failures) console.log(`  round ${f.round}: ${f.reason}  ${(f.error || '').slice(0, 200)}`);
  }

  const outPath = new URL(`./ratlc-tools-result-${MODEL}.json`, import.meta.url);
  fs.writeFileSync(outPath, JSON.stringify({ ...stats, wallMs, lifetimeMs, totalToolCalls, roundsWithTools }, null, 2));
  console.log(`\nresult written to ${outPath.pathname}`);

  try { bridge.close(); } catch { /* ignore */ }
  setTimeout(() => process.exit(stats.failures.length ? 1 : 0), 500);
}

main().catch((e) => { console.error('\nfatal:', e); process.exit(2); });
