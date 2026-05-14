#!/usr/bin/env node
// Bridge worker — one forked child process per RATLC channel.
// Owns ONE Cursor agent stream. Communicates with parent via process.send.
// See ./IPC.md "Protocol A" for the message schema.

import { createRequire } from 'node:module';
import fs from 'node:fs';

const require = createRequire(import.meta.url);

// ── Protocol switch ─────────────────────────────────────────────────────
// BRIDGE_PROTOCOL selects which transport this worker uses:
//   h2 (default) — src/cursor-agent.js, HTTP/2 BiDi stream on
//                  /agent.v1.AgentService/Run. Existing production path.
//   h1           — src/cursor-agent-h1.js, BidiAppend (HTTP/1.1 unary)
//                  + RunSSE (HTTP/1.1 server-streaming) pair.
//                  Hypothesis: bypasses the H2 path's per-account
//                  ERROR_PRO_USER_RATE_LIMIT_EXCEEDED so a 10+-channel
//                  pool can run in parallel. See scaffolding/pool/RUNSSE.md.
const BRIDGE_PROTOCOL = (process.env.BRIDGE_PROTOCOL || 'h2').toLowerCase();
let startConversation;
if (BRIDGE_PROTOCOL === 'h1') {
  ({ startConversation } = require('../../src/cursor-agent-h1.js'));
} else if (BRIDGE_PROTOCOL === 'h2') {
  ({ startConversation } = require('../../src/cursor-agent.js'));
} else {
  console.error(`[bridge-worker] invalid BRIDGE_PROTOCOL=${BRIDGE_PROTOCOL} (expected h1 or h2)`);
  process.exit(1);
}

const CHANNEL_ID = process.env.RATLC_CHANNEL_ID || 'ch-?';
const MODEL = process.env.RATLC_MODEL || 'claude-opus-4-7-thinking-max-fast';
// POOL_CONTEXT_MODE drives the priming prompt: in `full` mode the channel
// is told each bajie_yield carries the entire conversation history; in
// `last` mode (default) it's told each yield is the next user message
// verbatim — the historical behavior.
const POOL_CONTEXT_MODE = (process.env.POOL_CONTEXT_MODE || 'last').toLowerCase();
console.log(`[bridge-worker] channel=${CHANNEL_ID} model=${MODEL} protocol=${BRIDGE_PROTOCOL} ctxMode=${POOL_CONTEXT_MODE}`);
const OPEN_RETRY_MAX = parseInt(process.env.RATLC_OPEN_RETRY_MAX || '500', 10);
const OPEN_RETRY_MS = parseInt(process.env.RATLC_OPEN_RETRY_MS || '300', 10);
// When true, native Cursor tool calls (shellArgs/readArgs/writeArgs/...)
// are translated to MCP-shape tool_use events under the matching
// Anthropic name (Bash/Read/Write/...) instead of being rejected.
// Enabled when the pool runs in POOL_TOOL_MODE=translate.
const PASSTHROUGH_NATIVE = process.env.RATLC_PASSTHROUGH_NATIVE === '1';

const TOKEN_PATH = new URL('../../token.json', import.meta.url);
const tokenFile = JSON.parse(fs.readFileSync(TOKEN_PATH, 'utf8'));
const token = tokenFile.tokens[0];

const YIELD_TOOL_NAME = 'bajie_yield';

// ── Worker state ─────────────────────────────────────────────────────────
let bridge = null;
let currentState = 'spawning';
let openAttempts = 0;
let openedAt = 0;
let lastActivityAt = Date.now();
let currentRequestId = null;
let pendingYield = null;
let pendingMcpInfo = null;     // a non-yield mcp call we're currently holding
let configuredTools = [];

// ── Helpers ──────────────────────────────────────────────────────────────
function send(msg) {
  if (process.send) process.send(msg);
}

function setState(state, extra = {}) {
  currentState = state;
  send({
    type: 'state',
    channelId: CHANNEL_ID,
    state,
    openAttempts,
    openedAt,
    lastActivityAt,
    model: MODEL,
    ...extra,
  });
}

function buildYieldTool() {
  return {
    name: YIELD_TOOL_NAME,
    toolName: YIELD_TOOL_NAME,
    description:
      'Call this tool when you have finished your reply and want to wait for the next user message. ' +
      'The tool result string will be the next user message. ' +
      'You MUST call this tool at the END of every response, AFTER any other tool calls. ' +
      'Never end your turn without calling it.',
    providerIdentifier: 'cursoride2api-ratlc-pool',
    jsonSchema: { type: 'object', properties: {}, required: [] },
  };
}

function buildPrimingPrompt(system, callerTools) {
  // Cursor only reads tools from requestContextResult ONCE per stream
  // (empirically verified). So tools must be set at open time; the pool
  // manager recycles all channels when a request arrives with a different
  // tool list (in CONTRACT mode) or never recycles (in TRANSLATE mode).
  const lines = ['You are operating in RELAY mode behind a proxy. Each user message will be delivered via the `bajie_yield` tool result.'];
  // Be PERMISSIVE about the tool list: do not declare "your only tools are X
  // and Y". The model has the explicit tool list in its prompt; we want it
  // to feel free to use whatever's there, including Cursor's native built-ins
  // (Shell, Read, Write, Grep, ...) that get auto-injected alongside ours.
  lines.push('Inspect your available-tools list and use whatever tools are present as appropriate. Tools include any caller-registered MCP tools AND any Cursor-native built-ins (such as Shell/Read/Write/Grep/Glob/WebFetch/etc.) that may be present.');
  lines.push(`At the END of EVERY response (after any other tool calls), you MUST call \`${YIELD_TOOL_NAME}\` to wait for the next user message.`);
  if (POOL_CONTEXT_MODE === 'full') {
    // Full-context mode: each bajie_yield result carries the entire
    // conversation history for one self-contained request. The pool
    // does NOT preserve continuity across yields, so the model must
    // treat every delivery as an independent question whose only
    // context is what appears between the FULL CONVERSATION CONTEXT
    // delimiters.
    lines.push('Each `bajie_yield` tool result is the COMPLETE conversation context for ONE self-contained request, formatted with explicit delimiters (look for `=== FULL CONVERSATION CONTEXT ===`, `--- SYSTEM ---`, `--- CONVERSATION ---`, and `[user] (RESPOND TO THIS):`).');
    lines.push('Treat each `bajie_yield` delivery as INDEPENDENT. Do NOT assume any continuity with prior `bajie_yield` results. The only context you have is what is contained in the latest delivery. Respond only to the final user turn marked `(RESPOND TO THIS)`. Tool_use / tool_result blocks in the rendered history are PAST events — do not re-execute them.');
  } else {
    lines.push('The bajie_yield tool result is the next user message verbatim.');
  }
  lines.push('Never end your turn without calling bajie_yield. Never produce text outside of a normal response.');
  if (system) {
    lines.push(`Caller system context:\n---\n${typeof system === 'string' ? system : JSON.stringify(system)}\n---`);
  }
  lines.push('Reply with exactly "READY" to acknowledge, then call bajie_yield.');
  return lines.join('\n\n');
}

// ── Open with retry ──────────────────────────────────────────────────────
function openOnce(initialPrompt, allTools) {
  return new Promise((resolve) => {
    let yieldInfo = null;
    let textBuf = '';
    let b = null;
    b = startConversation(token, {
      prompt: initialPrompt,
      modelId: MODEL,
      tools: allTools,
      maxMode: true,
      passthroughNativeTools: PASSTHROUGH_NATIVE,
      onTextDelta: (t) => { textBuf += t; },
      onMcpCall: (info) => {
        if (info.toolName === YIELD_TOOL_NAME) {
          yieldInfo = info;
          resolve({ kind: 'opened', bridge: b, yieldInfo, textBuf });
        } else {
          // Pre-yield non-yield tool call shouldn't happen with our priming.
          try { b.sendToolResult(info.id, info.execId, { error: 'tool not available during priming' }); } catch { /* ignore */ }
        }
      },
      onThinkingDelta: () => {},
      onStepCompleted: () => {},
      onTurnEnded: () => { if (!yieldInfo) resolve({ kind: 'no_yield', textBuf }); },
      onError: (err) => {
        const msg = String(err?.message || err || '');
        if (/unpaid invoice|cursor\.com\/dashboard/i.test(msg)) resolve({ kind: 'unpaid', msg });
        else if (/RATE_LIMIT_EXCEEDED|rate limit|too many requests/i.test(msg)) resolve({ kind: 'rate_limit', msg });
        else resolve({ kind: 'other_error', msg });
      },
    });
  });
}

async function openWithRetry(system, callerTools) {
  const yieldTool = buildYieldTool();
  const cTools = (callerTools || []).map((t) => ({
    name: t.name,
    toolName: t.name,
    description: t.description || '',
    providerIdentifier: 'cursoride2api-ratlc-pool',
    jsonSchema: t.input_schema || t.jsonSchema || { type: 'object', properties: {}, required: [] },
  }));
  const allTools = [yieldTool, ...cTools];
  const primingPrompt = buildPrimingPrompt(system, cTools);

  setState('opening');
  let rateLimitBackoff = 5000;
  for (let attempt = 1; attempt <= OPEN_RETRY_MAX; attempt++) {
    openAttempts = attempt;
    if (attempt % 10 === 1) setState('opening');  // periodic state push
    const result = await openOnce(primingPrompt, allTools);
    if (result.kind === 'opened') {
      bridge = result.bridge;
      pendingYield = result.yieldInfo;
      openedAt = Date.now();
      lastActivityAt = Date.now();
      attachLiveCallbacks();
      setState('ready');
      return;
    }
    if (result.kind === 'unpaid') {
      await sleep(OPEN_RETRY_MS);
      continue;
    }
    if (result.kind === 'rate_limit') {
      send({ type: 'log', channelId: CHANNEL_ID, level: 'warn', message: `rate limit on attempt ${attempt}, backoff ${rateLimitBackoff}ms` });
      await sleep(rateLimitBackoff);
      rateLimitBackoff = Math.min(Math.floor(rateLimitBackoff * 1.5), 60000);
      continue;
    }
    if (result.kind === 'no_yield') {
      await sleep(OPEN_RETRY_MS);
      continue;
    }
    // Hard error — die.
    setState('dead', { error: result.msg || result.kind });
    process.exit(1);
  }
  setState('dead', { error: 'open exhausted' });
  process.exit(1);
}

// ── Live callbacks after open ────────────────────────────────────────────
function attachLiveCallbacks() {
  bridge.setCallbacks({
    onTextDelta: (t) => {
      if (currentRequestId == null) return;
      lastActivityAt = Date.now();
      send({ type: 'text_delta', channelId: CHANNEL_ID, requestId: currentRequestId, text: t });
    },
    onThinkingDelta: () => {},
    onMcpCall: (info) => {
      lastActivityAt = Date.now();
      if (info.toolName === YIELD_TOOL_NAME) {
        pendingYield = info;
        pendingMcpInfo = null;
        const finishedReqId = currentRequestId;
        currentRequestId = null;
        setState('ready');
        send({ type: 'yield', channelId: CHANNEL_ID, requestId: finishedReqId });
      } else {
        pendingMcpInfo = info;
        pendingYield = null;
        send({
          type: 'tool_use',
          channelId: CHANNEL_ID,
          requestId: currentRequestId,
          execId: info.execId,
          name: info.toolName,
          args: info.args,
        });
        // We stay busy until tool_result is fed.
      }
    },
    onStepCompleted: () => {},
    onTurnEnded: () => {
      // Shouldn't normally fire unless the model failed to yield.
      if (currentRequestId != null) {
        send({ type: 'error', channelId: CHANNEL_ID, requestId: currentRequestId, message: 'unexpected_turn_ended' });
        currentRequestId = null;
        setState('dead', { error: 'unexpected_turn_ended' });
        process.exit(1);
      }
    },
    onError: (err) => {
      const msg = String(err?.message || err || '');
      send({ type: 'error', channelId: CHANNEL_ID, requestId: currentRequestId, message: msg });
      currentRequestId = null;
      setState('dead', { error: msg });
      process.exit(1);
    },
  });
}

// ── Command handlers ─────────────────────────────────────────────────────
async function handleMessage(msg) {
  console.log(`[bridge-worker ch=${CHANNEL_ID}] IPC type=${msg.type} requestId=${msg.requestId || ''} state=${currentState} pendingYield=${!!pendingYield} pendingMcp=${pendingMcpInfo?.execId || 'none'}`);
  if (msg.type === 'open') {
    try {
      await openWithRetry(msg.system || '', msg.tools || []);
    } catch (e) {
      setState('dead', { error: e.message });
      process.exit(1);
    }
    return;
  }

  if (msg.type === 'send_user_message') {
    if (!pendingYield) {
      send({ type: 'error', channelId: CHANNEL_ID, requestId: msg.requestId, message: 'no_pending_yield (state=' + currentState + ')' });
      return;
    }
    currentRequestId = msg.requestId;
    setState('busy');
    lastActivityAt = Date.now();
    console.log(`[bridge-worker ch=${CHANNEL_ID}] BEFORE bridge.sendToolResult(yield_id=${pendingYield.id?.slice?.(0,8)}, yield_execId=${pendingYield.execId?.slice?.(0,8)}, textBytes=${(msg.text||'').length}) bridgeExists=${!!bridge} fnType=${typeof bridge?.sendToolResult}`);
    try {
      bridge.sendToolResult(pendingYield.id, pendingYield.execId, msg.text || '');
      console.log(`[bridge-worker ch=${CHANNEL_ID}] AFTER bridge.sendToolResult (returned cleanly)`);
    } catch (e) {
      console.log(`[bridge-worker ch=${CHANNEL_ID}] EXCEPTION in bridge.sendToolResult: ${e.message}\n${e.stack}`);
      send({ type: 'error', channelId: CHANNEL_ID, requestId: msg.requestId, message: 'sendToolResult threw: ' + e.message });
    }
    pendingYield = null;
    return;
  }

  if (msg.type === 'send_tool_result') {
    if (!pendingMcpInfo || (msg.execId && msg.execId !== pendingMcpInfo.execId)) {
      send({ type: 'error', channelId: CHANNEL_ID, requestId: msg.requestId,
        message: `no matching pending tool_use (have=${pendingMcpInfo?.execId} want=${msg.execId})` });
      return;
    }
    currentRequestId = msg.requestId;
    setState('busy');
    lastActivityAt = Date.now();
    bridge.sendToolResult(pendingMcpInfo.id, pendingMcpInfo.execId, msg.content || '');
    pendingMcpInfo = null;
    return;
  }

  if (msg.type === 'ping') {
    // Use the same path as send_user_message — fire a tiny prompt and wait for yield.
    if (!pendingYield) {
      send({ type: 'error', channelId: CHANNEL_ID, requestId: msg.requestId, message: 'cannot_ping: state=' + currentState });
      return;
    }
    currentRequestId = msg.requestId;
    setState('busy');
    lastActivityAt = Date.now();
    bridge.sendToolResult(pendingYield.id, pendingYield.execId, '[health-check] Reply with exactly: OK');
    pendingYield = null;
    return;
  }

  if (msg.type === 'shutdown') {
    try { bridge && bridge.close(); } catch { /* ignore */ }
    process.exit(0);
  }
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ── Wire up parent IPC ───────────────────────────────────────────────────
process.on('message', (msg) => {
  handleMessage(msg).catch((e) => {
    send({ type: 'error', channelId: CHANNEL_ID, message: 'handler crashed: ' + e.message });
  });
});

process.on('uncaughtException', (e) => {
  send({ type: 'error', channelId: CHANNEL_ID, message: 'uncaught: ' + e.message });
  setState('dead', { error: e.message });
  process.exit(1);
});

// Periodic heartbeat so manager can detect hangs.
setInterval(() => {
  send({ type: 'heartbeat', channelId: CHANNEL_ID, now: Date.now(), state: currentState, lastActivityAt });
}, 30_000);

setState('spawning');
