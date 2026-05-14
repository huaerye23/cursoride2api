#!/usr/bin/env node
// Bridge worker — one forked child process per RATLC channel.
// Owns ONE Cursor agent stream. Communicates with parent via process.send.
// See ./IPC.md "Protocol A" for the message schema.

import { createRequire } from 'node:module';
import fs from 'node:fs';

const require = createRequire(import.meta.url);
const { startConversation } = require('../../src/cursor-agent.js');

const CHANNEL_ID = process.env.RATLC_CHANNEL_ID || 'ch-?';
const MODEL = process.env.RATLC_MODEL || 'claude-opus-4-7-thinking-max-fast';
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
  // tool list.
  const lines = ['You are operating in RELAY mode behind a proxy. Each user message will be delivered via the `bajie_yield` tool result.'];
  if (callerTools && callerTools.length > 0) {
    lines.push(`Available tools: ${callerTools.map((t) => t.name).join(', ')}, and ${YIELD_TOOL_NAME}.`);
    lines.push('Use caller tools when the task requires them. Skip them when not needed.');
  } else {
    lines.push(`The only tool you have is ${YIELD_TOOL_NAME}.`);
  }
  lines.push(`At the END of EVERY response (after any other tool calls), you MUST call \`${YIELD_TOOL_NAME}\` to wait for the next user message.`);
  lines.push('The bajie_yield tool result is the next user message verbatim.');
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
    bridge.sendToolResult(pendingYield.id, pendingYield.execId, msg.text || '');
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
