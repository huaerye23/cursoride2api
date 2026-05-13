#!/usr/bin/env node
// RATLC API server — standalone Anthropic Messages endpoint backed by one
// persistent Cursor agent stream.
//
// Architecture:
//   [claude-code or curl]
//     → POST /v1/messages (Anthropic format, with tools)
//     → ratlc-api server (this file)
//     → bridge.sendToolResult(pendingYield, user_text)
//     → Cursor agent stream (opened once with retry-until-success)
//     → inner agent emits text + tool_use(...)
//     → text → SSE text_delta to client
//     → tool_use → SSE content_block(tool_use) to client, hold the call
//     → client sends next request with tool_result
//     → bridge.sendToolResult(pendingToolUseInfo, result) → inner agent continues
//
// One stream serves N requests until the model context bloats or Cursor kills
// the stream. Aggressive retry is paid once at server startup (or lazily on
// first request).
//
// Usage:
//   MODEL=claude-opus-4-7-thinking-max-fast PORT=4242 node scaffolding/ratlc-api.mjs
//
// Test:
//   curl -N http://127.0.0.1:4242/v1/messages -H "Content-Type: application/json" \
//     -d '{"model":"claude-opus-4-7-thinking-max-fast","messages":[{"role":"user","content":"hello"}],"max_tokens":50,"stream":true}'
//
// Point claude-code at it:
//   ANTHROPIC_BASE_URL=http://127.0.0.1:4242 ANTHROPIC_MODEL=claude-opus-4-7-thinking-max-fast claude

import http from 'node:http';
import { randomUUID } from 'node:crypto';
import { createRequire } from 'node:module';
import fs from 'node:fs';

const require = createRequire(import.meta.url);
const { startConversation } = require('../src/cursor-agent.js');

const PORT = parseInt(process.env.PORT || '4242', 10);
const HOST = process.env.HOST || '127.0.0.1';
const MODEL = process.env.MODEL || 'claude-opus-4-7-thinking-max-fast';
const OPEN_RETRY_MAX = parseInt(process.env.OPEN_RETRY_MAX || '300', 10);
const OPEN_RETRY_MS = parseInt(process.env.OPEN_RETRY_MS || '300', 10);

const tokenFile = JSON.parse(fs.readFileSync(new URL('../token.json', import.meta.url), 'utf8'));
const token = tokenFile.tokens[0];

const YIELD_TOOL_NAME = 'bajie_yield';

// ── State (module-level — one bridge per server lifetime) ────────────────
let bridge = null;
let bridgeReady = false;
let openingPromise = null;
let openAttempts = 0;

let pendingYield = null;        // { id, execId, toolName: 'bajie_yield' }
let pendingToolUseInfo = null;  // { id, execId, toolName, args, anthropicToolUseId }

// Current in-flight SSE response state. Only one request at a time.
let currentResponse = null;
let currentMessageId = null;
let currentModel = null;
let currentBlockIdx = -1;
let textBlockOpen = false;
let inputTokensSoFar = 0;
let outputTokensSoFar = 0;

// Captured caller tools (learned from first request)
let callerToolDefs = [];

// Queue of incoming requests when a response is already in flight
const pendingRequests = [];

// ── logging ──────────────────────────────────────────────────────────────
const log = (...a) => console.log(`[${new Date().toISOString().slice(11, 23)}]`, ...a);

// ── Anthropic SSE writer ─────────────────────────────────────────────────
function sse(event, data) {
  if (!currentResponse || currentResponse.writableEnded) return;
  try {
    currentResponse.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
  } catch (e) {
    log('sse write failed:', e.message);
  }
}

function startMessage(model) {
  currentMessageId = 'msg_' + randomUUID().replace(/-/g, '').slice(0, 24);
  currentBlockIdx = -1;
  textBlockOpen = false;
  inputTokensSoFar = 0;
  outputTokensSoFar = 0;
  sse('message_start', {
    type: 'message_start',
    message: {
      id: currentMessageId,
      type: 'message',
      role: 'assistant',
      content: [],
      model: model || currentModel,
      stop_reason: null,
      stop_sequence: null,
      usage: { input_tokens: 0, output_tokens: 0 },
    },
  });
  sse('ping', { type: 'ping' });
}

function startTextBlock() {
  currentBlockIdx++;
  sse('content_block_start', {
    type: 'content_block_start',
    index: currentBlockIdx,
    content_block: { type: 'text', text: '' },
  });
  textBlockOpen = true;
}

function emitTextDelta(text) {
  if (!text) return;
  if (!textBlockOpen) startTextBlock();
  outputTokensSoFar += Math.ceil(text.length / 4);
  sse('content_block_delta', {
    type: 'content_block_delta',
    index: currentBlockIdx,
    delta: { type: 'text_delta', text },
  });
}

function stopCurrentBlock() {
  if (currentBlockIdx < 0) return;
  sse('content_block_stop', { type: 'content_block_stop', index: currentBlockIdx });
  textBlockOpen = false;
}

function emitToolUseBlock(anthropicId, toolName, args) {
  if (textBlockOpen) stopCurrentBlock();
  currentBlockIdx++;
  sse('content_block_start', {
    type: 'content_block_start',
    index: currentBlockIdx,
    content_block: { type: 'tool_use', id: anthropicId, name: toolName, input: {} },
  });
  // Stream input as one big input_json_delta (Anthropic spec allows this)
  const partialJson = JSON.stringify(args || {});
  sse('content_block_delta', {
    type: 'content_block_delta',
    index: currentBlockIdx,
    delta: { type: 'input_json_delta', partial_json: partialJson },
  });
  sse('content_block_stop', { type: 'content_block_stop', index: currentBlockIdx });
}

function finishMessage(stopReason) {
  if (textBlockOpen) stopCurrentBlock();
  sse('message_delta', {
    type: 'message_delta',
    delta: { stop_reason: stopReason, stop_sequence: null },
    usage: { input_tokens: inputTokensSoFar, output_tokens: outputTokensSoFar },
  });
  sse('message_stop', { type: 'message_stop' });
  if (currentResponse) {
    try { currentResponse.end(); } catch { /* ignore */ }
  }
  currentResponse = null;
  // Drain queued requests, if any
  drainQueue();
}

function drainQueue() {
  if (pendingRequests.length > 0 && !currentResponse) {
    const next = pendingRequests.shift();
    log(`draining: ${pendingRequests.length} still queued`);
    setImmediate(() => processMessagesRequest(next.req, next.res, next.body));
  }
}

// ── Stream open (with retry-until-success) ───────────────────────────────
function buildYieldTool() {
  return {
    name: YIELD_TOOL_NAME,
    toolName: YIELD_TOOL_NAME,
    description:
      'Call this tool when you have finished your reply and want to wait for the next user message. ' +
      'The tool result string will be the next user message. ' +
      'You MUST call this tool at the END of every response, AFTER any other tool calls. ' +
      'Never end your turn without calling it.',
    providerIdentifier: 'cursoride2api-ratlc-api',
    jsonSchema: { type: 'object', properties: {}, required: [] },
  };
}

function buildPrimingPrompt(system, callerTools) {
  const parts = [];
  parts.push('You are operating in RELAY mode behind a proxy. Each user message will be delivered via the `bajie_yield` tool result.');
  if (callerTools && callerTools.length > 0) {
    parts.push(`You have these tools available: ${callerTools.map((t) => t.name).join(', ')}, and ${YIELD_TOOL_NAME}.`);
    parts.push('Use any caller tool naturally when the task calls for it.');
  } else {
    parts.push(`The only tool you have is ${YIELD_TOOL_NAME}.`);
  }
  parts.push(`At the END of EVERY response (after any other tool calls), you MUST call \`${YIELD_TOOL_NAME}\` to wait for the next user message.`);
  parts.push('The bajie_yield tool result is the next user message verbatim.');
  parts.push('Never end your turn without calling bajie_yield. Never produce text outside of a normal response.');
  if (system) {
    const sys = typeof system === 'string'
      ? system
      : Array.isArray(system) ? system.map((p) => (typeof p === 'string' ? p : p.text || '')).join('\n') : '';
    if (sys) parts.push(`Caller's system context (incorporate when responding):\n---\n${sys}\n---`);
  }
  parts.push('Reply with exactly "READY" to acknowledge this priming, then call bajie_yield.');
  return parts.join('\n\n');
}

function openOnce(initialPrompt, tools) {
  return new Promise((resolve) => {
    let yieldInfo = null;
    let textBuf = '';
    let b = null;
    b = startConversation(token, {
      prompt: initialPrompt,
      modelId: MODEL,
      tools,
      maxMode: true,
      onTextDelta: (t) => { textBuf += t; },
      onMcpCall: (info) => {
        if (info.toolName === YIELD_TOOL_NAME) {
          yieldInfo = info;
          resolve({ kind: 'opened', bridge: b, yieldInfo, textBuf });
        } else {
          // Pre-yield tool call — shouldn't happen with our priming prompt
          log(`unexpected pre-yield tool call: ${info.toolName}`);
          try { b.sendToolResult(info.id, info.execId, { error: 'not available during priming' }); } catch { /* ignore */ }
        }
      },
      onThinkingDelta: () => {},
      onStepCompleted: () => {},
      onTurnEnded: () => {
        if (!yieldInfo) resolve({ kind: 'no_yield', textBuf });
      },
      onError: (err) => {
        const msg = String(err?.message || err || '');
        if (/unpaid invoice|cursor\.com\/dashboard/i.test(msg)) resolve({ kind: 'unpaid', msg });
        else if (/RATE_LIMIT_EXCEEDED|rate limit/i.test(msg)) resolve({ kind: 'rate_limit', msg });
        else resolve({ kind: 'other_error', msg });
      },
    });
  });
}

// Compare requested tool list to the bridge's current tools — if they
// differ, the bridge needs to be closed and re-opened so the new tools
// are visible to the inner Cursor agent. Same names + same schemas = no
// re-open. Name-only mismatch (e.g., one extra tool) = re-open.
function toolsSignature(toolList) {
  if (!Array.isArray(toolList)) return '';
  return toolList
    .filter((t) => t && t.name)
    .map((t) => `${t.name}:${JSON.stringify(t.input_schema || t.jsonSchema || {})}`)
    .sort()
    .join('|');
}

let currentToolSig = '';

async function ensureBridge(system, tools) {
  const incomingSig = toolsSignature(tools || []);
  if (bridgeReady && incomingSig === currentToolSig) return;
  if (bridgeReady && incomingSig !== currentToolSig) {
    log(`tools changed (was=[${currentToolSig.slice(0, 80)}] now=[${incomingSig.slice(0, 80)}]) — closing bridge to re-open`);
    try { bridge.close(); } catch { /* ignore */ }
    bridge = null;
    bridgeReady = false;
    pendingYield = null;
    pendingToolUseInfo = null;
    callerToolDefs = [];
    openingPromise = null;
    openAttempts = 0;
  }
  if (openingPromise) return openingPromise;
  currentToolSig = incomingSig;
  openingPromise = (async () => {
    log(`opening RATLC stream (model=${MODEL}, tools=${tools.length}, retry up to ${OPEN_RETRY_MAX})...`);
    const yieldTool = buildYieldTool();
    callerToolDefs = tools.map((t) => ({
      name: t.name,
      toolName: t.name,
      description: t.description || '',
      providerIdentifier: 'cursoride2api-ratlc-api',
      jsonSchema: t.input_schema || t.jsonSchema || { type: 'object', properties: {}, required: [] },
    }));
    const allTools = [yieldTool, ...callerToolDefs];
    const primingPrompt = buildPrimingPrompt(system, callerToolDefs);

    const t0 = Date.now();
    // Two retry schedules: unpaid_invoice (fast, probabilistic) and
    // rate_limit (slow, time-based — the per-account hard limit that fires
    // after ~500 reqs in a window and clears in seconds to minutes).
    let rateLimitBackoffMs = 5000;
    for (let attempt = 1; attempt <= OPEN_RETRY_MAX; attempt++) {
      openAttempts = attempt;
      const result = await openOnce(primingPrompt, allTools);
      if (result.kind === 'opened') {
        bridge = result.bridge;
        pendingYield = result.yieldInfo;
        bridgeReady = true;
        attachBridgeCallbacks();
        log(`\n✅ stream opened on attempt ${attempt} (${((Date.now() - t0) / 1000).toFixed(1)}s wall)`);
        log(`   pre-yield text: ${JSON.stringify(result.textBuf.slice(0, 80))}`);
        return;
      }
      if (result.kind === 'unpaid') {
        if (attempt % 10 === 0) process.stdout.write(`[${attempt}]`);
        else process.stdout.write('.');
        await new Promise((r) => setTimeout(r, OPEN_RETRY_MS));
        continue;
      }
      if (result.kind === 'rate_limit') {
        log(`\nrate-limit hit on attempt ${attempt}; backing off ${rateLimitBackoffMs}ms`);
        await new Promise((r) => setTimeout(r, rateLimitBackoffMs));
        rateLimitBackoffMs = Math.min(Math.floor(rateLimitBackoffMs * 1.5), 60_000);
        continue;
      }
      log(`\nopen attempt ${attempt} non-retryable: ${result.kind} ${result.msg || ''}`);
      if (result.kind === 'no_yield') {
        await new Promise((r) => setTimeout(r, OPEN_RETRY_MS));
        continue;
      }
      throw new Error(`open failed: ${result.msg}`);
    }
    throw new Error(`open exhausted after ${OPEN_RETRY_MAX} attempts`);
  })();
  return openingPromise;
}

function attachBridgeCallbacks() {
  bridge.setCallbacks({
    onTextDelta: (t) => {
      if (currentResponse) emitTextDelta(t);
    },
    onThinkingDelta: () => {},
    onMcpCall: (info) => {
      if (info.toolName === YIELD_TOOL_NAME) {
        log(`← yield (round done)`);
        pendingYield = info;
        pendingToolUseInfo = null;
        if (currentResponse) finishMessage('end_turn');
      } else {
        const anthropicId = 'toolu_' + randomUUID().replace(/-/g, '').slice(0, 16);
        pendingToolUseInfo = { ...info, anthropicToolUseId: anthropicId };
        pendingYield = null;
        log(`← tool_use ${info.toolName}(${JSON.stringify(info.args).slice(0, 80)}) → forwarding to client`);
        if (currentResponse) {
          emitToolUseBlock(anthropicId, info.toolName, info.args);
          finishMessage('tool_use');
        }
      }
    },
    onStepCompleted: () => {},
    onTurnEnded: () => {
      // Should not happen unless something broke (the model always yields)
      log(`unexpected onTurnEnded — closing current response if any`);
      if (currentResponse) finishMessage('end_turn');
    },
    onError: (err) => {
      const msg = String(err?.message || err || '');
      log(`bridge error: ${msg.slice(0, 200)}`);
      if (currentResponse) {
        sse('error', { type: 'error', error: { type: 'api_error', message: msg } });
        try { currentResponse.end(); } catch { /* ignore */ }
        currentResponse = null;
        drainQueue();
      }
    },
  });
}

// ── HTTP handlers ────────────────────────────────────────────────────────
async function readJsonBody(req) {
  const chunks = [];
  for await (const c of req) chunks.push(c);
  return JSON.parse(Buffer.concat(chunks).toString('utf8'));
}

function extractTextFromContent(content) {
  if (typeof content === 'string') return content;
  if (!Array.isArray(content)) return '';
  return content.filter((c) => c.type === 'text').map((c) => c.text || '').join('\n');
}

function findToolResult(content, anthropicId) {
  if (!Array.isArray(content)) return null;
  const tr = content.find((c) => c.type === 'tool_result' && c.tool_use_id === anthropicId);
  if (!tr) return null;
  if (typeof tr.content === 'string') return tr.content;
  if (Array.isArray(tr.content)) return tr.content.map((p) => (p.type === 'text' ? p.text : JSON.stringify(p))).join('\n');
  return '';
}

async function processMessagesRequest(req, res, body) {
  const { messages, system, model: requestedModel, tools, max_tokens, stream } = body;
  currentModel = requestedModel || MODEL;

  if (!Array.isArray(messages) || messages.length === 0) {
    res.writeHead(400, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify({ type: 'error', error: { type: 'invalid_request_error', message: 'messages required' } }));
  }
  try {
    await ensureBridge(system, tools || []);
  } catch (e) {
    log(`ensureBridge failed: ${e.message}`);
    res.writeHead(500, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify({ type: 'error', error: { type: 'api_error', message: e.message } }));
  }

  // If a response is already in flight, queue this one
  if (currentResponse) {
    log(`queue: another response in flight; queuing this request`);
    pendingRequests.push({ req, res, body });
    return;
  }

  // Begin SSE
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'X-Accel-Buffering': 'no',
  });
  currentResponse = res;
  startMessage(currentModel);

  // Detect what to feed: tool_result or new user message
  const lastMsg = messages[messages.length - 1];
  if (lastMsg.role !== 'user') {
    log('last message is not from user — closing turn');
    return finishMessage('end_turn');
  }
  let fed = false;

  if (pendingToolUseInfo) {
    const toolResultText = findToolResult(lastMsg.content, pendingToolUseInfo.anthropicToolUseId);
    if (toolResultText != null) {
      log(`→ tool_result for ${pendingToolUseInfo.toolName} (${toolResultText.length}b)`);
      bridge.sendToolResult(pendingToolUseInfo.id, pendingToolUseInfo.execId, toolResultText);
      pendingToolUseInfo = null;
      fed = true;
    }
  }

  if (!fed) {
    const userText = extractTextFromContent(lastMsg.content);
    inputTokensSoFar = Math.ceil(userText.length / 4);
    if (pendingYield) {
      log(`→ user msg (${userText.length}b) "${userText.slice(0, 60).replace(/\n/g, ' ')}"`);
      bridge.sendToolResult(pendingYield.id, pendingYield.execId, userText);
      pendingYield = null;
      fed = true;
    }
  }

  if (!fed) {
    log('no pending state — cannot feed; closing turn');
    return finishMessage('end_turn');
  }

  // Response will be emitted via bridge callbacks
  // Client may disconnect during stream — handle gracefully
  req.on('close', () => {
    if (res === currentResponse && !res.writableEnded) {
      log('client disconnected mid-stream');
      try { res.end(); } catch { /* ignore */ }
      currentResponse = null;
      // Don't drain queue yet — bridge is still processing.
      // Will drain naturally when next yield/tool_use fires.
    }
  });
}

function handleModels(req, res) {
  res.writeHead(200, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({
    data: [
      { id: 'claude-opus-4-7-thinking-max-fast', type: 'model', display_name: 'Claude Opus 4.7 Thinking Max (Fast)', created_at: '2026-01-01T00:00:00Z' },
      { id: 'claude-4.6-opus-max-thinking-fast', type: 'model', display_name: 'Claude Opus 4.6 Max Thinking (Fast)', created_at: '2026-01-01T00:00:00Z' },
    ],
  }));
}

function handleHealth(req, res) {
  res.writeHead(200, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({
    ok: bridgeReady,
    model: MODEL,
    bridgeReady,
    openAttempts,
    state: pendingToolUseInfo ? 'awaiting_tool_result'
      : pendingYield ? 'awaiting_user_message'
      : currentResponse ? 'streaming'
      : 'idle',
    pendingTool: pendingToolUseInfo ? pendingToolUseInfo.toolName : null,
    queuedRequests: pendingRequests.length,
    callerToolCount: callerToolDefs.length,
  }));
}

// ── Server ───────────────────────────────────────────────────────────────
const server = http.createServer(async (req, res) => {
  // Strip query string for routing (claude-code sends e.g. /v1/messages?beta=true)
  const path = (req.url || '').split('?')[0];
  log(`${req.method} ${req.url}`);
  if (req.method === 'POST' && path === '/v1/messages') {
    let body;
    try { body = await readJsonBody(req); }
    catch (e) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify({ type: 'error', error: { type: 'invalid_request_error', message: 'bad json' } }));
    }
    return processMessagesRequest(req, res, body);
  }
  if (req.method === 'GET' && (path === '/v1/models' || path === '/models')) {
    return handleModels(req, res);
  }
  if (req.method === 'GET' && path === '/health') {
    return handleHealth(req, res);
  }
  // claude-code probes with HEAD / before opening — respond 200 so it doesn't
  // assume the endpoint is dead.
  if (req.method === 'HEAD') {
    res.writeHead(200);
    return res.end();
  }
  res.writeHead(404, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ error: 'not found', method: req.method, url: req.url }));
});

server.listen(PORT, HOST, () => {
  console.log('');
  console.log('╔═══════════════════════════════════════════╗');
  console.log('║       RATLC API Server (scaffolding)      ║');
  console.log('╠═══════════════════════════════════════════╣');
  console.log(`║  🌐 http://${HOST}:${PORT}              `);
  console.log(`║  🤖 Model: ${MODEL}`);
  console.log('║  🔌 POST /v1/messages                     ║');
  console.log('║  📋 GET  /v1/models                       ║');
  console.log('║  ❤️  GET  /health                         ║');
  console.log('╚═══════════════════════════════════════════╝');
  console.log('');
  console.log('Stream will open lazily on first /v1/messages request.');
  console.log('Expected first-request latency: 30-300s (retry lottery).');
});

process.on('SIGINT', () => {
  log('shutting down');
  if (bridge) try { bridge.close(); } catch { /* ignore */ }
  server.close();
  process.exit(0);
});
