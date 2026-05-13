#!/usr/bin/env node
// API server — stateless HTTP front for the RATLC pool.
// Speaks Anthropic Messages on the client side; speaks Protocol B
// (newline-delimited JSON over Unix socket) to pool-manager.
//
// Restart-safe: holds no Cursor state. Pool manager owns the warm channels.

import http from 'node:http';
import net from 'node:net';
import { randomUUID } from 'node:crypto';

const PORT = parseInt(process.env.PORT || '4242', 10);
const HOST = process.env.HOST || '127.0.0.1';
const POOL_SOCK = process.env.POOL_SOCK || '/tmp/ratlc-pool.sock';

const log = (...args) => console.log(`[${new Date().toISOString().slice(11, 23)}] [api]`, ...args);

// ── Pool socket connection ──────────────────────────────────────────────
let poolSock = null;
let poolBuf = '';
const reqHandlers = new Map();      // requestId -> { onEvent }
let reconnectTimer = null;

function connectPool() {
  poolSock = net.createConnection(POOL_SOCK);
  poolSock.on('connect', () => {
    log(`connected to pool at ${POOL_SOCK}`);
    if (reconnectTimer) { clearTimeout(reconnectTimer); reconnectTimer = null; }
  });
  poolSock.on('data', (chunk) => {
    poolBuf += chunk.toString('utf8');
    let idx;
    while ((idx = poolBuf.indexOf('\n')) !== -1) {
      const line = poolBuf.slice(0, idx);
      poolBuf = poolBuf.slice(idx + 1);
      if (!line.trim()) continue;
      try {
        const msg = JSON.parse(line);
        const h = msg.requestId ? reqHandlers.get(msg.requestId) : null;
        if (h) h.onEvent(msg);
      } catch (e) {
        log('bad json from pool:', e.message);
      }
    }
  });
  poolSock.on('error', (e) => log('pool socket error:', e.message));
  poolSock.on('close', () => {
    log('pool socket closed; will retry in 2s');
    poolSock = null;
    // Fail any in-flight handlers
    for (const [reqId, h] of reqHandlers.entries()) {
      h.onEvent({ type: 'error', requestId: reqId, message: 'pool socket disconnected' });
    }
    reqHandlers.clear();
    if (!reconnectTimer) reconnectTimer = setTimeout(connectPool, 2000);
  });
}
connectPool();

function poolWrite(obj) {
  if (!poolSock || poolSock.destroyed) return false;
  try { poolSock.write(JSON.stringify(obj) + '\n'); return true; }
  catch { return false; }
}

// ── Anthropic SSE encoder ───────────────────────────────────────────────
function sseWrite(res, event, data) {
  if (!res || res.writableEnded) return;
  try { res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`); }
  catch (e) { /* client disconnect */ }
}

// ── Request handler ─────────────────────────────────────────────────────
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

function findToolResult(content) {
  if (!Array.isArray(content)) return null;
  for (const c of content) {
    if (c.type === 'tool_result') {
      const text = typeof c.content === 'string' ? c.content :
        Array.isArray(c.content) ? c.content.map((p) => p.type === 'text' ? p.text : JSON.stringify(p)).join('\n') : '';
      return { tool_use_id: c.tool_use_id, text };
    }
  }
  return null;
}

function extractSystemPrompt(system) {
  if (!system) return '';
  if (typeof system === 'string') return system;
  if (Array.isArray(system)) return system.map((p) => typeof p === 'string' ? p : p.text || '').join('\n');
  return '';
}

async function handleMessagesRequest(req, res) {
  let body;
  try { body = await readJsonBody(req); }
  catch (e) {
    res.writeHead(400, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify({ type: 'error', error: { type: 'invalid_request_error', message: 'bad json' } }));
  }
  const { messages, system, tools, model } = body;
  if (!Array.isArray(messages) || messages.length === 0) {
    res.writeHead(400, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify({ type: 'error', error: { type: 'invalid_request_error', message: 'messages required' } }));
  }
  const lastMsg = messages[messages.length - 1];
  if (lastMsg.role !== 'user') {
    res.writeHead(400, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify({ type: 'error', error: { type: 'invalid_request_error', message: 'last message must be user' } }));
  }

  // Decide what to send: tool_result or user message.
  const toolResult = findToolResult(lastMsg.content);
  const requestId = 'req-' + randomUUID().replace(/-/g, '').slice(0, 16);

  // Set up SSE
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'X-Accel-Buffering': 'no',
  });

  // Anthropic message bookkeeping
  const messageId = 'msg_' + randomUUID().replace(/-/g, '').slice(0, 24);
  let blockIdx = -1;
  let textBlockOpen = false;
  let outputTokens = 0;
  let stopReason = 'end_turn';
  let done = false;
  let toolUseEmitted = false;

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

  function startTextBlock() {
    blockIdx++;
    sseWrite(res, 'content_block_start', {
      type: 'content_block_start', index: blockIdx,
      content_block: { type: 'text', text: '' },
    });
    textBlockOpen = true;
  }

  function emitTextDelta(text) {
    if (!textBlockOpen) startTextBlock();
    outputTokens += Math.ceil(text.length / 4);
    sseWrite(res, 'content_block_delta', {
      type: 'content_block_delta', index: blockIdx,
      delta: { type: 'text_delta', text },
    });
  }

  function stopTextBlock() {
    if (!textBlockOpen) return;
    sseWrite(res, 'content_block_stop', { type: 'content_block_stop', index: blockIdx });
    textBlockOpen = false;
  }

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

  function finishMessage() {
    if (done) return;
    done = true;
    stopTextBlock();
    sseWrite(res, 'message_delta', {
      type: 'message_delta',
      delta: { stop_reason: stopReason, stop_sequence: null },
      usage: { input_tokens: 0, output_tokens: outputTokens },
    });
    sseWrite(res, 'message_stop', { type: 'message_stop' });
    try { res.end(); } catch { /* ignore */ }
    reqHandlers.delete(requestId);
  }

  startMsg();

  reqHandlers.set(requestId, {
    onEvent: (msg) => {
      if (msg.type === 'text_delta') {
        emitTextDelta(msg.text);
      } else if (msg.type === 'tool_use') {
        emitToolUseBlock(msg.anthropic_id, msg.name, msg.args);
        stopReason = 'tool_use';
        finishMessage();
      } else if (msg.type === 'yield') {
        stopReason = toolUseEmitted ? 'tool_use' : 'end_turn';
        finishMessage();
      } else if (msg.type === 'error') {
        sseWrite(res, 'error', { type: 'error', error: { type: 'api_error', message: msg.message } });
        finishMessage();
      }
    },
  });

  // Send to pool
  if (toolResult) {
    poolWrite({
      type: 'request', requestId, action: 'send_tool_result',
      anthropic_tool_use_id: toolResult.tool_use_id, content: toolResult.text,
    });
  } else {
    poolWrite({
      type: 'request', requestId, action: 'send_user_message',
      text: extractTextFromContent(lastMsg.content),
      system: extractSystemPrompt(system),
      tools: tools || [],
    });
  }

  // Handle client disconnect
  req.on('close', () => {
    if (!done) {
      log(`client disconnected mid-stream for ${requestId}`);
      finishMessage();
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
  // Open a one-shot socket to the pool — keeps administrative requests
  // off the main streaming socket.
  const sock = net.createConnection(POOL_SOCK);
  let buf = '';
  const timer = setTimeout(() => {
    try { sock.destroy(); } catch { /* ignore */ }
    if (!res.writableEnded) {
      res.writeHead(503, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: 'pool status timeout' }));
    }
  }, 5000);
  sock.on('connect', () => sock.write(JSON.stringify({ type: 'status' }) + '\n'));
  sock.on('data', (chunk) => {
    buf += chunk.toString('utf8');
    const idx = buf.indexOf('\n');
    if (idx === -1) return;
    try {
      const m = JSON.parse(buf.slice(0, idx));
      clearTimeout(timer);
      sock.end();
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(m));
    } catch { /* not json yet — keep waiting */ }
  });
  sock.on('error', (e) => {
    clearTimeout(timer);
    if (!res.writableEnded) {
      res.writeHead(503, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: e.message }));
    }
  });
}

// ── Server ──────────────────────────────────────────────────────────────
const server = http.createServer(async (req, res) => {
  const path = (req.url || '').split('?')[0];
  log(`${req.method} ${req.url}`);
  if (req.method === 'POST' && path === '/v1/messages') return handleMessagesRequest(req, res);
  if (req.method === 'GET' && (path === '/v1/models' || path === '/models')) return handleModels(req, res);
  if (req.method === 'GET' && path === '/health') return handleHealth(req, res);
  if (req.method === 'HEAD') { res.writeHead(200); return res.end(); }
  res.writeHead(404, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ error: 'not found' }));
});

server.listen(PORT, HOST, () => {
  log(`api-server listening on http://${HOST}:${PORT}`);
  log(`pool socket: ${POOL_SOCK}`);
});

process.on('SIGINT', () => { try { server.close(); } catch { /* ignore */ } process.exit(0); });
process.on('SIGTERM', () => { try { server.close(); } catch { /* ignore */ } process.exit(0); });
