#!/usr/bin/env node
// API server — stateless HTTP front for the RATLC pool.
// Speaks Anthropic Messages on the client side; speaks Protocol B
// (newline-delimited JSON over Unix socket) to pool-manager.
//
// Restart-safe: holds no Cursor state. Pool manager owns the warm channels.

import http from 'node:http';
import net from 'node:net';
import { randomUUID } from 'node:crypto';
import { cursorToAnthropic, isInternalTool } from './tool-translator.mjs';

const PORT = parseInt(process.env.PORT || '4242', 10);
const HOST = process.env.HOST || '127.0.0.1';
const POOL_SOCK = process.env.POOL_SOCK || '/tmp/ratlc-pool.sock';
const POOL_TOOL_MODE = (process.env.POOL_TOOL_MODE || 'contract').toLowerCase();
// POOL_CONTEXT_MODE selects how multi-turn conversations are forwarded
// to the pool channel:
//   last (default) — only the last user message text is sent. Backwards-
//                    compatible. Pool channels accumulate per-conversation
//                    state inside the model's context window, so multi-turn
//                    coherence requires every turn of one conversation to
//                    land on the SAME channel. LRU rotation breaks this.
//   full           — every POST renders the entire messages[] history into
//                    one self-contained prompt. The channel is treated as
//                    a stateless carrier — each `bajie_yield` result is a
//                    complete fresh request. Channel rotation is now safe.
const POOL_CONTEXT_MODE = (process.env.POOL_CONTEXT_MODE || 'last').toLowerCase();
if (!['full', 'last'].includes(POOL_CONTEXT_MODE)) {
  console.error(`invalid POOL_CONTEXT_MODE=${POOL_CONTEXT_MODE} (must be full|last)`);
  process.exit(1);
}

const log = (...args) => console.log(`[${new Date().toISOString().slice(11, 23)}] [api]`, ...args);
log(`POOL_CONTEXT_MODE=${POOL_CONTEXT_MODE}  POOL_TOOL_MODE=${POOL_TOOL_MODE}`);

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

// Render an Anthropic content block array as a flat string, stable across
// nesting shapes. Used by renderFullContext to expand both top-level message
// content and the inner content of tool_result blocks.
function renderContentBlocks(blocks) {
  if (typeof blocks === 'string') return blocks;
  if (!Array.isArray(blocks)) return '';
  const out = [];
  for (const c of blocks) {
    if (!c || typeof c !== 'object') continue;
    if (c.type === 'text') {
      out.push(c.text || '');
    } else if (c.type === 'tool_use') {
      // Show the assistant's tool call: name + JSON args.
      const args = c.input == null ? {} : c.input;
      out.push(`<tool_use name="${c.name || '?'}" id="${c.id || ''}">\n${JSON.stringify(args, null, 2)}\n</tool_use>`);
    } else if (c.type === 'tool_result') {
      // Recursively render nested content blocks. Anthropic SDK allows the
      // result body to be either a string or an array of {type:text|image}
      // entries; both shapes are handled.
      const inner = typeof c.content === 'string'
        ? c.content
        : (Array.isArray(c.content) ? renderContentBlocks(c.content) : '');
      const err = c.is_error ? ' is_error="true"' : '';
      out.push(`<tool_result tool_use_id="${c.tool_use_id || ''}"${err}>\n${inner}\n</tool_result>`);
    } else if (c.type === 'image') {
      out.push('<image/>');
    } else if (typeof c.text === 'string') {
      // Tolerate untyped {text:"..."} entries (older SDKs).
      out.push(c.text);
    } else {
      // Unknown block type — dump as JSON so nothing is silently dropped.
      out.push(`<unknown type="${c.type || '?'}">${JSON.stringify(c).slice(0, 500)}</unknown>`);
    }
  }
  return out.join('\n');
}

// Render the entire messages[] history into a single self-contained prompt.
// Used when POOL_CONTEXT_MODE=full so the pool channel (which is stateless
// across conversation turns under LRU rotation) gets the full context every
// turn. Format design goals:
//   - Clearly delimit user vs assistant turns
//   - Expand tool_use blocks (tool name + args) and tool_result blocks
//   - End with the latest user turn marked as the one to respond to
//   - Stable across content-shape variations (string vs array, nested
//     tool_result.content of either shape)
function renderFullContext({ messages, system, tools }) {
  const lines = [];
  lines.push('=== FULL CONVERSATION CONTEXT ===');
  lines.push('You are receiving the complete conversation history for ONE self-contained request. Respond to the FINAL user turn below. Do not assume any continuity with prior bajie_yield results — each delivery is independent and the history below is the only context you have.');
  lines.push('');

  const sys = extractSystemPrompt(system);
  if (sys) {
    lines.push('--- SYSTEM ---');
    lines.push(sys);
    lines.push('');
  }

  if (Array.isArray(tools) && tools.length > 0) {
    lines.push('--- AVAILABLE TOOLS (for reference; use the live tool list bound to this stream) ---');
    for (const t of tools) {
      if (!t || !t.name) continue;
      const desc = t.description ? ` — ${String(t.description).slice(0, 200)}` : '';
      lines.push(`* ${t.name}${desc}`);
    }
    lines.push('');
  }

  lines.push('--- CONVERSATION ---');
  const arr = Array.isArray(messages) ? messages : [];
  for (let i = 0; i < arr.length; i++) {
    const m = arr[i];
    if (!m || !m.role) continue;
    const isLastUser = (i === arr.length - 1) && m.role === 'user';
    const tag = isLastUser ? `[user] (RESPOND TO THIS)` : `[${m.role}]`;
    lines.push(tag + ':');
    const body = typeof m.content === 'string'
      ? m.content
      : renderContentBlocks(m.content);
    lines.push(body || '(empty)');
    lines.push('');
  }

  lines.push('--- END CONVERSATION ---');
  lines.push('Respond to the final user turn now. Then call bajie_yield to wait for the next request.');
  return lines.join('\n');
}

async function handleMessagesRequest(req, res) {
  let body;
  try { body = await readJsonBody(req); }
  catch (e) {
    res.writeHead(400, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify({ type: 'error', error: { type: 'invalid_request_error', message: 'bad json' } }));
  }
  const { messages, system, tools, model } = body;
  if (process.env.LOG_REQUEST_TOOLS === '1') {
    log(`incoming /v1/messages: tools=${Array.isArray(tools) ? tools.length : 0} [${(tools || []).map((t) => t.name).slice(0, 30).join(', ')}]  system=${typeof system === 'string' ? system.length + 'c' : Array.isArray(system) ? 'array(' + system.length + ')' : 'none'}  model=${model || '(default)'}`);
  }
  // Body summary — every POST gets a one-liner showing the LAST message's
  // shape. This is the ONE log line you need to see whether a POST is a
  // tool_result round-trip or a fresh user turn.
  if (Array.isArray(messages) && messages.length > 0) {
    const last = messages[messages.length - 1];
    let summary;
    if (typeof last.content === 'string') {
      summary = `text="${last.content.slice(0, 80).replace(/\n/g, '\\n')}"`;
    } else if (Array.isArray(last.content)) {
      const parts = last.content.map((c) => {
        if (c.type === 'tool_result') return `tool_result(id=${c.tool_use_id}, ${typeof c.content === 'string' ? c.content.length + 'c' : 'blocks=' + (Array.isArray(c.content) ? c.content.length : '?')}${c.is_error ? ', is_error=true' : ''})`;
        if (c.type === 'text') return `text(${(c.text || '').length}c)`;
        if (c.type === 'image') return 'image';
        return c.type;
      });
      summary = parts.join(', ');
    } else {
      summary = `content type=${typeof last.content}`;
    }
    log(`  body: lastMsg.role=${last.role} content=[${summary}] msgCount=${messages.length}`);
  }
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
      delta: { type: 'input_json_delta', partial_json: '' },
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
      usage: {
        input_tokens: 0,
        output_tokens: outputTokens,
        cache_creation_input_tokens: 0,
        cache_read_input_tokens: 0,
        server_tool_use: null,
      },
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
        // In contract mode, names are passed through unchanged.
        // In translate mode, the model emitted a Cursor name (e.g. Shell);
        // we map to the Anthropic name (Bash) and adapt args. If the
        // Cursor tool has no Anthropic equivalent, we silently reject
        // back to the inner agent by sending a tool_error result via
        // the pool socket — the agent picks a different approach.
        if (POOL_TOOL_MODE === 'translate' && !isInternalTool(msg.name)) {
          const xlated = cursorToAnthropic(msg.name, msg.args || {});
          if (!xlated.ok) {
            // Rejection — feed the error back through the pool to the inner
            // agent. The api-server's request stream stays open; the inner
            // agent will keep generating after seeing this tool_result.
            poolWrite({
              type: 'request',
              requestId: requestId + ':auto_reject',
              action: 'send_tool_result',
              anthropic_tool_use_id: msg.anthropic_id,
              content: `[proxy_error] ${xlated.error}`,
            });
            // Don't emit anything to the client — pretend the tool_use
            // never happened from claude-code's POV.
            return;
          }
          log(`→ tool_use to client (translated): name=${xlated.name} args=${JSON.stringify(xlated.input).slice(0, 200)}`);
          emitToolUseBlock(msg.anthropic_id, xlated.name, xlated.input);
        } else {
          log(`→ tool_use to client: name=${msg.name} args=${JSON.stringify(msg.args).slice(0, 200)}`);
          emitToolUseBlock(msg.anthropic_id, msg.name, msg.args);
        }
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
    log(`  → pool send_tool_result requestId=${requestId} tool_use_id=${toolResult.tool_use_id} bytes=${toolResult.text.length}`);
    poolWrite({
      type: 'request', requestId, action: 'send_tool_result',
      anthropic_tool_use_id: toolResult.tool_use_id, content: toolResult.text,
    });
  } else {
    // Mode selection: in `full` mode, render the ENTIRE messages[] into
    // one self-contained prompt; in `last` mode (default, backwards-
    // compatible), forward only the last user message text. The pool
    // socket frame is identical in both — only the `text` payload changes.
    const text = POOL_CONTEXT_MODE === 'full'
      ? renderFullContext({ messages, system, tools })
      : extractTextFromContent(lastMsg.content);
    log(`  → pool send_user_message requestId=${requestId} mode=${POOL_CONTEXT_MODE} textBytes=${text.length} msgCount=${messages.length} tools=${(tools || []).length}`);
    poolWrite({
      type: 'request', requestId, action: 'send_user_message',
      text,
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

function handleMetrics(req, res) {
  // Prometheus-style text exposition. Pull from pool's status snapshot,
  // augment with api-server-local counters (TODO).
  const sock = net.createConnection(POOL_SOCK);
  let buf = '';
  const t = setTimeout(() => { try { sock.destroy(); } catch {} ; if (!res.writableEnded) { res.writeHead(503); res.end(''); } }, 5000);
  sock.on('connect', () => sock.write(JSON.stringify({ type: 'status' }) + '\n'));
  sock.on('data', (c) => {
    buf += c.toString('utf8');
    const idx = buf.indexOf('\n');
    if (idx === -1) return;
    try {
      const m = JSON.parse(buf.slice(0, idx));
      clearTimeout(t); sock.end();
      const lines = [];
      const p = m.pool || {};
      const cfg = m.config || {};
      lines.push('# HELP ratlc_pool_channels_total Channels alive in the pool.');
      lines.push('# TYPE ratlc_pool_channels_total gauge');
      lines.push(`ratlc_pool_channels_total{model="${cfg.model || ''}",mode="${cfg.toolMode || ''}"} ${p.actualSize || 0}`);
      lines.push('# HELP ratlc_pool_channels_target Target channel count.');
      lines.push('# TYPE ratlc_pool_channels_target gauge');
      lines.push(`ratlc_pool_channels_target ${p.configuredSize || 0}`);
      lines.push('# HELP ratlc_pool_channels_by_state Channels by state.');
      lines.push('# TYPE ratlc_pool_channels_by_state gauge');
      lines.push(`ratlc_pool_channels_by_state{state="ready"} ${p.readyCount || 0}`);
      lines.push(`ratlc_pool_channels_by_state{state="busy"} ${p.busyCount || 0}`);
      lines.push(`ratlc_pool_channels_by_state{state="opening"} ${p.openingCount || 0}`);
      lines.push(`ratlc_pool_channels_by_state{state="dead"} ${p.deadCount || 0}`);
      lines.push('# HELP ratlc_pool_pending_requests Requests queued awaiting a ready channel.');
      lines.push('# TYPE ratlc_pool_pending_requests gauge');
      lines.push(`ratlc_pool_pending_requests ${p.pendingRequests || 0}`);
      lines.push('# HELP ratlc_pool_tool_use_held Tool_use round-trips currently held awaiting tool_result.');
      lines.push('# TYPE ratlc_pool_tool_use_held gauge');
      lines.push(`ratlc_pool_tool_use_held ${p.toolUseIndex || 0}`);
      lines.push('# HELP ratlc_channel_rounds Successful rounds served per channel.');
      lines.push('# TYPE ratlc_channel_rounds counter');
      for (const ch of (p.channels || [])) {
        lines.push(`ratlc_channel_rounds{channel="${ch.id}"} ${ch.roundsServed || 0}`);
        lines.push(`ratlc_channel_open_attempts{channel="${ch.id}"} ${ch.openAttempts || 0}`);
      }
      res.writeHead(200, { 'Content-Type': 'text/plain; version=0.0.4' });
      res.end(lines.join('\n') + '\n');
    } catch { /* keep accumulating */ }
  });
  sock.on('error', (e) => { clearTimeout(t); if (!res.writableEnded) { res.writeHead(503); res.end(`pool socket error: ${e.message}`); } });
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
  if (req.method === 'GET' && path === '/metrics') return handleMetrics(req, res);
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
