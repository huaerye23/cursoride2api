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
// Map<execId, info> — tracks every non-yield mcp call we're currently holding.
// In the parallel-tools case the model emits N tool_uses in one assistant
// turn (e.g. Bash + Read back-to-back); each gets its own execId. We must
// retain all of them until tool_results arrive, otherwise the 2nd
// onMcpCall would clobber the 1st (the original bug). The map is cleared
// per-execId in the send_tool_result handler after each dispatch.
const pendingMcpInfo = new Map();
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
      // During the priming pass the model emits thinking too, but no
      // currentRequestId exists yet — drop those frames silently. The
      // live-callbacks attachment below is where forwarding kicks in.
      onThinkingDelta: () => {},
      onStepCompleted: () => {},
      onTurnEnded: () => { if (!yieldInfo) resolve({ kind: 'no_yield', textBuf }); },
      onError: (err) => {
        const msg = String(err?.message || err || '');
        if (/unpaid invoice|cursor\.com\/dashboard/i.test(msg)) resolve({ kind: 'unpaid', msg });
        else if (/RATE_LIMIT_EXCEEDED|too many requests/i.test(msg)) resolve({ kind: 'rate_limit_hard', msg });
        else if (/rate limit/i.test(msg)) resolve({ kind: 'rate_limit_soft', msg });
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
  // Empirically (confirmed on Mac), every Cursor refusal during open —
  // unpaid_invoice, soft "Please wait" rate-limit, hard RATE_LIMIT_EXCEEDED,
  // no_yield — is short-lived. Backing off makes things worse, not better:
  // the channel sits idle while the gate has long since cleared. So we just
  // retry every OPEN_RETRY_MS (default 300 ms) regardless of error kind,
  // with a ±25% jitter so 5 concurrent channels don't burst-synchronize.
  for (let attempt = 1; attempt <= OPEN_RETRY_MAX; attempt++) {
    openAttempts = attempt;
    // Push state on every attempt so the TUI's ATTEMPTS column tracks retry
    // activity live. The cost is one ~120-byte IPC message per retry; with
    // POOL_CONCURRENT_OPENS=5 worst-case ~15 msgs/sec, well below anything
    // the pool socket cares about.
    setState('opening');
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
    if (result.kind === 'other_error') {
      setState('dead', { error: result.msg || result.kind });
      process.exit(1);
    }
    // unpaid / rate_limit_soft / rate_limit_hard / no_yield → just retry.
    await sleep(jitter(OPEN_RETRY_MS));
    continue;
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
    // Forward thinking deltas via IPC so pool-manager + api-server can
    // buffer them for proxy-side re-injection on subsequent turns
    // (mirrors src/thinking-history.js + CURSOR_REINJECT_THINKING in
    // server.js). Capture is unconditional at the wire — the api-server
    // decides whether to keep the data based on POOL_REINJECT_THINKING.
    // We deliberately do NOT emit thinking to the client SSE here: the
    // signed thinking-block round-trip is not portable across providers,
    // and proxy-internal text-form re-injection is the only useful path
    // (see DEVLOG: "Conversation key collision" / `_emitThinkingBlocks=false`).
    onThinkingDelta: (text) => {
      if (currentRequestId == null) return;
      if (!text) return;
      lastActivityAt = Date.now();
      send({ type: 'thinking_delta', channelId: CHANNEL_ID, requestId: currentRequestId, text });
    },
    onMcpCall: (info) => {
      lastActivityAt = Date.now();
      if (info.toolName === YIELD_TOOL_NAME) {
        // The model called bajie_yield — the turn is over. Any pending
        // non-yield tool_uses being held should be cleared (they were
        // resolved earlier in this turn or never resolved cleanly; either
        // way they're done).
        pendingYield = info;
        pendingMcpInfo.clear();
        const finishedReqId = currentRequestId;
        currentRequestId = null;
        setState('ready');
        send({ type: 'yield', channelId: CHANNEL_ID, requestId: finishedReqId });
      } else {
        // Parallel-tools fix: store every non-yield mcp call in the map
        // keyed by execId — DO NOT overwrite. In the parallel case the
        // model emits multiple onMcpCall events back-to-back (e.g.
        // Bash + Read); each must be retrievable when its tool_result
        // arrives via send_tool_result. Previously this was a single
        // variable that the 2nd call clobbered, which was the bug heart.
        pendingMcpInfo.set(info.execId, info);
        // The model has emitted a tool_use; it's no longer waiting in a
        // yield, so clear pendingYield. (A yield-result followup would
        // arrive via send_user_message, not via this path.)
        pendingYield = null;
        send({
          type: 'tool_use',
          channelId: CHANNEL_ID,
          requestId: currentRequestId,
          execId: info.execId,
          name: info.toolName,
          args: info.args,
        });
        // We stay busy until tool_result(s) feed in.
      }
    },
    onStepCompleted: () => {
      // Forward step boundaries so api-server can finalize a tool_use turn
      // promptly (no 250 ms debounce wait) once the model has emitted all
      // its parallel tool_uses for this step and is now paused waiting
      // for results.
      if (currentRequestId != null) {
        send({ type: 'step_completed', channelId: CHANNEL_ID, requestId: currentRequestId });
      }
    },
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
  console.log(`[bridge-worker ch=${CHANNEL_ID}] IPC type=${msg.type} requestId=${msg.requestId || ''} state=${currentState} pendingYield=${!!pendingYield} pendingMcp=[${[...pendingMcpInfo.keys()].map(k => k.slice(0, 8)).join(',') || 'none'}]`);
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
    // Singular form (legacy / kept for compat). Looks up the pending info
    // by execId in the map. The map may hold multiple entries when the
    // model fired parallel tool_uses; we only consume the matching one
    // and leave the others pending for their own send_tool_result.
    const info = pendingMcpInfo.get(msg.execId);
    if (!info) {
      send({
        type: 'error', channelId: CHANNEL_ID, requestId: msg.requestId,
        message: `no matching pending tool_use (have=[${[...pendingMcpInfo.keys()].join(',') || 'none'}] want=${msg.execId})`,
      });
      return;
    }
    currentRequestId = msg.requestId;
    setState('busy');
    lastActivityAt = Date.now();
    bridge.sendToolResult(info.id, info.execId, msg.content || '');
    pendingMcpInfo.delete(msg.execId);
    return;
  }

  if (msg.type === 'send_tool_results') {
    // Plural form (parallel-tools fix): dispatch N tool_results in one IPC
    // batch. Each result targets a distinct execId in the pendingMcpInfo
    // map. The underlying transport (cursor-agent-h1 / cursor-agent.js)
    // supports per-execId result dispatch via _nativeExecKinds, so we just
    // call bridge.sendToolResult(id, execId, content) N times.
    const results = Array.isArray(msg.results) ? msg.results : [];
    if (results.length === 0) {
      send({ type: 'error', channelId: CHANNEL_ID, requestId: msg.requestId,
        message: 'send_tool_results: empty results array' });
      return;
    }
    // Validate every execId resolves before we dispatch anything — partial
    // dispatch on bad input would leave the model waiting on results that
    // never come.
    const dispatchPlan = [];
    for (const r of results) {
      const info = pendingMcpInfo.get(r.execId);
      if (!info) {
        send({
          type: 'error', channelId: CHANNEL_ID, requestId: msg.requestId,
          message: `no matching pending tool_use for execId=${r.execId} (have=[${[...pendingMcpInfo.keys()].join(',') || 'none'}])`,
        });
        return;
      }
      dispatchPlan.push({ info, content: r.content || '' });
    }
    currentRequestId = msg.requestId;
    setState('busy');
    lastActivityAt = Date.now();
    for (const { info, content } of dispatchPlan) {
      try {
        bridge.sendToolResult(info.id, info.execId, content);
        pendingMcpInfo.delete(info.execId);
      } catch (e) {
        send({
          type: 'error', channelId: CHANNEL_ID, requestId: msg.requestId,
          message: `bridge.sendToolResult threw for execId=${info.execId}: ${e.message}`,
        });
        return;
      }
    }
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
// jitter — ±25% random multiplier. Used on every retry backoff so 5
// concurrently-opening channels don't synchronize their requests into
// bursts that trigger Cursor's hard per-account rate-limit.
const jitter = (ms) => Math.max(1, Math.floor(ms * (0.75 + Math.random() * 0.5)));

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
