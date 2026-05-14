#!/usr/bin/env node
// Pool manager — long-lived daemon that forks N bridge-worker children,
// tracks state, routes requests, pings idle workers, auto-respawns dead
// ones. Exposes Unix socket /tmp/ratlc-pool.sock to api-server and
// ratlc-ctl. See ./IPC.md for the wire format.

import { fork } from 'node:child_process';
import net from 'node:net';
import fs from 'node:fs';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { defaultTranslateModeTools } from './tool-translator.mjs';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const POOL_SIZE = parseInt(process.env.POOL_SIZE || '2', 10);
const POOL_SOCK = process.env.POOL_SOCK || '/tmp/ratlc-pool.sock';
const POOL_MODEL = process.env.POOL_MODEL || 'claude-opus-4-7-thinking-max-fast';
const IDLE_PING_MS = parseInt(process.env.IDLE_PING_MS || '1200000', 10);  // 20 min
const PING_TIMEOUT_MS = parseInt(process.env.PING_TIMEOUT_MS || '45000', 10);
const STAGGER_OPEN_MS = parseInt(process.env.STAGGER_OPEN_MS || '5000', 10); // wait between worker spawns
const POOL_TOOL_MODE = (process.env.POOL_TOOL_MODE || 'contract').toLowerCase();
// Protocol the bridge workers use to talk to Cursor's backend.
//   h2 (default) — HTTP/2 BiDi via /agent.v1.AgentService/Run
//   h1           — HTTP/1.1 via BidiAppend + RunSSE pair (rate-limit hypothesis)
// Propagated to each forked worker as BRIDGE_PROTOCOL. See
// scaffolding/pool/RUNSSE.md for the protocol notes.
const POOL_BRIDGE_PROTOCOL = (process.env.POOL_BRIDGE_PROTOCOL || 'h2').toLowerCase();
if (!['h1', 'h2'].includes(POOL_BRIDGE_PROTOCOL)) {
  console.error(`invalid POOL_BRIDGE_PROTOCOL=${POOL_BRIDGE_PROTOCOL} (must be h1|h2)`);
  process.exit(1);
}
// POOL_CONTEXT_MODE — see api-server.mjs / bridge-worker.mjs for semantics.
// Propagated to every forked worker so the priming prompt matches what
// api-server actually sends each turn.
const POOL_CONTEXT_MODE = (process.env.POOL_CONTEXT_MODE || 'last').toLowerCase();
if (!['full', 'last'].includes(POOL_CONTEXT_MODE)) {
  console.error(`invalid POOL_CONTEXT_MODE=${POOL_CONTEXT_MODE} (must be full|last)`);
  process.exit(1);
}
// POOL_REINJECT_THINKING — opt-in symmetry with CURSOR_REINJECT_THINKING
// in server.js. Captures thinking_delta events from the bridge per
// convKey and re-injects them as `<thinking>...</thinking>` text on the
// next turn. Default OFF (no behavior change). The actual buffering
// lives in api-server.mjs; pool-manager just forwards the env var to
// each spawned worker (so bridge-worker doesn't have to read it on
// its own — the env is already in the process tree).
const POOL_REINJECT_THINKING = process.env.POOL_REINJECT_THINKING === '1';
// How many channels are allowed to run the retry lottery concurrently.
// Default 1 (sequential, safe against rate-limit). Set higher to bring the
// pool up faster at risk of tripping ERROR_PRO_USER_RATE_LIMIT_EXCEEDED.
// 2 is usually fine on a fresh quota; 3+ regularly trips.
const POOL_CONCURRENT_OPENS = Math.max(1, parseInt(process.env.POOL_CONCURRENT_OPENS || '1', 10));
const WORKER_SCRIPT = path.join(__dirname, 'bridge-worker.mjs');

if (!['contract', 'translate'].includes(POOL_TOOL_MODE)) {
  console.error(`invalid POOL_TOOL_MODE=${POOL_TOOL_MODE} (must be contract|translate)`);
  process.exit(1);
}

const log = (...args) => console.log(`[${new Date().toISOString().slice(11, 23)}] [pool]`, ...args);

// ── Worker (channel) record ──────────────────────────────────────────────
let nextChannelSeq = 0;
const channels = new Map();   // channelId -> { id, proc, state, ..., currentRequestId, anthropicByExecId }

// Pending requests waiting for a ready channel
const requestQueue = [];      // [{ requestId, action, payload, client }]

// Map anthropic_tool_use_id -> { channelId, execId }
// (set when manager forwards a tool_use to a client; consulted on send_tool_result)
const toolUseIndex = new Map();

// Map requestId -> client socket (so we know where to stream events back)
const requestClient = new Map();

// Empirical finding: Cursor only reads tools from requestContextResult ONCE
// per stream (at the first requestContextArgs cycle, near stream open). So
// tools must be set at worker open and CANNOT be updated mid-stream.
//
// Two modes:
//   contract  — first client request's tools become the pool's tool list;
//               subsequent mismatched requests trigger a full recycle.
//               Workers stay 'spawning' until first request bootstraps.
//   translate — workers open immediately with a tiny placeholder tool list
//               that triggers Cursor to inject its full default toolset
//               (Shell/Read/Write/Grep/Glob/StrReplace/...). Caller-side
//               tool names are mapped via tool-translator.mjs. No recycle.
let poolTools = null;
let poolSystem = null;
let toolsSignature = '';

if (POOL_TOOL_MODE === 'translate') {
  // Pre-set the contract so spawn() opens immediately with translate-mode tools.
  poolTools = defaultTranslateModeTools();
  poolSystem = '';
  toolsSignature = 'translate-mode-static';
}

// Signature mode controls how strictly we compare tool lists between
// requests to decide whether to recycle the pool.
//   'name'   — tool name set must match. Schema drift is absorbed
//              silently (model uses the pool's open-time schema).
//   'schema' — name + JSON-serialized input_schema must match.
//              Any field-level change triggers a recycle.
// Default 'name' is much more forgiving for claude-code, which can
// add/remove optional schema fields between versions without changing
// tool surface.
const POOL_SIG_MODE = (process.env.POOL_SIG_MODE || 'name').toLowerCase();

function signatureOf(tools) {
  if (!Array.isArray(tools)) return '';
  const filtered = tools.filter((t) => t && t.name);
  if (POOL_SIG_MODE === 'schema') {
    return filtered
      .map((t) => `${t.name}:${JSON.stringify(t.input_schema || t.jsonSchema || {})}`)
      .sort().join('|');
  }
  // name-only (default)
  return filtered.map((t) => t.name).sort().join(',');
}

function setPoolContract(system, tools) {
  poolSystem = system || '';
  poolTools = tools || [];
  toolsSignature = signatureOf(tools);
}

function poolNeedsReopen(tools) {
  return signatureOf(tools) !== toolsSignature;
}

function reopenAllChannels() {
  log(`recycling all channels (new tools sig=[${toolsSignature.slice(0, 80)}])`);
  for (const ch of channels.values()) {
    try { ch.proc.send({ type: 'shutdown' }); } catch { /* ignore */ }
  }
  // exit handlers will respawn through maybeSpawnNext (sequentially).
}

// ── Channel management ──────────────────────────────────────────────────
function spawnChannel() {
  const channelId = `ch-${nextChannelSeq++}`;
  const env = {
    ...process.env,
    RATLC_CHANNEL_ID: channelId,
    RATLC_MODEL: POOL_MODEL,
    // Propagate the bridge transport choice to each worker.
    BRIDGE_PROTOCOL: POOL_BRIDGE_PROTOCOL,
    // Propagate the context-rendering mode so the worker's priming prompt
    // matches the shape of the prompts api-server will deliver each turn.
    POOL_CONTEXT_MODE,
    // Propagate the reinject-thinking flag. Workers don't read it
    // themselves (the buffer lives in api-server), but forwarding via
    // env keeps the whole process tree on a consistent setting and
    // makes status snapshots accurate.
    POOL_REINJECT_THINKING: POOL_REINJECT_THINKING ? '1' : '0',
    // In translate mode, the worker tells startConversation to passthrough
    // native Cursor tools (Shell/Read/Write/Grep/Fetch) as MCP-shape
    // tool_use events with Anthropic names (Bash/Read/Write/Grep/WebFetch).
    RATLC_PASSTHROUGH_NATIVE: POOL_TOOL_MODE === 'translate' ? '1' : '0',
    // In translate mode, native tool calls round-trip through the API
    // client (claude-code), which can take seconds-to-minutes. The
    // default stall watchdog assumes Cursor will continue emitting
    // frames; here it has to wait on us. Bump the threshold to 30 min
    // so the watchdog doesn't trip while we wait on the client.
    ...(POOL_TOOL_MODE === 'translate' ? {
      CURSOR_STALL_TIMEOUT_MS_WITH_CONTENT: '1800000',
      CURSOR_STALL_TIMEOUT_MS: '600000',
      CURSOR_LOG_NATIVE_EXEC: '1',
    } : {}),
  };
  const proc = fork(WORKER_SCRIPT, [], { env, silent: false });
  const ch = {
    id: channelId,
    proc,
    pid: proc.pid,
    state: 'spawning',
    openAttempts: 0,
    openedAt: 0,
    lastActivityAt: Date.now(),
    spawnedAt: Date.now(),
    currentRequestId: null,
    pendingExecId: null,        // execId of an in-flight non-yield tool_use
    pendingAnthropicId: null,
    roundsServed: 0,
    error: null,
  };
  channels.set(channelId, ch);

  proc.on('message', (msg) => handleWorkerMessage(ch, msg));
  proc.on('exit', (code, signal) => handleWorkerExit(ch, code, signal));
  proc.on('error', (err) => {
    log(`channel ${channelId} proc error:`, err.message);
  });

  // If we already have a pool contract, open with those tools now. Otherwise
  // the worker sits in `spawning` until the first client request bootstraps
  // the contract.
  if (poolTools !== null) {
    proc.send({ type: 'open', model: POOL_MODEL, tools: poolTools, system: poolSystem });
  }
  log(`spawned ${channelId} (pid=${proc.pid}); pool size=${channels.size}`);
  return ch;
}

function handleWorkerMessage(ch, msg) {
  // Clear ping timer FIRST — ping responses don't have a requestClient mapping,
  // so they would skip forwardToClient and the timer would leak.
  if (msg.type === 'yield' && ch._pingTimer && ch.currentRequestId &&
      String(ch.currentRequestId).startsWith('ping-')) {
    clearTimeout(ch._pingTimer);
    ch._pingTimer = null;
    log(`ping ${ch.currentRequestId} OK on ${ch.id} (idle reset)`);
    ch.currentRequestId = null;
    ch.lastActivityAt = Date.now();
    ch.state = 'ready';
    setImmediate(drainQueue);
    return;
  }
  if (msg.type === 'error' && ch._pingTimer && ch.currentRequestId &&
      String(ch.currentRequestId).startsWith('ping-')) {
    clearTimeout(ch._pingTimer);
    ch._pingTimer = null;
    log(`ping ${ch.currentRequestId} FAILED on ${ch.id}: ${msg.message}`);
    try { ch.proc.kill('SIGTERM'); } catch { /* ignore */ }
    return;
  }

  switch (msg.type) {
    case 'state':
      ch.state = msg.state;
      ch.openAttempts = msg.openAttempts || ch.openAttempts;
      ch.openedAt = msg.openedAt || ch.openedAt;
      ch.lastActivityAt = msg.lastActivityAt || ch.lastActivityAt;
      ch.error = msg.error || null;
      if (msg.state === 'ready') {
        log(`channel ${ch.id} READY after ${ch.openAttempts} attempts (${((Date.now() - ch.spawnedAt) / 1000).toFixed(1)}s)`);
        setImmediate(drainQueue);
        // A channel just became ready — see if we should spawn the next one.
        setImmediate(maybeSpawnNext);
      } else if (msg.state === 'dead') {
        // Worker reported dead but hasn't exited yet — spawn replacement on exit hook.
      }
      break;

    case 'heartbeat':
      ch.lastActivityAt = msg.lastActivityAt || ch.lastActivityAt;
      break;

    case 'text_delta':
    case 'thinking_delta':
    case 'tool_use':
    case 'yield':
    case 'step_completed':
    case 'error':
      forwardToClient(ch, msg);
      break;

    case 'log':
      log(`[${ch.id}] ${msg.level}: ${msg.message}`);
      break;
  }
}

function handleWorkerExit(ch, code, signal) {
  log(`channel ${ch.id} exited code=${code} signal=${signal} state=${ch.state}`);
  channels.delete(ch.id);
  // Fail any in-flight request bound to this channel.
  if (ch.currentRequestId) {
    const client = requestClient.get(ch.currentRequestId);
    if (client) {
      writeToClient(client, {
        type: 'error',
        requestId: ch.currentRequestId,
        message: `channel ${ch.id} died (code=${code} signal=${signal})`,
      });
    }
    requestClient.delete(ch.currentRequestId);
  }
  // Try to respawn — but only when there's no other channel already in the
  // middle of opening (to avoid concurrent lotteries hammering the account
  // rate limit).
  setTimeout(maybeSpawnNext, 500);
}

// Concurrent-open guard: at most POOL_CONCURRENT_OPENS channels in
// spawning/opening state at once. Default 1 (rate-limit safe).
function countOpening() {
  let n = 0;
  for (const ch of channels.values()) {
    if (ch.state === 'spawning' || ch.state === 'opening') n++;
  }
  return n;
}

function maybeSpawnNext() {
  while (channels.size < currentTargetSize && countOpening() < POOL_CONCURRENT_OPENS) {
    spawnChannel();
  }
}

// Target pool size, mutable via ramp_up / ramp_down
let currentTargetSize = POOL_SIZE;

function forwardToClient(ch, msg) {
  const reqId = msg.requestId;
  if (!reqId) return;
  const client = requestClient.get(reqId);
  if (!client) return;

  if (msg.type === 'tool_use') {
    // Manager mints the anthropic_id and remembers (channelId, execId).
    const anthropic_id = 'toolu_' + randomUUID().replace(/-/g, '').slice(0, 16);
    toolUseIndex.set(anthropic_id, { channelId: ch.id, execId: msg.execId });
    ch.pendingExecId = msg.execId;
    ch.pendingAnthropicId = anthropic_id;
    writeToClient(client, {
      type: 'tool_use',
      requestId: reqId,
      anthropic_id,
      name: msg.name,
      args: msg.args,
    });
    // Channel stays busy until tool_result feeds back.
    return;
  }

  if (msg.type === 'yield') {
    ch.currentRequestId = null;
    ch.roundsServed = (ch.roundsServed || 0) + 1;
    requestClient.delete(reqId);
    writeToClient(client, { type: 'yield', requestId: reqId });
    setImmediate(drainQueue);
    return;
  }

  if (msg.type === 'error') {
    requestClient.delete(reqId);
    ch.currentRequestId = null;
    writeToClient(client, { type: 'error', requestId: reqId, message: msg.message });
    return;
  }

  // text_delta / thinking_delta / step_completed — pass through as-is.
  // thinking_delta carries the model's emitted reasoning text; api-server
  // buffers it per-convKey for proxy-side re-injection
  // (POOL_REINJECT_THINKING). The data is NOT relayed downstream to
  // claude-code — see api-server.mjs for the policy.
  writeToClient(client, msg);
}

// ── Routing ─────────────────────────────────────────────────────────────
function pickReadyChannel() {
  // Pick least-recently-used ready channel
  let best = null;
  for (const ch of channels.values()) {
    if (ch.state !== 'ready') continue;
    if (!best || ch.lastActivityAt < best.lastActivityAt) best = ch;
  }
  return best;
}

function drainQueue() {
  while (requestQueue.length > 0) {
    const job = requestQueue[0];
    const ch = pickReadyChannel();
    if (!ch) return;
    requestQueue.shift();
    routeRequest(job, ch);
  }
}

function routeRequest(job, ch) {
  ch.currentRequestId = job.requestId;
  ch.state = 'busy';
  ch.lastActivityAt = Date.now();
  requestClient.set(job.requestId, job.client);
  if (job.action === 'send_user_message') {
    ch.proc.send({
      type: 'send_user_message',
      requestId: job.requestId,
      text: job.payload.text,
    });
  } else if (job.action === 'send_tool_result') {
    ch.proc.send({
      type: 'send_tool_result',
      requestId: job.requestId,
      execId: ch.pendingExecId,
      content: job.payload.content,
    });
  } else {
    writeToClient(job.client, { type: 'error', requestId: job.requestId, message: 'unknown action: ' + job.action });
    return;
  }
}

// ── Idle ping ────────────────────────────────────────────────────────────
setInterval(() => {
  const now = Date.now();
  for (const ch of channels.values()) {
    if (ch.state !== 'ready') continue;
    if (now - ch.lastActivityAt < IDLE_PING_MS) continue;
    const pingReqId = 'ping-' + randomUUID().slice(0, 8);
    log(`pinging idle ${ch.id} (idle for ${Math.floor((now - ch.lastActivityAt) / 1000)}s)`);
    ch.currentRequestId = pingReqId;
    ch.state = 'busy';
    ch.lastActivityAt = now;
    // Set up a timeout to detect ping failure
    const pingTimer = setTimeout(() => {
      log(`ping timeout on ${ch.id}; killing for respawn`);
      try { ch.proc.kill('SIGTERM'); } catch { /* ignore */ }
    }, PING_TIMEOUT_MS);
    // Store the timer so we can clear it on yield
    ch._pingTimer = pingTimer;
    ch.proc.send({ type: 'ping', requestId: pingReqId });
  }
}, 30_000);

// (Ping timer clearing is handled at the top of handleWorkerMessage.)

// ── Client socket plumbing (Unix socket) ─────────────────────────────────
function writeToClient(client, obj) {
  if (!client || client.destroyed) return;
  try {
    client.write(JSON.stringify(obj) + '\n');
  } catch (e) {
    log('writeToClient failed:', e.message);
  }
}

function handleClientMessage(client, msg) {
  if (msg.type === 'request') {
    const { requestId, action, text, content, anthropic_tool_use_id, system, tools, results } = msg;

    if (action === 'send_user_message') {
      // Tool list bootstrap / recycle logic only runs in CONTRACT mode.
      // In TRANSLATE mode the pool always opens with a fixed placeholder
      // tool list (which triggers Cursor's default toolset to be injected
      // into the model's prompt); caller tools are translated on the wire.
      if (POOL_TOOL_MODE === 'contract') {
        const incomingTools = tools || [];
        // claude-code (and similar Anthropic SDK clients) issues parallel
        // requests with mixed tool surfaces: some POSTs send tools=[] (e.g.
        // token-count probes / system-reminder pings), others send the
        // actual tool list. Empty-tools requests must NEVER bootstrap or
        // recycle the contract — otherwise an empty probe arriving before
        // the real request sets the contract to [], then the next real
        // request triggers a recycle.
        const isEmptyToolsProbe = incomingTools.length === 0;

        if (!isEmptyToolsProbe) {
          if (poolTools === null) {
            // First non-empty request bootstraps the contract.
            setPoolContract(system, incomingTools);
            log(`pool contract set: tools=${incomingTools.length} (${(incomingTools.map(t=>t.name).join(',')).slice(0, 80)})`);
            for (const ch of channels.values()) {
              if (ch.state === 'spawning') {
                ch.proc.send({ type: 'open', model: POOL_MODEL, tools: poolTools, system: poolSystem });
              }
            }
          } else if (poolNeedsReopen(incomingTools)) {
            log(`tools mismatch — recycling pool (have=[${toolsSignature.slice(0, 60)}] want=[${signatureOf(incomingTools).slice(0, 60)}])`);
            setPoolContract(system, incomingTools);
            reopenAllChannels();
            writeToClient(client, {
              type: 'error', requestId,
              message: 'pool recycling for new tools contract — retry in 30-180s',
            });
            return;
          }
        }
        // Empty-tools probes always fall through and route to whatever
        // the pool has. If no channel is ready yet (we're still spawning
        // because no real contract has come in), the request queues
        // naturally via the routeRequest path.
      }
      // TRANSLATE mode: pool is pre-warmed with placeholder tools, nothing
      // to bootstrap or recycle.

      const ch = pickReadyChannel();
      const payload = { text };
      if (ch) {
        routeRequest({ requestId, action, payload, client }, ch);
      } else {
        log(`no ready channel — queuing requestId=${requestId} (queue depth ${requestQueue.length + 1})`);
        requestQueue.push({ requestId, action, payload, client });
      }
      return;
    }

    if (action === 'send_tool_result') {
      // Legacy single-result action. Kept for backwards-compat. The
      // parallel-tools fix prefers `send_tool_results` (plural) so a single
      // POST carrying N parallel tool_results goes to the same channel in
      // one IPC batch.
      const entry = toolUseIndex.get(anthropic_tool_use_id);
      log(`route send_tool_result requestId=${requestId} anthropic_tool_use_id=${anthropic_tool_use_id} found=${!!entry} indexSize=${toolUseIndex.size}`);
      if (!entry) {
        log(`  ❌ unknown anthropic_tool_use_id — known ids: [${[...toolUseIndex.keys()].slice(0, 5).join(', ')}${toolUseIndex.size > 5 ? '…' : ''}]`);
        writeToClient(client, { type: 'error', requestId, message: `unknown anthropic_tool_use_id: ${anthropic_tool_use_id}` });
        return;
      }
      toolUseIndex.delete(anthropic_tool_use_id);
      const ch = channels.get(entry.channelId);
      if (!ch || ch.state === 'dead') {
        log(`  ❌ channel ${entry.channelId} no longer alive (state=${ch?.state})`);
        writeToClient(client, { type: 'error', requestId, message: `channel ${entry.channelId} no longer alive` });
        return;
      }
      log(`  ✅ routing to ${entry.channelId} execId=${entry.execId} (state was ${ch.state})`);
      ch.currentRequestId = requestId;
      ch.state = 'busy';
      ch.lastActivityAt = Date.now();
      requestClient.set(requestId, client);
      ch.proc.send({ type: 'send_tool_result', requestId, execId: entry.execId, content });
      return;
    }

    if (action === 'send_tool_results') {
      // Parallel-tools fix: a single POST may carry N tool_result blocks
      // (one per parallel tool_use the model emitted in its previous
      // assistant turn). Resolve every anthropic_tool_use_id against the
      // toolUseIndex; ALL must resolve to the SAME channel (they will, by
      // construction — they were emitted by one channel in one assistant
      // turn). Defensively error out on mismatch.
      const rs = Array.isArray(results) ? results : [];
      if (rs.length === 0) {
        writeToClient(client, { type: 'error', requestId, message: 'send_tool_results: empty results array' });
        return;
      }
      log(`route send_tool_results requestId=${requestId} count=${rs.length} ids=[${rs.map(r => r.anthropic_tool_use_id).join(', ')}] indexSize=${toolUseIndex.size}`);
      const resolved = [];
      let channelId = null;
      for (const r of rs) {
        const entry = toolUseIndex.get(r.anthropic_tool_use_id);
        if (!entry) {
          log(`  ❌ unknown anthropic_tool_use_id=${r.anthropic_tool_use_id} — known ids: [${[...toolUseIndex.keys()].slice(0, 5).join(', ')}${toolUseIndex.size > 5 ? '…' : ''}]`);
          writeToClient(client, { type: 'error', requestId, message: `unknown anthropic_tool_use_id: ${r.anthropic_tool_use_id}` });
          return;
        }
        if (channelId === null) channelId = entry.channelId;
        else if (entry.channelId !== channelId) {
          log(`  ❌ tool_use_ids span multiple channels: ${channelId} vs ${entry.channelId} (impossible by construction)`);
          writeToClient(client, {
            type: 'error', requestId,
            message: `tool_use_ids span multiple channels (${channelId} vs ${entry.channelId}) — possibly stale conversation`,
          });
          return;
        }
        resolved.push({ anthropic_tool_use_id: r.anthropic_tool_use_id, execId: entry.execId, content: r.content });
      }
      const ch = channels.get(channelId);
      if (!ch || ch.state === 'dead') {
        log(`  ❌ channel ${channelId} no longer alive (state=${ch?.state})`);
        writeToClient(client, { type: 'error', requestId, message: `channel ${channelId} no longer alive` });
        return;
      }
      // Per-id delete from the index (each id resolves only once).
      for (const r of resolved) toolUseIndex.delete(r.anthropic_tool_use_id);
      log(`  ✅ routing ${resolved.length} result(s) to ${channelId} execIds=[${resolved.map(r => r.execId).join(', ')}] (state was ${ch.state})`);
      ch.currentRequestId = requestId;
      ch.state = 'busy';
      ch.lastActivityAt = Date.now();
      requestClient.set(requestId, client);
      ch.proc.send({
        type: 'send_tool_results', requestId,
        results: resolved.map((r) => ({ execId: r.execId, content: r.content })),
      });
      return;
    }

    writeToClient(client, { type: 'error', requestId, message: 'unknown action: ' + action });
    return;
  }

  if (msg.type === 'status') {
    writeToClient(client, statusSnapshot());
    return;
  }

  if (msg.type === 'ramp_up') {
    const n = Math.max(1, parseInt(msg.count || 1, 10));
    currentTargetSize += n;
    log(`ramp_up by ${n} → target=${currentTargetSize}`);
    // Sequential — just nudge the spawn loop, which respects "one opening at a time".
    setImmediate(maybeSpawnNext);
    writeToClient(client, { type: 'ack', message: `ramping up ${n} (target=${currentTargetSize}, sequential)` });
    return;
  }

  if (msg.type === 'ramp_down') {
    const n = Math.max(1, parseInt(msg.count || 1, 10));
    currentTargetSize = Math.max(0, currentTargetSize - n);
    log(`ramp_down by ${n} → target=${currentTargetSize}`);
    // Find idle ready channels first; kill busy ones last.
    const candidates = Array.from(channels.values()).sort((a, b) => {
      const aReady = a.state === 'ready' ? 0 : 1;
      const bReady = b.state === 'ready' ? 0 : 1;
      return aReady - bReady;
    }).slice(0, n);
    for (const ch of candidates) {
      try { ch.proc.send({ type: 'shutdown' }); } catch { /* ignore */ }
    }
    writeToClient(client, { type: 'ack', message: `ramping down ${candidates.length} (target=${currentTargetSize})` });
    return;
  }

  if (msg.type === 'restart_channel') {
    const ch = channels.get(msg.channelId);
    if (!ch) {
      writeToClient(client, { type: 'error', message: `channel not found: ${msg.channelId}` });
      return;
    }
    try { ch.proc.kill('SIGTERM'); } catch { /* ignore */ }
    writeToClient(client, { type: 'ack', message: `killing ${msg.channelId} for respawn` });
    return;
  }

  if (msg.type === 'shutdown') {
    log('shutdown requested');
    for (const ch of channels.values()) {
      try { ch.proc.send({ type: 'shutdown' }); } catch { /* ignore */ }
    }
    setTimeout(() => process.exit(0), 1500);
    writeToClient(client, { type: 'ack', message: 'shutting down' });
    return;
  }

  writeToClient(client, { type: 'error', message: 'unknown command: ' + msg.type });
}

function statusSnapshot() {
  const list = [];
  let readyCount = 0, busyCount = 0, openingCount = 0, deadCount = 0;
  const now = Date.now();
  for (const ch of channels.values()) {
    list.push({
      id: ch.id,
      pid: ch.pid,
      state: ch.state,
      openAttempts: ch.openAttempts,
      openedAt: ch.openedAt,
      openedAgoMs: ch.openedAt ? now - ch.openedAt : null,
      lastActivityAt: ch.lastActivityAt,
      idleMs: ch.lastActivityAt ? now - ch.lastActivityAt : null,
      roundsServed: ch.roundsServed,
      currentRequestId: ch.currentRequestId,
      error: ch.error,
    });
    if (ch.state === 'ready') readyCount++;
    else if (ch.state === 'busy') busyCount++;
    else if (ch.state === 'opening' || ch.state === 'spawning') openingCount++;
    else if (ch.state === 'dead') deadCount++;
  }
  list.sort((a, b) => a.id.localeCompare(b.id));
  return {
    type: 'status',
    pool: {
      configuredSize: currentTargetSize,
      actualSize: channels.size,
      channels: list,
      readyCount, busyCount, openingCount, deadCount,
      pendingRequests: requestQueue.length,
      toolUseIndex: toolUseIndex.size,
    },
    config: {
      model: POOL_MODEL,
      toolMode: POOL_TOOL_MODE,
      bridgeProtocol: POOL_BRIDGE_PROTOCOL,
      contextMode: POOL_CONTEXT_MODE,
      reinjectThinking: POOL_REINJECT_THINKING ? 1 : 0,
      concurrentOpens: POOL_CONCURRENT_OPENS,
      idlePingMs: IDLE_PING_MS,
      pingTimeoutMs: PING_TIMEOUT_MS,
      poolToolsContractCount: poolTools ? poolTools.length : null,
      poolToolsSignature: toolsSignature.slice(0, 80),
    },
  };
}

// ── Listen on Unix socket ────────────────────────────────────────────────
try { fs.unlinkSync(POOL_SOCK); } catch { /* ignore */ }
const server = net.createServer((socket) => {
  let buf = '';
  socket.on('data', (chunk) => {
    buf += chunk.toString('utf8');
    let idx;
    while ((idx = buf.indexOf('\n')) !== -1) {
      const line = buf.slice(0, idx);
      buf = buf.slice(idx + 1);
      if (!line.trim()) continue;
      try {
        const msg = JSON.parse(line);
        handleClientMessage(socket, msg);
      } catch (e) {
        writeToClient(socket, { type: 'error', message: 'bad json: ' + e.message });
      }
    }
  });
  socket.on('error', () => {});
  socket.on('close', () => {
    // Clean up any requests bound to this client
    for (const [reqId, c] of requestClient.entries()) {
      if (c === socket) requestClient.delete(reqId);
    }
  });
});
server.listen(POOL_SOCK, () => {
  log(`listening on ${POOL_SOCK}, target size=${currentTargetSize}, model=${POOL_MODEL}, protocol=${POOL_BRIDGE_PROTOCOL}, contextMode=${POOL_CONTEXT_MODE}, reinjectThinking=${POOL_REINJECT_THINKING ? 1 : 0}`);
});

// ── Spawn initial pool, honoring POOL_CONCURRENT_OPENS ───────────────────
log(`bringing up initial pool target=${currentTargetSize}, up to ${POOL_CONCURRENT_OPENS} concurrent opens`);
maybeSpawnNext();

// ── Shutdown ─────────────────────────────────────────────────────────────
function shutdown(signal) {
  log(`received ${signal}, shutting down`);
  for (const ch of channels.values()) {
    try { ch.proc.send({ type: 'shutdown' }); } catch { /* ignore */ }
    setTimeout(() => { try { ch.proc.kill('SIGKILL'); } catch { /* ignore */ } }, 2000);
  }
  try { server.close(); } catch { /* ignore */ }
  try { fs.unlinkSync(POOL_SOCK); } catch { /* ignore */ }
  setTimeout(() => process.exit(0), 3000);
}
process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));
