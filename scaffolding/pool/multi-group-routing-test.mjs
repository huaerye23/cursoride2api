#!/usr/bin/env node
// multi-group-routing-test.mjs
//
// Tests the pool-manager's MULTI-GROUP ROUTING LOGIC end-to-end via the
// Unix socket protocol, WITHOUT opening real Cursor channels (which cost
// API quota). We stub the channel pipeline by injecting fake channel
// records straight into the pool-manager state through a tiny test
// harness child process that wires a mock-mode flag.
//
// Strategy: launch a private pool-manager on a temp socket with a stub-
// only env (POOL_TEST_MOCK_CHANNELS=1, makes pool-manager fork a no-op
// child that flips immediately to `ready`). Then drive add_group +
// requestChannel + status purely through socket IPC and assert:
//
//   1. Default group is the only group at boot when POOL_GROUPS unset.
//   2. add_group spawns a new group with the requested targetSize.
//   3. A request whose model matches a group is routed to a channel
//      in THAT group (route_decision.servedModel === target).
//   4. A request with an unknown model FALLS BACK to default with
//      route_decision.fallback === true and reason === 'unknown-model'.
//   5. A request with a known but draining group falls back with
//      reason === 'group-draining'.
//   6. A request whose target group has zero ready channels and the
//      group is non-default falls back after POOL_GROUP_WAIT_MS with
//      reason === 'group-no-ready'.
//   7. Default group cannot be removed (error response).
//   8. /health (via api-server, if reachable) exposes pool.groups[].
//
// Cheap by design: mock channels never touch cursor-agent.

import net from 'node:net';
import { spawn } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const MGR = path.join(__dirname, 'pool-manager.mjs');

const SOCK = `/tmp/ratlc-pool-test-${process.pid}.sock`;
const LOG = `/tmp/ratlc-pool-test-${process.pid}.log`;

let mgrProc = null;
const failures = [];
let stepCount = 0;

function step(label) {
  stepCount++;
  console.log(`\n━━━ STEP ${stepCount}: ${label} ━━━`);
}
function pass(msg) { console.log(`  ✅ ${msg}`); }
function fail(msg) { console.log(`  ❌ ${msg}`); failures.push(msg); }
function assert(cond, msg) { (cond ? pass : fail)(msg); }

async function startPool({ envExtra = {}, waitMs = 1200 } = {}) {
  try { fs.unlinkSync(SOCK); } catch {}
  try { fs.unlinkSync(LOG); } catch {}
  const env = {
    ...process.env,
    POOL_SOCK: SOCK,
    POOL_MODEL: 'mock-default-model',
    POOL_SIZE: '2',
    POOL_TEST_MOCK_CHANNELS: '1',
    POOL_GROUP_WAIT_MS: '500',
    POOL_CONCURRENT_OPENS: '8',
    POOL_TOOL_MODE: 'translate',
    POOL_BRIDGE_PROTOCOL: 'h1',
    POOL_CONTEXT_MODE: 'full',
    ...envExtra,
  };
  const fd = fs.openSync(LOG, 'a');
  mgrProc = spawn('node', [MGR], { env, stdio: ['ignore', fd, fd] });
  await new Promise((r) => setTimeout(r, waitMs));
}

async function stopPool() {
  if (!mgrProc) return;
  try { mgrProc.kill('SIGTERM'); } catch {}
  await new Promise((r) => setTimeout(r, 300));
  try { mgrProc.kill('SIGKILL'); } catch {}
  mgrProc = null;
  try { fs.unlinkSync(SOCK); } catch {}
}

// One-shot socket request, returns the FIRST line received.
function poolRequest(obj, timeoutMs = 4000) {
  return new Promise((resolve, reject) => {
    const sock = net.createConnection(SOCK);
    let buf = '';
    const t = setTimeout(() => { try { sock.destroy(); } catch {} ; reject(new Error('timeout: ' + JSON.stringify(obj))); }, timeoutMs);
    sock.on('connect', () => sock.write(JSON.stringify(obj) + '\n'));
    sock.on('data', (c) => {
      buf += c.toString();
      const idx = buf.indexOf('\n');
      if (idx === -1) return;
      try {
        const m = JSON.parse(buf.slice(0, idx));
        clearTimeout(t);
        sock.end();
        resolve(m);
      } catch { /* incomplete */ }
    });
    sock.on('error', (e) => { clearTimeout(t); reject(e); });
  });
}

// Open a streaming request (action send_user_message) and collect events
// until terminal (yield / error). Returns the array of events received.
function streamRequest(obj, terminalMs = 4000) {
  return new Promise((resolve, reject) => {
    const events = [];
    const sock = net.createConnection(SOCK);
    let buf = '';
    const t = setTimeout(() => { try { sock.destroy(); } catch {} ; resolve(events); }, terminalMs);
    sock.on('connect', () => sock.write(JSON.stringify(obj) + '\n'));
    sock.on('data', (c) => {
      buf += c.toString();
      let idx;
      while ((idx = buf.indexOf('\n')) !== -1) {
        const line = buf.slice(0, idx);
        buf = buf.slice(idx + 1);
        if (!line.trim()) continue;
        try {
          const m = JSON.parse(line);
          events.push(m);
          if (m.type === 'yield' || (m.type === 'error' && m.requestId)) {
            clearTimeout(t);
            sock.end();
            return resolve(events);
          }
        } catch (e) { /* ignore */ }
      }
    });
    sock.on('close', () => { clearTimeout(t); resolve(events); });
    sock.on('error', (e) => { clearTimeout(t); reject(e); });
  });
}

async function waitForReady(targetModel, minReady, timeoutMs = 4000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const r = await poolRequest({ type: 'list_groups' });
    const g = (r?.groups || []).find((x) => x.model === targetModel);
    if (g && g.ready >= minReady) return g;
    await new Promise((r) => setTimeout(r, 100));
  }
  return null;
}

(async () => {
  await startPool();

  step('boot — only default group present');
  {
    const r = await poolRequest({ type: 'list_groups' });
    const groups = r?.groups || [];
    assert(groups.length === 1, `groups.length === 1 (got ${groups.length})`);
    assert(groups[0]?.model === 'mock-default-model', `default group model = mock-default-model (got ${groups[0]?.model})`);
    assert(groups[0]?.isDefault === true, 'default group has isDefault: true');
    assert(groups[0]?.target === 2, `default target = 2 (got ${groups[0]?.target})`);
    const dflt = await waitForReady('mock-default-model', 2);
    assert(!!dflt, 'default group reaches ready≥2 within 4s');
  }

  step('add_group — new group registered with target size');
  {
    const r = await poolRequest({ type: 'add_group', model: 'mock-haiku', size: 2 });
    assert(r?.type === 'ack', `add_group ack (got ${r?.type})`);
    const g = await waitForReady('mock-haiku', 2);
    assert(!!g, 'mock-haiku group reaches ready≥2 within 4s');
    const r2 = await poolRequest({ type: 'list_groups' });
    assert((r2.groups || []).length === 2, `groups.length === 2 (got ${(r2.groups || []).length})`);
  }

  step('route to matching group when model is known');
  {
    const reqId = 'req-test-haiku-1';
    const events = await streamRequest({
      type: 'request', requestId: reqId,
      action: 'send_user_message',
      model: 'mock-haiku',
      text: 'hello',
      system: '', tools: [],
    });
    const route = events.find((e) => e.type === 'route_decision');
    assert(!!route, 'received route_decision');
    assert(route?.servedModel === 'mock-haiku', `servedModel = mock-haiku (got ${route?.servedModel})`);
    assert(route?.fallback === false, `fallback === false (got ${route?.fallback})`);
    assert(typeof route?.channelId === 'string' && route.channelId.startsWith('ch-'),
      `channelId is ch-* (got ${route?.channelId})`);
  }

  step('unknown model → fallback to default with reason=unknown-model');
  {
    const reqId = 'req-test-unk-1';
    const events = await streamRequest({
      type: 'request', requestId: reqId,
      action: 'send_user_message',
      model: 'gpt-unknown',
      text: 'hello',
      system: '', tools: [],
    });
    const route = events.find((e) => e.type === 'route_decision');
    assert(!!route, 'received route_decision (unknown model)');
    assert(route?.servedModel === 'mock-default-model',
      `servedModel = mock-default-model (got ${route?.servedModel})`);
    assert(route?.fallback === true, 'fallback === true');
    assert(route?.fallbackReason === 'unknown-model',
      `fallbackReason = unknown-model (got ${route?.fallbackReason})`);
  }

  step('group-no-ready → fall back after POOL_GROUP_WAIT_MS');
  {
    // Add a group sized 0 — it exists but has zero channels (or all busy).
    const r = await poolRequest({ type: 'add_group', model: 'mock-stuck', size: 0 });
    assert(r?.type === 'ack', `add_group(mock-stuck:0) ack (got ${r?.type})`);
    // Now request against it. With POOL_GROUP_WAIT_MS=500 set in env, the
    // request should fall back to default within ~500ms.
    const startT = Date.now();
    const events = await streamRequest({
      type: 'request', requestId: 'req-test-stuck-1',
      action: 'send_user_message',
      model: 'mock-stuck',
      text: 'hello',
      system: '', tools: [],
    }, 4000);
    const elapsed = Date.now() - startT;
    const route = events.find((e) => e.type === 'route_decision');
    assert(!!route, 'received route_decision (group-no-ready)');
    assert(route?.fallback === true, 'fallback === true');
    assert(route?.fallbackReason === 'group-no-ready',
      `fallbackReason = group-no-ready (got ${route?.fallbackReason})`);
    assert(elapsed >= 400 && elapsed < 3000,
      `wait was ~POOL_GROUP_WAIT_MS (got ${elapsed}ms; expected 400..3000ms)`);
  }

  step('default group cannot be removed');
  {
    const r = await poolRequest({ type: 'remove_group', model: 'mock-default-model' });
    assert(r?.type === 'error', `remove_group(default) is error (got ${r?.type})`);
    const msg = String(r?.message || '');
    assert(msg.toLowerCase().includes('default'),
      `error mentions default (got "${msg}")`);
  }

  step('remove_group on a non-default group transitions to draining');
  {
    // Use mock-stuck (size=0) so there's nothing to drain — should
    // disappear immediately. Add mock-codex with size=2 then remove it.
    await poolRequest({ type: 'add_group', model: 'mock-codex', size: 2 });
    await waitForReady('mock-codex', 2);
    const r = await poolRequest({ type: 'remove_group', model: 'mock-codex' });
    assert(r?.type === 'ack', `remove_group(non-default) ack (got ${r?.type})`);
    // Verify it's draining or already gone:
    await new Promise((r) => setTimeout(r, 250));
    const r2 = await poolRequest({ type: 'list_groups' });
    const found = (r2.groups || []).find((g) => g.model === 'mock-codex');
    if (found) {
      assert(found.draining === true, 'mock-codex.draining === true');
    } else {
      pass('mock-codex group removed (channels closed immediately)');
    }
    // route to draining group with no ready channels falls back as
    // group-draining (or unknown-model if already gone).
    const events = await streamRequest({
      type: 'request', requestId: 'req-test-drain-1',
      action: 'send_user_message',
      model: 'mock-codex',
      text: 'hello',
      system: '', tools: [],
    }, 4000);
    const route = events.find((e) => e.type === 'route_decision');
    assert(!!route, 'received route_decision (post-drain)');
    assert(route?.fallback === true, 'fallback === true (post-drain)');
    assert(['group-draining', 'unknown-model'].includes(route?.fallbackReason),
      `fallbackReason ∈ {group-draining, unknown-model} (got ${route?.fallbackReason})`);
  }

  step('status snapshot exposes pool.groups[] with per-group counts');
  {
    const r = await poolRequest({ type: 'status' });
    const groups = r?.pool?.groups;
    assert(Array.isArray(groups), 'pool.groups is an array');
    const dflt = groups.find((g) => g.isDefault);
    assert(!!dflt, 'pool.groups contains the default group');
    assert(typeof dflt?.ready === 'number', 'default group has ready count');
    assert(typeof dflt?.target === 'number', 'default group has target');
    assert(r?.pool?.defaultGroup === 'mock-default-model',
      `pool.defaultGroup = mock-default-model (got ${r?.pool?.defaultGroup})`);
    // Per-channel group field
    const channels = r?.pool?.channels || [];
    assert(channels.every((c) => typeof c.group === 'string' && c.group),
      'every channel has a non-empty group field');
  }

  await stopPool();

  console.log('');
  if (failures.length === 0) {
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    console.log(`✅ multi-group-routing: ALL ${stepCount} STEPS PASS`);
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    process.exit(0);
  } else {
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    console.log(`❌ multi-group-routing: ${failures.length} failure(s):`);
    for (const m of failures) console.log(`   - ${m}`);
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    process.exit(1);
  }
})().catch(async (e) => {
  console.error('test exception:', e);
  try { await stopPool(); } catch {}
  process.exit(2);
});
