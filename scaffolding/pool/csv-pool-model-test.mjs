#!/usr/bin/env node
// csv-pool-model-test.mjs
//
// Verifies the CSV form of POOL_MODEL parses into the same group state as
// the equivalent POOL_MODEL+POOL_GROUPS combo. Uses POOL_TEST_MOCK_CHANNELS=1
// so channels open instantly and we never pay Cursor quota.
//
// Scenarios:
//   A. POOL_MODEL=mock-default-model (legacy)
//      → 1 group (default), targetSize = POOL_SIZE
//   B. POOL_MODEL=mock-default-model,mock-haiku:3
//      → 2 groups: default (POOL_SIZE), mock-haiku (3)
//   C. POOL_MODEL=mock-default-model:5,mock-haiku:3,mock-codex:1
//      → 3 groups: default(5), haiku(3), codex(1) — explicit size on default
//   D. POOL_MODEL=mock-default-model:5,mock-haiku    (no size on tail)
//      → mock-haiku gets POOL_SIZE
//   E. Mixed: POOL_MODEL=mock-default-model,mock-haiku:3 + POOL_GROUPS=mock-codex:1
//      → 3 groups in total (POOL_GROUPS merges in)
//   F. Whitespace/duplicates: POOL_MODEL="mock-default-model:5,  mock-default-model:2"
//      → folds back into default with targetSize = 5 + 2 = 7

import net from 'node:net';
import { spawn } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const MGR = path.join(__dirname, 'pool-manager.mjs');

const SOCK_BASE = `/tmp/ratlc-pool-csv-test-${process.pid}`;
const LOG_BASE = `/tmp/ratlc-pool-csv-test-${process.pid}`;

let mgrProc = null;
const failures = [];
let stepCount = 0;
let currentSock = null;

function step(label) {
  stepCount++;
  console.log(`\n━━━ STEP ${stepCount}: ${label} ━━━`);
}
function pass(msg) { console.log(`  ✅ ${msg}`); }
function fail(msg) { console.log(`  ❌ ${msg}`); failures.push(msg); }
function assert(cond, msg) { (cond ? pass : fail)(msg); }

async function startPool({ envExtra = {}, sockSuffix = '', waitMs = 1200 } = {}) {
  if (mgrProc) await stopPool();
  currentSock = `${SOCK_BASE}-${sockSuffix}.sock`;
  const logFile = `${LOG_BASE}-${sockSuffix}.log`;
  try { fs.unlinkSync(currentSock); } catch {}
  try { fs.unlinkSync(logFile); } catch {}
  const env = {
    ...process.env,
    POOL_SOCK: currentSock,
    POOL_SIZE: '2',
    POOL_TEST_MOCK_CHANNELS: '1',
    POOL_GROUP_WAIT_MS: '500',
    POOL_CONCURRENT_OPENS: '8',
    POOL_TOOL_MODE: 'translate',
    POOL_BRIDGE_PROTOCOL: 'h1',
    POOL_CONTEXT_MODE: 'full',
    ...envExtra,
  };
  const fd = fs.openSync(logFile, 'a');
  mgrProc = spawn('node', [MGR], { env, stdio: ['ignore', fd, fd] });
  await new Promise((r) => setTimeout(r, waitMs));
}

async function stopPool() {
  if (!mgrProc) return;
  try { mgrProc.kill('SIGTERM'); } catch {}
  await new Promise((r) => setTimeout(r, 300));
  try { mgrProc.kill('SIGKILL'); } catch {}
  mgrProc = null;
  if (currentSock) {
    try { fs.unlinkSync(currentSock); } catch {}
    currentSock = null;
  }
}

function poolRequest(obj, timeoutMs = 4000) {
  return new Promise((resolve, reject) => {
    const sock = net.createConnection(currentSock);
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

function findGroup(groups, model) {
  return (groups || []).find((g) => g.model === model);
}

(async () => {
  step('A. legacy single-value POOL_MODEL — default group only');
  {
    await startPool({
      envExtra: { POOL_MODEL: 'mock-default-model' },
      sockSuffix: 'a',
    });
    const r = await poolRequest({ type: 'list_groups' });
    const groups = r?.groups || [];
    assert(groups.length === 1, `1 group (got ${groups.length})`);
    const dflt = findGroup(groups, 'mock-default-model');
    assert(!!dflt, 'default group present');
    assert(dflt?.isDefault === true, 'isDefault=true');
    assert(dflt?.target === 2, `default target = POOL_SIZE = 2 (got ${dflt?.target})`);
  }

  step('B. CSV — POOL_MODEL=opus,haiku:3');
  {
    await startPool({
      envExtra: { POOL_MODEL: 'mock-default-model,mock-haiku:3' },
      sockSuffix: 'b',
    });
    const r = await poolRequest({ type: 'list_groups' });
    const groups = r?.groups || [];
    assert(groups.length === 2, `2 groups (got ${groups.length})`);
    const dflt = findGroup(groups, 'mock-default-model');
    const haiku = findGroup(groups, 'mock-haiku');
    assert(dflt?.isDefault === true, 'default group is mock-default-model');
    assert(dflt?.target === 2, `default target = POOL_SIZE = 2 (got ${dflt?.target})`);
    assert(haiku?.isDefault === false, 'haiku is NOT default');
    assert(haiku?.target === 3, `haiku target = 3 (got ${haiku?.target})`);
  }

  step('C. CSV with explicit default size — POOL_MODEL=opus:5,haiku:3,codex:1');
  {
    await startPool({
      envExtra: { POOL_MODEL: 'mock-default-model:5,mock-haiku:3,mock-codex:1' },
      sockSuffix: 'c',
    });
    const r = await poolRequest({ type: 'list_groups' });
    const groups = r?.groups || [];
    assert(groups.length === 3, `3 groups (got ${groups.length})`);
    assert(findGroup(groups, 'mock-default-model')?.target === 5,
      `default target = 5 (got ${findGroup(groups, 'mock-default-model')?.target})`);
    assert(findGroup(groups, 'mock-haiku')?.target === 3,
      `haiku target = 3 (got ${findGroup(groups, 'mock-haiku')?.target})`);
    assert(findGroup(groups, 'mock-codex')?.target === 1,
      `codex target = 1 (got ${findGroup(groups, 'mock-codex')?.target})`);
  }

  step('D. CSV with no size on trailing entry — defaults to POOL_SIZE');
  {
    await startPool({
      envExtra: { POOL_MODEL: 'mock-default-model:5,mock-haiku' },
      sockSuffix: 'd',
    });
    const r = await poolRequest({ type: 'list_groups' });
    const groups = r?.groups || [];
    assert(groups.length === 2, `2 groups (got ${groups.length})`);
    assert(findGroup(groups, 'mock-default-model')?.target === 5,
      `default target = 5 (got ${findGroup(groups, 'mock-default-model')?.target})`);
    assert(findGroup(groups, 'mock-haiku')?.target === 2,
      `haiku target = POOL_SIZE = 2 (got ${findGroup(groups, 'mock-haiku')?.target})`);
  }

  step('E. CSV equivalence — POOL_MODEL=opus,haiku:3 ≡ POOL_MODEL=opus POOL_GROUPS=haiku:3');
  {
    await startPool({
      envExtra: { POOL_MODEL: 'mock-default-model', POOL_GROUPS: 'mock-haiku:3' },
      sockSuffix: 'e1',
    });
    const r1 = await poolRequest({ type: 'list_groups' });
    await stopPool();
    await startPool({
      envExtra: { POOL_MODEL: 'mock-default-model,mock-haiku:3' },
      sockSuffix: 'e2',
    });
    const r2 = await poolRequest({ type: 'list_groups' });
    const g1 = (r1?.groups || []).map((g) => ({ model: g.model, target: g.target, isDefault: g.isDefault }))
      .sort((a, b) => a.model.localeCompare(b.model));
    const g2 = (r2?.groups || []).map((g) => ({ model: g.model, target: g.target, isDefault: g.isDefault }))
      .sort((a, b) => a.model.localeCompare(b.model));
    assert(JSON.stringify(g1) === JSON.stringify(g2),
      `groups identical (legacy=${JSON.stringify(g1)} csv=${JSON.stringify(g2)})`);
  }

  step('F. CSV + POOL_GROUPS — both merge in');
  {
    await startPool({
      envExtra: { POOL_MODEL: 'mock-default-model,mock-haiku:3', POOL_GROUPS: 'mock-codex:1' },
      sockSuffix: 'f',
    });
    const r = await poolRequest({ type: 'list_groups' });
    const groups = r?.groups || [];
    assert(groups.length === 3, `3 groups (got ${groups.length}: ${groups.map((g) => g.model).join(',')})`);
    assert(findGroup(groups, 'mock-codex')?.target === 1,
      `codex from POOL_GROUPS target = 1 (got ${findGroup(groups, 'mock-codex')?.target})`);
  }

  step('G. duplicate-default folds into default');
  {
    await startPool({
      envExtra: { POOL_MODEL: 'mock-default-model:5,mock-default-model:2' },
      sockSuffix: 'g',
    });
    const r = await poolRequest({ type: 'list_groups' });
    const groups = r?.groups || [];
    assert(groups.length === 1, `1 group (got ${groups.length})`);
    assert(findGroup(groups, 'mock-default-model')?.target === 7,
      `merged target = 5 + 2 = 7 (got ${findGroup(groups, 'mock-default-model')?.target})`);
  }

  step('H. whitespace tolerance — POOL_MODEL=" opus , haiku:3 "');
  {
    await startPool({
      envExtra: { POOL_MODEL: ' mock-default-model , mock-haiku:3 ' },
      sockSuffix: 'h',
    });
    const r = await poolRequest({ type: 'list_groups' });
    const groups = r?.groups || [];
    assert(groups.length === 2, `2 groups (got ${groups.length})`);
    assert(findGroup(groups, 'mock-haiku')?.target === 3,
      `haiku target = 3 (got ${findGroup(groups, 'mock-haiku')?.target})`);
  }

  await stopPool();

  console.log('');
  if (failures.length === 0) {
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    console.log(`✅ csv-pool-model: ALL ${stepCount} STEPS PASS`);
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    process.exit(0);
  } else {
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    console.log(`❌ csv-pool-model: ${failures.length} failure(s):`);
    for (const m of failures) console.log(`   - ${m}`);
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    process.exit(1);
  }
})().catch(async (e) => {
  console.error('test exception:', e);
  try { await stopPool(); } catch {}
  process.exit(2);
});
