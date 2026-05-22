#!/usr/bin/env node

import assert from 'node:assert/strict';
import fs from 'node:fs';
import fsp from 'node:fs/promises';
import net from 'node:net';
import os from 'node:os';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const ROOT = path.resolve(__dirname, '../..');
const MGR = path.join(ROOT, 'scaffolding/pool/pool-manager.mjs');
const API = path.join(ROOT, 'scaffolding/pool/api-server.mjs');
const SOCK = `/tmp/ratlc-context-manifest-${process.pid}.sock`;
const STORE = path.join(os.tmpdir(), `ratlc-context-manifest-store-${process.pid}`);
const MGR_LOG = `/tmp/ratlc-context-manifest-mgr-${process.pid}.log`;
const API_LOG = `/tmp/ratlc-context-manifest-api-${process.pid}.log`;
process.env.RATLC_CONTEXT_STORE_DIR = STORE;

const { runContextTool } = await import('../../scaffolding/pool/context-store.mjs');
const { defaultTranslateModeTools } = await import('../../scaffolding/pool/tool-translator.mjs');

let mgrProc = null;
let apiProc = null;

async function getFreePort() {
  return new Promise((resolve, reject) => {
    const server = net.createServer();
    server.on('error', reject);
    server.listen(0, '127.0.0.1', () => {
      const port = server.address().port;
      server.close(() => resolve(port));
    });
  });
}

async function waitFor(fn, timeoutMs = 5000) {
  const deadline = Date.now() + timeoutMs;
  let lastError;
  while (Date.now() < deadline) {
    try {
      const value = await fn();
      if (value) return value;
    } catch (e) {
      lastError = e;
    }
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw lastError || new Error(`timeout after ${timeoutMs}ms`);
}

async function startPool() {
  for (const file of [SOCK, MGR_LOG]) {
    try { fs.unlinkSync(file); } catch {}
  }
  await fsp.rm(STORE, { recursive: true, force: true });
  const env = {
    ...process.env,
    POOL_SOCK: SOCK,
    POOL_MODEL: 'mock-default-model',
    POOL_SIZE: '1',
    POOL_TEST_MOCK_CHANNELS: '1',
    POOL_GROUP_WAIT_MS: '100',
    POOL_CONCURRENT_OPENS: '1',
    POOL_TOOL_MODE: 'translate',
    POOL_BRIDGE_PROTOCOL: 'h1',
    POOL_CONTEXT_MODE: 'full',
    RATLC_CONTEXT_STORE_DIR: STORE,
  };
  const fd = fs.openSync(MGR_LOG, 'a');
  mgrProc = spawn('node', [MGR], { env, stdio: ['ignore', fd, fd] });
  await waitFor(() => fs.existsSync(SOCK));
  await waitFor(() => fs.existsSync(MGR_LOG) && fs.readFileSync(MGR_LOG, 'utf8').includes('READY'));
}

async function startApi(port) {
  try { fs.unlinkSync(API_LOG); } catch {}
  const env = {
    ...process.env,
    POOL_SOCK: SOCK,
    RATLC_API_HOST: '127.0.0.1',
    RATLC_API_PORT: String(port),
    POOL_CONTEXT_MODE: 'full',
    POOL_TOOL_MODE: 'translate',
    RATLC_CONTEXT_MANIFEST_THRESHOLD_BYTES: '1000',
    RATLC_CONTEXT_STORE_DIR: STORE,
    RATLC_NO_VISIBLE_EVENT_TIMEOUT_MS: '5000',
  };
  const fd = fs.openSync(API_LOG, 'a');
  apiProc = spawn('node', [API], { env, stdio: ['ignore', fd, fd] });
  await waitFor(async () => {
    const res = await fetch(`http://127.0.0.1:${port}/health`);
    return res.ok;
  });
}

async function stopAll() {
  for (const proc of [apiProc, mgrProc]) {
    if (!proc) continue;
    try { proc.kill('SIGTERM'); } catch {}
  }
  await new Promise((resolve) => setTimeout(resolve, 200));
  for (const proc of [apiProc, mgrProc]) {
    if (!proc) continue;
    try { proc.kill('SIGKILL'); } catch {}
  }
  try { fs.unlinkSync(SOCK); } catch {}
  await fsp.rm(STORE, { recursive: true, force: true });
}

try {
  const tools = defaultTranslateModeTools();
  assert(tools.some((t) => t.name === 'ratlc_context_read'), 'context read tool is registered in translate mode');
  assert(tools.some((t) => t.name === 'ratlc_context_search'), 'context search tool is registered in translate mode');

  await startPool();
  const port = await getFreePort();
  await startApi(port);

  const needle = 'NEEDLE_VALUE_42';
  const largeText = `prefix ${'x'.repeat(1600)} ${needle} suffix`;
  const res = await fetch(`http://127.0.0.1:${port}/v1/messages`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'anthropic-version': '2023-06-01',
    },
    body: JSON.stringify({
      model: 'mock-default-model',
      max_tokens: 64,
      stream: true,
      messages: [
        { role: 'user', content: 'remember this long text' },
        { role: 'assistant', content: largeText },
        { role: 'user', content: 'what is the needle value?' },
      ],
    }),
  });

  assert.equal(res.status, 200);
  const sse = await res.text();
  assert.match(sse, /message_stop/);

  const requestsRes = await fetch(`http://127.0.0.1:${port}/requests?limit=1`);
  assert.equal(requestsRes.status, 200);
  const requests = await requestsRes.json();
  const item = requests.items?.[0];
  assert.equal(item?.effectiveContextMode, 'full_manifest');
  assert(item?.contextManifestSnapshotId, 'request log should expose snapshot id');
  assert(item?.contextManifestTotalBytes > 1000, 'manifest records original oversized context bytes');
  assert(item?.textBytes < item?.contextManifestTotalBytes, 'sent manifest text should be smaller than original context');

  const search = await runContextTool('ratlc_context_search', {
    snapshotId: item.contextManifestSnapshotId,
    query: needle,
  });
  assert.match(search, new RegExp(needle), 'context search should find original omitted text');

  const msg = await runContextTool('ratlc_context_get_message', {
    snapshotId: item.contextManifestSnapshotId,
    index: 1,
  });
  assert.match(msg, new RegExp(needle), 'context get_message should retrieve original assistant message');

  const apiLog = fs.readFileSync(API_LOG, 'utf8');
  assert.match(apiLog, /manifest=[a-f0-9]{64}/, 'API log should include manifest snapshot id');

  console.log('api-context-manifest-test: OK');
} finally {
  await stopAll();
}
