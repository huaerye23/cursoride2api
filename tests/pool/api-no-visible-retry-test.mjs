#!/usr/bin/env node

import assert from 'node:assert/strict';
import fs from 'node:fs';
import net from 'node:net';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const ROOT = path.resolve(__dirname, '../..');
const MGR = path.join(ROOT, 'scaffolding/pool/pool-manager.mjs');
const API = path.join(ROOT, 'scaffolding/pool/api-server.mjs');
const SOCK = `/tmp/ratlc-no-visible-retry-${process.pid}.sock`;
const STALL_FILE = `/tmp/ratlc-no-visible-retry-${process.pid}.stall`;
const MGR_LOG = `/tmp/ratlc-no-visible-retry-mgr-${process.pid}.log`;
const API_LOG = `/tmp/ratlc-no-visible-retry-api-${process.pid}.log`;

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

async function waitFor(fn, timeoutMs = 7000) {
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
  for (const file of [SOCK, STALL_FILE, MGR_LOG]) {
    try { fs.unlinkSync(file); } catch {}
  }
  const env = {
    ...process.env,
    POOL_SOCK: SOCK,
    POOL_MODEL: 'mock-default-model',
    POOL_SIZE: '2',
    POOL_TEST_MOCK_CHANNELS: '1',
    POOL_GROUP_WAIT_MS: '100',
    POOL_CONCURRENT_OPENS: '2',
    POOL_TOOL_MODE: 'translate',
    POOL_BRIDGE_PROTOCOL: 'h1',
    POOL_CONTEXT_MODE: 'full',
    MOCK_STALL_ONCE_FILE: STALL_FILE,
  };
  const fd = fs.openSync(MGR_LOG, 'a');
  mgrProc = spawn('node', [MGR], { env, stdio: ['ignore', fd, fd] });
  await waitFor(() => fs.existsSync(SOCK));
  await waitFor(() => {
    const log = fs.existsSync(MGR_LOG) ? fs.readFileSync(MGR_LOG, 'utf8') : '';
    return (log.match(/READY/g) || []).length >= 2;
  });
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
    RATLC_NO_VISIBLE_EVENT_TIMEOUT_MS: '5000',
    RATLC_NO_VISIBLE_EVENT_RETRIES: '1',
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
  for (const file of [SOCK, STALL_FILE]) {
    try { fs.unlinkSync(file); } catch {}
  }
}

try {
  await startPool();
  const port = await getFreePort();
  await startApi(port);

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
      messages: [{
        role: 'user',
        content: '__MOCK_STALL_ONCE__ answer after retry',
      }],
    }),
  });

  assert.equal(res.status, 200);
  const sse = await res.text();
  assert.match(sse, /message_stop/, 'API response should complete after retry');
  assert.match(sse, /\[mock [^\]]+\] ack/, 'retry should stream mock answer');
  assert.doesNotMatch(sse, /Retried 1 time\(s\) before giving up/, 'retry should not leak final failure notice');

  const requestsRes = await fetch(`http://127.0.0.1:${port}/requests?limit=1`);
  assert.equal(requestsRes.status, 200);
  const requests = await requestsRes.json();
  const item = requests.items?.[0];
  assert.equal(item?.status, 'completed');
  assert.equal(item?.noVisibleEventRetryCount, 1);

  const apiLog = fs.readFileSync(API_LOG, 'utf8');
  const mgrLog = fs.readFileSync(MGR_LOG, 'utf8');
  assert.match(apiLog, /no visible upstream event timeout .*retry 1\/1/, 'API should log first-event retry');
  assert.match(mgrLog, /cancelling active channel .*no_visible_event_timeout.* silently/, 'pool should silently cancel stalled channel');
  assert.match(mgrLog, /routed action=send_user_message to ch-/, 'pool should reroute the request');

  console.log('api-no-visible-retry-test: OK');
} finally {
  await stopAll();
}
