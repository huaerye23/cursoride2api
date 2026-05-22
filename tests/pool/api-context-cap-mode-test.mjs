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

async function runScenario(contextMode) {
  const label = `${contextMode}-${process.pid}-${Date.now()}`;
  const sock = `/tmp/ratlc-context-cap-${label}.sock`;
  const mgrLog = `/tmp/ratlc-context-cap-mgr-${label}.log`;
  const apiLog = `/tmp/ratlc-context-cap-api-${label}.log`;
  let mgrProc = null;
  let apiProc = null;

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
    try { fs.unlinkSync(sock); } catch {}
  }

  try {
    try { fs.unlinkSync(sock); } catch {}
    try { fs.unlinkSync(mgrLog); } catch {}
    try { fs.unlinkSync(apiLog); } catch {}

    const commonEnv = {
      ...process.env,
      POOL_SOCK: sock,
      POOL_MODEL: 'mock-default-model',
      POOL_SIZE: '1',
      POOL_TEST_MOCK_CHANNELS: '1',
      POOL_GROUP_WAIT_MS: '100',
      POOL_CONCURRENT_OPENS: '1',
      POOL_TOOL_MODE: 'translate',
      POOL_BRIDGE_PROTOCOL: 'h1',
      POOL_CONTEXT_MODE: contextMode,
    };

    const mgrFd = fs.openSync(mgrLog, 'a');
    mgrProc = spawn('node', [MGR], { env: commonEnv, stdio: ['ignore', mgrFd, mgrFd] });
    await waitFor(() => fs.existsSync(sock), 5000);
    await waitFor(() => fs.existsSync(mgrLog) && fs.readFileSync(mgrLog, 'utf8').includes('READY'), 5000);

    const port = await getFreePort();
    const apiFd = fs.openSync(apiLog, 'a');
    apiProc = spawn('node', [API], {
      env: {
        ...commonEnv,
        RATLC_API_HOST: '127.0.0.1',
        RATLC_API_PORT: String(port),
        RATLC_CONTEXT_MAX_BYTES: '1000',
        RATLC_CONTEXT_MANIFEST_THRESHOLD_BYTES: '0',
        RATLC_NO_VISIBLE_EVENT_TIMEOUT_MS: '5000',
      },
      stdio: ['ignore', apiFd, apiFd],
    });
    await waitFor(async () => {
      const res = await fetch(`http://127.0.0.1:${port}/health`);
      return res.ok;
    }, 5000);

    const largeText = 'x'.repeat(2500);
    const res = await fetch(`http://127.0.0.1:${port}/v1/messages`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'anthropic-version': '2023-06-01',
        'x-claude-code-session-id': `cap-test-${label}`,
      },
      body: JSON.stringify({
        model: 'mock-default-model',
        max_tokens: 64,
        stream: true,
        messages: [
          { role: 'user', content: 'remember the following large context' },
          { role: 'assistant', content: `ack ${largeText}` },
          { role: 'user', content: 'what did I ask you to remember?' },
        ],
      }),
    });

    assert.equal(res.status, 200);
    const sse = await res.text();
    assert.match(sse, /message_stop/, `${contextMode}: response should complete`);

    const requestsRes = await fetch(`http://127.0.0.1:${port}/requests?limit=1`);
    assert.equal(requestsRes.status, 200);
    const requests = await requestsRes.json();
    const item = requests.items?.[0];
    assert.equal(item?.contextMode, contextMode);

    return { request: item, apiLog: fs.readFileSync(apiLog, 'utf8') };
  } finally {
    await stopAll();
  }
}

const full = await runScenario('full');
assert.equal(full.request.effectiveContextMode, 'full', 'full mode must keep full context even above guard size');
assert.equal(full.request.contextGuardReason, null, 'full mode must not record a guard downgrade');
assert(full.request.textBytes > 1000, `full mode should send oversized textBytes (${full.request.textBytes})`);
assert.match(full.apiLog, /mode=full .*textBytes=/, 'full mode log should show full routing');
assert.doesNotMatch(full.apiLog, /guard=full-context-too-large/, 'full mode log should not show context guard');

const hybrid = await runScenario('hybrid');
assert.equal(hybrid.request.contextMode, 'hybrid');
assert.equal(hybrid.request.effectiveContextMode, 'last', 'hybrid full turn above guard size should fall back to last');
assert.match(hybrid.request.contextGuardReason || '', /^full-context-too-large:/);
assert(hybrid.request.textBytes < 1000, `hybrid fallback should send small last-turn textBytes (${hybrid.request.textBytes})`);
assert.match(hybrid.apiLog, /mode=hybrid\/full .*guard=full-context-too-large:/, 'hybrid log should show guard downgrade');

console.log('api-context-cap-mode-test: OK');
