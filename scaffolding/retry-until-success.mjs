#!/usr/bin/env node
// Aggressive-retry scaffolding: hammer the proxy until one premium-model
// request gets through. The "unpaid invoice" error Cursor returns is
// probabilistic for paid Pro accounts on Claude models — empirically ~1-2%
// success rate per attempt. This script tests whether short-interval retry
// matches Bajie's renderer-side recipe.

import http from 'node:http';

const PROXY_URL = process.env.PROXY_URL || 'http://127.0.0.1:4141/v1/chat/completions';
const MODEL = process.env.MODEL || 'claude-opus-4-7-thinking-max';
const PROMPT = process.env.PROMPT || 'Reply with exactly: PONG';
const MAX_ATTEMPTS = parseInt(process.env.MAX_ATTEMPTS || '200', 10);
const INTERVAL_MS = parseInt(process.env.INTERVAL_MS || '300', 10);
const STOP_ON_SUCCESS = process.env.KEEP_GOING !== '1';

// Keep-alive agent so we reuse the TCP connection to the proxy and don't
// pay handshake cost on every attempt. We're testing Cursor's gate, not
// localhost TCP setup.
const agent = new http.Agent({ keepAlive: true, maxSockets: 1 });

const url = new URL(PROXY_URL);

function postOnce() {
  return new Promise((resolve) => {
    const startedAt = Date.now();
    const body = JSON.stringify({
      model: MODEL,
      messages: [{ role: 'user', content: PROMPT }],
      stream: false,
      max_tokens: 30,
    });
    const req = http.request(
      {
        hostname: url.hostname,
        port: url.port,
        path: url.pathname,
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Content-Length': Buffer.byteLength(body),
        },
        agent,
      },
      (res) => {
        let chunks = [];
        res.on('data', (c) => chunks.push(c));
        res.on('end', () => {
          const elapsed = Date.now() - startedAt;
          const text = Buffer.concat(chunks).toString('utf8');
          resolve({ status: res.statusCode, text, elapsed });
        });
      }
    );
    req.setTimeout(30_000, () => {
      req.destroy(new Error('timeout'));
    });
    req.on('error', (e) => {
      resolve({ status: 0, text: `network_error:${e.code || e.message}`, elapsed: Date.now() - startedAt });
    });
    req.write(body);
    req.end();
  });
}

function classify(res) {
  if (res.status === 0) return 'network_error';
  if (res.status >= 200 && res.status < 300) {
    if (/"choices"/.test(res.text) && /"finish_reason"/.test(res.text)) return 'success';
    return 'success_partial';
  }
  if (/unpaid invoice|cursor\.com\/dashboard/i.test(res.text)) return 'unpaid_invoice';
  if (/rate.?limit/i.test(res.text)) return 'rate_limit';
  return 'other_error';
}

function pct(arr, p) {
  if (!arr.length) return 0;
  const sorted = [...arr].sort((a, b) => a - b);
  const idx = Math.min(sorted.length - 1, Math.floor(p * sorted.length));
  return sorted[idx];
}

async function main() {
  console.log(`scaffold: retry-until-success`);
  console.log(`  proxy:    ${PROXY_URL}`);
  console.log(`  model:    ${MODEL}`);
  console.log(`  attempts: ${MAX_ATTEMPTS}, interval: ${INTERVAL_MS}ms`);
  console.log(`  stop on first success: ${STOP_ON_SUCCESS}`);
  console.log('');

  const counts = {};
  const latencies = { success: [], unpaid_invoice: [], other_error: [], network_error: [], rate_limit: [] };
  const startedAt = Date.now();
  let firstSuccessAttempt = null;
  let firstSuccessText = null;

  for (let i = 1; i <= MAX_ATTEMPTS; i++) {
    const res = await postOnce();
    const kind = classify(res);
    counts[kind] = (counts[kind] || 0) + 1;
    (latencies[kind] || (latencies[kind] = [])).push(res.elapsed);

    const stamp = `[${String(i).padStart(3, ' ')}/${MAX_ATTEMPTS}]`;
    const ms = `${String(res.elapsed).padStart(4, ' ')}ms`;
    if (kind === 'success' || kind === 'success_partial') {
      if (firstSuccessAttempt == null) {
        firstSuccessAttempt = i;
        firstSuccessText = res.text;
      }
      console.log(`${stamp} ${ms}  SUCCESS  status=${res.status}`);
      if (STOP_ON_SUCCESS) break;
    } else if (kind === 'unpaid_invoice') {
      console.log(`${stamp} ${ms}  unpaid_invoice`);
    } else if (kind === 'rate_limit') {
      console.log(`${stamp} ${ms}  rate_limit  ${res.text.slice(0, 120)}`);
    } else if (kind === 'network_error') {
      console.log(`${stamp} ${ms}  net  ${res.text}`);
    } else {
      console.log(`${stamp} ${ms}  other(${res.status})  ${res.text.slice(0, 200)}`);
    }

    if (i < MAX_ATTEMPTS) {
      await new Promise((r) => setTimeout(r, INTERVAL_MS));
    }
  }

  const totalElapsed = Date.now() - startedAt;
  console.log('');
  console.log('=== summary ===');
  console.log(`total wall time:      ${(totalElapsed / 1000).toFixed(1)}s`);
  console.log(`attempts:             ${Object.values(counts).reduce((a, b) => a + b, 0)}`);
  for (const [k, n] of Object.entries(counts)) {
    const lat = latencies[k] || [];
    const p50 = pct(lat, 0.5);
    const p95 = pct(lat, 0.95);
    console.log(`  ${k.padEnd(16)} count=${String(n).padStart(3)}  p50=${p50}ms  p95=${p95}ms`);
  }
  console.log('');
  if (firstSuccessAttempt != null) {
    console.log(`first success on attempt #${firstSuccessAttempt}`);
    const total = Object.values(counts).reduce((a, b) => a + b, 0);
    console.log(`empirical success rate: ${firstSuccessAttempt > 0 ? (1 / firstSuccessAttempt * 100).toFixed(2) : 0}% (1 of ${firstSuccessAttempt})`);
    console.log('');
    console.log('first success body:');
    try {
      const parsed = JSON.parse(firstSuccessText);
      const content = parsed.choices?.[0]?.message?.content ?? '<no content field>';
      console.log(`  model:   ${parsed.model}`);
      console.log(`  content: ${JSON.stringify(content).slice(0, 200)}`);
      console.log(`  usage:   ${JSON.stringify(parsed.usage)}`);
    } catch {
      console.log(firstSuccessText.slice(0, 400));
    }
  } else {
    console.log(`NO SUCCESS in ${MAX_ATTEMPTS} attempts.`);
  }

  process.exit(firstSuccessAttempt != null ? 0 : 1);
}

main().catch((e) => {
  console.error('fatal:', e);
  process.exit(2);
});
