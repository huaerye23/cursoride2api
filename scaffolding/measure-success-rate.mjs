#!/usr/bin/env node
// Measure per-attempt success rate for HTTP/2 vs HTTP/1.1 against Cursor's
// claude-opus-4-7-thinking-max gate.
//
// H2 path: through the local proxy (which uses http2.connect to api2.cursor.sh).
//          Full request payload — exercises the actual gate.
// H1 path: direct curl-style POST to api2.cursor.sh with HTTP/1.1.
//          Tests whether HTTP/1.1 can reach the gate at all.
//
// Output: per-attempt log + summary breakdown for each phase.

import http from 'node:http';
import https from 'node:https';
import fs from 'node:fs';
import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);
const { generateChecksum } = require('../src/cursor-client');

const PROXY_URL = process.env.PROXY_URL || 'http://127.0.0.1:4141/v1/chat/completions';
const CURSOR_HOST = process.env.CURSOR_HOST || 'api2.cursor.sh';
const MODEL = process.env.MODEL || 'claude-opus-4-7-thinking-max';
const PROMPT = process.env.PROMPT || 'Reply with exactly: PONG';
const MAX_H2 = parseInt(process.env.MAX_H2 || '300', 10);
const MAX_H1 = parseInt(process.env.MAX_H1 || '100', 10);
const INTERVAL_MS = parseInt(process.env.INTERVAL_MS || '300', 10);
const SKIP_H2 = process.env.SKIP_H2 === '1';
const SKIP_H1 = process.env.SKIP_H1 === '1';

const tokenFile = JSON.parse(fs.readFileSync(new URL('../token.json', import.meta.url), 'utf8'));
const tok = tokenFile.tokens[0];
const checksum = generateChecksum(tok.machineId, tok.macMachineId);

const h2Url = new URL(PROXY_URL);
const h2Agent = new http.Agent({ keepAlive: true, maxSockets: 1 });
const h1Agent = new https.Agent({ keepAlive: true, maxSockets: 1 });

// ── H2 path: through proxy with full payload ────────────────────────────────
function attemptH2() {
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
        hostname: h2Url.hostname,
        port: h2Url.port,
        path: h2Url.pathname,
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) },
        agent: h2Agent,
      },
      (res) => {
        let chunks = [];
        res.on('data', (c) => chunks.push(c));
        res.on('end', () => {
          resolve({
            status: res.statusCode,
            text: Buffer.concat(chunks).toString('utf8'),
            elapsed: Date.now() - startedAt,
          });
        });
      }
    );
    req.setTimeout(45_000, () => req.destroy(new Error('timeout')));
    req.on('error', (e) => resolve({ status: 0, text: `network:${e.code || e.message}`, elapsed: Date.now() - startedAt }));
    req.write(body);
    req.end();
  });
}

// ── H1 path: direct to api2.cursor.sh ───────────────────────────────────────
// We send a minimal connect+proto frame (5-byte envelope header + empty body).
// The point isn't to get a valid response — it's to see if HTTP/1.1 ever
// reaches Cursor's gate at all, or whether ELB rejects everything with 464.
function attemptH1() {
  return new Promise((resolve) => {
    const startedAt = Date.now();
    const body = Buffer.from([0, 0, 0, 0, 0]); // empty connect-proto frame
    const req = https.request(
      {
        hostname: CURSOR_HOST,
        port: 443,
        path: '/agent.v1.AgentService/Run',
        method: 'POST',
        headers: {
          'Content-Type': 'application/connect+proto',
          'Authorization': `Bearer ${tok.accessToken}`,
          'x-cursor-checksum': checksum,
          'x-cursor-client-version': '2.6.20',
          'connect-protocol-version': '1',
          'Content-Length': body.length,
        },
        agent: h1Agent,
        // Node's https client is HTTP/1.1 by default — no ALPN negotiation for h2
      },
      (res) => {
        let chunks = [];
        res.on('data', (c) => chunks.push(c));
        res.on('end', () => {
          resolve({
            status: res.statusCode,
            httpVersion: res.httpVersion,
            text: Buffer.concat(chunks).toString('utf8'),
            elapsed: Date.now() - startedAt,
            server: res.headers.server || '',
          });
        });
      }
    );
    req.setTimeout(20_000, () => req.destroy(new Error('timeout')));
    req.on('error', (e) => resolve({ status: 0, text: `network:${e.code || e.message}`, elapsed: Date.now() - startedAt }));
    req.write(body);
    req.end();
  });
}

function classifyH2(res) {
  if (res.status === 0) return 'network_error';
  if (res.status >= 200 && res.status < 300 && /"choices"/.test(res.text)) return 'success';
  if (/unpaid invoice|cursor\.com\/dashboard/i.test(res.text)) return 'unpaid_invoice';
  if (/rate.?limit/i.test(res.text)) return 'rate_limit';
  return 'other_error';
}

function classifyH1(res) {
  if (res.status === 0) return 'network_error';
  if (res.status === 464) return 'elb_reject_464';
  if (res.status >= 200 && res.status < 300) {
    // If a 200 ever comes back, parse for unpaid_invoice signal
    if (/unpaid invoice/.test(res.text)) return 'h1_reached_gate_unpaid';
    if (/invalid_argument|Request is empty/.test(res.text)) return 'h1_reached_backend';
    return 'h1_success_unexpected';
  }
  return `h1_other_${res.status}`;
}

function pct(arr, p) {
  if (!arr.length) return 0;
  const sorted = [...arr].sort((a, b) => a - b);
  return sorted[Math.min(sorted.length - 1, Math.floor(p * sorted.length))];
}

// Wilson 95% CI for binomial proportion — much better than naive ± at small n.
function wilson95(successes, n) {
  if (n === 0) return [0, 0];
  const z = 1.96;
  const phat = successes / n;
  const denom = 1 + (z * z) / n;
  const center = (phat + (z * z) / (2 * n)) / denom;
  const margin = (z * Math.sqrt((phat * (1 - phat)) / n + (z * z) / (4 * n * n))) / denom;
  return [Math.max(0, center - margin), Math.min(1, center + margin)];
}

async function runPhase(name, fn, n, classifier) {
  console.log(`\n=== ${name} (${n} attempts, ${INTERVAL_MS}ms interval) ===`);
  const counts = {};
  const latencies = {};
  let successes = 0;
  for (let i = 1; i <= n; i++) {
    const res = await fn();
    const kind = classifier(res);
    counts[kind] = (counts[kind] || 0) + 1;
    (latencies[kind] || (latencies[kind] = [])).push(res.elapsed);
    if (kind === 'success' || kind.startsWith('h1_success')) successes++;
    const stamp = `[${String(i).padStart(3, ' ')}/${n}]`;
    const ms = `${String(res.elapsed).padStart(4, ' ')}ms`;
    const extra = res.server ? ` server=${res.server}` : '';
    if (kind === 'success') {
      console.log(`${stamp} ${ms}  ✅ SUCCESS  status=${res.status}`);
    } else if (kind === 'unpaid_invoice') {
      console.log(`${stamp} ${ms}  unpaid_invoice`);
    } else if (kind === 'elb_reject_464') {
      console.log(`${stamp} ${ms}  464${extra}`);
    } else {
      console.log(`${stamp} ${ms}  ${kind}  status=${res.status}${extra} ${res.text.slice(0, 120)}`);
    }
    if (i < n) await new Promise((r) => setTimeout(r, INTERVAL_MS));
  }
  const [ci_lo, ci_hi] = wilson95(successes, n);
  console.log(`\n${name} results:`);
  console.log(`  successes: ${successes} / ${n}  (${(successes / n * 100).toFixed(2)}%)`);
  console.log(`  Wilson 95% CI: [${(ci_lo * 100).toFixed(2)}%, ${(ci_hi * 100).toFixed(2)}%]`);
  for (const [k, c] of Object.entries(counts)) {
    const lat = latencies[k] || [];
    console.log(`  ${k.padEnd(24)} count=${String(c).padStart(3)}  p50=${pct(lat, 0.5)}ms  p95=${pct(lat, 0.95)}ms`);
  }
  return { name, n, successes, counts, latencies, ci_lo, ci_hi };
}

async function main() {
  console.log('Measurement: HTTP/2-via-proxy  vs  HTTP/1.1-direct');
  console.log(`Model:    ${MODEL}`);
  console.log(`H2 path:  ${PROXY_URL}  (proxy → api2.cursor.sh via http2.connect)`);
  console.log(`H1 path:  https://${CURSOR_HOST}/agent.v1.AgentService/Run  (direct, HTTP/1.1)`);

  const results = {};
  if (!SKIP_H2) results.h2 = await runPhase('HTTP/2 via proxy', attemptH2, MAX_H2, classifyH2);
  if (!SKIP_H1) results.h1 = await runPhase('HTTP/1.1 direct', attemptH1, MAX_H1, classifyH1);

  console.log('\n=== summary ===');
  for (const r of Object.values(results)) {
    console.log(`${r.name}:  ${r.successes}/${r.n}  =  ${(r.successes / r.n * 100).toFixed(2)}%  (95% CI ${(r.ci_lo * 100).toFixed(2)}–${(r.ci_hi * 100).toFixed(2)}%)`);
  }

  // Persist JSON for the survey doc to reference
  const out = {
    model: MODEL,
    intervalMs: INTERVAL_MS,
    timestamp: new Date().toISOString(),
    results: Object.fromEntries(Object.entries(results).map(([k, v]) => [k, {
      n: v.n, successes: v.successes,
      successRate: v.successes / v.n,
      ci95: [v.ci_lo, v.ci_hi],
      counts: v.counts,
      latencyP50: Object.fromEntries(Object.entries(v.latencies).map(([kk, vv]) => [kk, pct(vv, 0.5)])),
      latencyP95: Object.fromEntries(Object.entries(v.latencies).map(([kk, vv]) => [kk, pct(vv, 0.95)])),
    }])),
  };
  const outPath = new URL('./measurement-result.json', import.meta.url);
  fs.writeFileSync(outPath, JSON.stringify(out, null, 2));
  console.log(`\nresult written to ${outPath.pathname}`);
}

main().catch((e) => { console.error('fatal:', e); process.exit(2); });
