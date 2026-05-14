#!/usr/bin/env node
// tap.mjs — transparent HTTP proxy that captures both directions.
//
// Sits between claude-code and the api-server, dumps full request body + full
// SSE response stream to per-request .jsonl files in /tmp/ratlc-tap/.
//
// Usage:
//   TAP_TARGET=http://127.0.0.1:4242 TAP_PORT=4343 node scaffolding/pool/tap.mjs
//   # then point claude-code at the tap:
//   ANTHROPIC_BASE_URL=http://127.0.0.1:4343 claude ...
//
// Each request gets its own file:
//   /tmp/ratlc-tap/<timestamp>-<requestId>.jsonl
//     { ts, dir: 'request_headers', data: { method, url, headers } }
//     { ts, dir: 'request_body', data: <parsed JSON or raw string> }
//     { ts, dir: 'response_headers', data: { status, headers } }
//     { ts, dir: 'sse_event', data: { event, data } }     // for each SSE event
//     { ts, dir: 'response_end' }
//     { ts, dir: 'client_disconnect' }   // if the client bails

import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
import { URL } from 'node:url';

const TAP_PORT = parseInt(process.env.TAP_PORT || '4343', 10);
const TAP_TARGET = process.env.TAP_TARGET || 'http://127.0.0.1:4242';
const CAP_DIR = process.env.TAP_DIR || '/tmp/ratlc-tap';
try { fs.mkdirSync(CAP_DIR, { recursive: true }); } catch {}

const targetUrl = new URL(TAP_TARGET);

function tsfile() {
  const reqId = randomUUID().slice(0, 8);
  const stamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
  return path.join(CAP_DIR, `${stamp}-${reqId}.jsonl`);
}

function append(file, dir, data) {
  try {
    fs.appendFileSync(file, JSON.stringify({ ts: Date.now(), dir, data }) + '\n');
  } catch (e) {
    console.error('append failed:', e.message);
  }
}

const server = http.createServer(async (req, res) => {
  const file = tsfile();
  const startedAt = Date.now();
  console.log(`[tap] ${req.method} ${req.url} → ${file}`);

  // Capture request headers
  append(file, 'request_headers', {
    method: req.method, url: req.url,
    headers: req.headers,
  });

  // Buffer the request body
  const chunks = [];
  for await (const c of req) chunks.push(c);
  const bodyBuf = Buffer.concat(chunks);
  let bodyDecoded;
  try { bodyDecoded = JSON.parse(bodyBuf.toString('utf8')); }
  catch { bodyDecoded = bodyBuf.toString('utf8'); }
  append(file, 'request_body', bodyDecoded);

  // Forward to target
  const opts = {
    hostname: targetUrl.hostname,
    port: targetUrl.port || 80,
    path: req.url,
    method: req.method,
    headers: { ...req.headers, host: targetUrl.host },
  };
  delete opts.headers['content-length']; // recompute
  opts.headers['content-length'] = String(bodyBuf.length);

  const upstreamReq = http.request(opts, (upstreamRes) => {
    append(file, 'response_headers', {
      status: upstreamRes.statusCode,
      headers: upstreamRes.headers,
    });
    res.writeHead(upstreamRes.statusCode, upstreamRes.headers);

    // Tee SSE response: parse and capture each event
    let sseBuf = '';
    upstreamRes.on('data', (chunk) => {
      res.write(chunk);
      sseBuf += chunk.toString('utf8');
      // Parse complete SSE events (delimited by \n\n)
      let idx;
      while ((idx = sseBuf.indexOf('\n\n')) !== -1) {
        const eventBlock = sseBuf.slice(0, idx);
        sseBuf = sseBuf.slice(idx + 2);
        const lines = eventBlock.split('\n');
        const evt = {};
        for (const ln of lines) {
          if (ln.startsWith('event: ')) evt.event = ln.slice(7).trim();
          else if (ln.startsWith('data: ')) {
            const raw = ln.slice(6);
            try { evt.data = JSON.parse(raw); } catch { evt.data = raw; }
          }
        }
        if (evt.event || evt.data !== undefined) {
          append(file, 'sse_event', evt);
        }
      }
    });
    upstreamRes.on('end', () => {
      append(file, 'response_end', { totalMs: Date.now() - startedAt });
      try { res.end(); } catch {}
    });
    upstreamRes.on('error', (e) => {
      append(file, 'response_error', { error: e.message });
      try { res.end(); } catch {}
    });
  });
  upstreamReq.on('error', (e) => {
    append(file, 'upstream_error', { error: e.message });
    if (!res.writableEnded) { res.writeHead(502); res.end(); }
  });
  upstreamReq.write(bodyBuf);
  upstreamReq.end();

  // Detect client disconnect
  req.on('close', () => {
    if (!res.writableEnded) {
      append(file, 'client_disconnect', { afterMs: Date.now() - startedAt });
    }
  });
});

server.listen(TAP_PORT, '127.0.0.1', () => {
  console.log(`[tap] listening on http://127.0.0.1:${TAP_PORT}, forwarding to ${TAP_TARGET}`);
  console.log(`[tap] captures: ${CAP_DIR}/<timestamp>-<reqid>.jsonl`);
});

process.on('SIGINT', () => { server.close(); process.exit(0); });
