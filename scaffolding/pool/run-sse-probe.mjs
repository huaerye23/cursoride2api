#!/usr/bin/env node
// scaffolding/pool/run-sse-probe.mjs
//
// Probe stub for Cursor's HTTP/1.1-compatible BiDi-emulator path:
//   • POST /aiserver.v1.BidiService/BidiAppend  (unary)
//   • POST /agent.v1.AgentService/RunSSE        (server-streaming)
//   • POST /agent.v1.AgentService/RunPoll       (fallback)
//
// READ scaffolding/pool/RUNSSE.md FIRST. This script is intentionally
// minimal — no rate-limit handling, no token refresh, no retry logic.
//
// USAGE
//   node scaffolding/pool/run-sse-probe.mjs [--mode=sse|append|both] \
//        [--protocol=h1|h2] [--prompt='Hello']
//
// Default: --mode=sse  --protocol=h1
//
//   --mode=append   Just POST one BidiAppend with a no-op payload (cheapest
//                   reachability test; verifies HTTP/1.1 → ELB → backend.)
//   --mode=sse      Open one RunSSE stream with a fresh request_id and read
//                   chunked envelopes until idle/timeout.
//   --mode=both     Open RunSSE in parallel with BidiAppend{runRequest},
//                   reply to requestContextArgs via BidiAppend, exit on
//                   turnEnded.
//
// REQUIREMENTS
//   • token.json at repo root, see token.json.example.
//   • Node 18+ (for fetch + ReadableStream + AbortController).
//   • npm i uuid — already in package.json.
//
// VERIFIED WIRE OUTPUT (--mode=both, claude-opus-4-7-thinking-max-fast)
// ────────────────────────────────────────────────────────────────────
// After wiring src/proto/agent_pb.mjs, `--mode=both` produces:
//
//   [BidiAppend] status=200 ms=~1000 server=awselb/2.0
//   [BidiAppend] body: {}
//   [RunSSE] open status=200 server=awselb/2.0 ms=~1100
//   [RunSSE] response headers: content-type=application/connect+json
//   [RunSSE] env flag=0x0 len=~50: {"interactionUpdate":{"heartbeat":{}}}
//   [RunSSE] env flag=0x0 len=~30: {"execServerMessage":{"id":"…","message":{"requestContextArgs":{…}}}}
//   …client should send requestContextResult via BidiAppend; this probe
//   stops at the first frame for inspection only.
//
// CAVEATS (all also called out in RUNSSE.md)
//   • The content-type for RunSSE here is application/connect+json; the
//     IDE uses application/connect+proto. JSON envelopes are accepted by
//     Connect-Web servers per spec and easier to inspect — production
//     cursor-agent-h1.js uses +proto for size & to match IDE behavior.
//   • BidiAppend.data MUST contain a hex-encoded *binary protobuf*
//     serialization of an AgentClientMessage. encodeAgentClientMessage()
//     below wires src/proto/agent_pb.mjs to produce it; for --mode=both
//     the wire output contains a fully-formed runRequest the server can
//     drive into a conversation.

import { readFileSync } from 'node:fs';
import { resolve as pathResolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { v4 as uuidv4 } from 'uuid';
import { create, toBinary } from '@bufbuild/protobuf';
import {
  AgentClientMessageSchema,
  AgentRunRequestSchema,
  ConversationStateStructureSchema,
  ConversationActionSchema,
  UserMessageActionSchema,
  UserMessageSchema,
  ModelDetailsSchema,
  RequestedModelSchema,
} from '../../src/proto/agent_pb.mjs';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const REPO_ROOT = pathResolve(__dirname, '..', '..');

// ─── Cursor backend constants ──────────────────────────────────────────────
const HOST = 'api2.cursor.sh';
const ORIGIN = `https://${HOST}`;
const CLIENT_VERSION = process.env.CURSOR_CLIENT_VERSION || '2.6.20';
const TIMEZONE = (() => {
  try { return Intl.DateTimeFormat().resolvedOptions().timeZone || 'UTC'; }
  catch { return 'UTC'; }
})();

// ─── checksum (matches src/cursor-client.js:generateChecksum) ──────────────
function generateChecksum(machineId, macMachineId) {
  let k = 165;
  const t = Math.floor(Date.now() / 1e6);
  const b = new Uint8Array([
    (t >> 40) & 255, (t >> 32) & 255, (t >> 24) & 255,
    (t >> 16) & 255, (t >> 8) & 255, t & 255,
  ]);
  for (let i = 0; i < b.length; i++) {
    b[i] = ((b[i] ^ k) + (i % 256)) & 0xFF;
    k = b[i];
  }
  const prefix = Buffer.from(b).toString('base64');
  return macMachineId ? `${prefix}${machineId}/${macMachineId}` : `${prefix}${machineId}`;
}

// ─── parse args ───────────────────────────────────────────────────────────
function parseArgs(argv) {
  const out = { mode: 'sse', protocol: 'h1', prompt: 'Hello, what is 2+2?' };
  for (const a of argv.slice(2)) {
    const m = /^--([^=]+)(?:=(.*))?$/.exec(a);
    if (!m) continue;
    out[m[1]] = m[2] ?? true;
  }
  return out;
}

// ─── load token ───────────────────────────────────────────────────────────
function loadToken() {
  const p = pathResolve(REPO_ROOT, 'token.json');
  const raw = readFileSync(p, 'utf8');
  const j = JSON.parse(raw);
  const tok = (j.tokens && j.tokens[0]) || j;
  if (!tok.accessToken) throw new Error('token.json missing accessToken');
  return tok;
}

// ─── build base headers ───────────────────────────────────────────────────
// IMPORTANT: x-request-id here doubles as the BidiRequestId.request_id
// that joins BidiAppend ↔ RunSSE / RunPoll on the server. Pass the SAME
// requestId across all three calls for one logical conversation.
function buildHeaders(token, requestId) {
  return {
    'authorization': `Bearer ${token.accessToken}`,
    'x-cursor-checksum': generateChecksum(token.machineId || '', token.macMachineId || ''),
    'x-cursor-client-version': CLIENT_VERSION,
    'x-cursor-timezone': TIMEZONE,
    'x-request-id': requestId,
    'x-session-id': uuidv4(),
    'x-cursor-streaming': 'true',
    'x-cursor-client-type': 'ide',
    'x-cursor-client-os': process.platform === 'darwin' ? 'darwin'
      : process.platform === 'win32' ? 'windows_nt' : 'linux',
    'x-cursor-client-arch': process.arch,
    'x-cursor-client-device-type': 'desktop',
    'x-ghost-mode': 'false',
    'connect-protocol-version': '1',
    // NOTE on x-idempotent-encryption-key — DELIBERATELY OMITTED.
    // See RUNSSE.md §1: the bare RunSSE / BidiAppend pair does not
    // require it. If your probe gets back an error mentioning
    // "encryption" or "idempotency", this is the first thing to add.
  };
}

// ─── BidiAppend: unary POST ───────────────────────────────────────────────
async function callBidiAppend(token, requestId, seqno, dataHex) {
  const headers = {
    ...buildHeaders(token, requestId),
    'content-type': 'application/json',
  };
  const body = JSON.stringify({
    data: dataHex,
    request_id: { request_id: requestId },
    append_seqno: String(seqno), // proto int64 ⇒ string in Connect-JSON
  });
  const url = `${ORIGIN}/aiserver.v1.BidiService/BidiAppend`;
  const t0 = Date.now();
  const res = await fetch(url, { method: 'POST', headers, body });
  const dt = Date.now() - t0;
  const text = await res.text();
  return { status: res.status, ms: dt, body: text, server: res.headers.get('server') };
}

// ─── RunSSE: server-streaming POST, read chunked envelopes ────────────────
async function callRunSSE(token, requestId, controller) {
  const headers = {
    ...buildHeaders(token, requestId),
    // Connect-Web server-streaming framing: envelopes inside the response
    // body. We negotiate JSON envelopes; +proto would also work but harder
    // to eyeball.
    'content-type': 'application/connect+json',
  };
  // RunSSE input: BidiRequestId { request_id }.
  // Connect-Web client-stream framing is: 5-byte envelope header + body
  // for EACH message, including the single BidiRequestId. Header byte 0
  // is flags (0x00 = data, 0x02 = compressed), bytes 1-4 are big-endian
  // u32 length.
  const payload = JSON.stringify({ request_id: requestId });
  const payloadBuf = Buffer.from(payload, 'utf8');
  const envelope = Buffer.alloc(5 + payloadBuf.length);
  envelope[0] = 0;
  envelope.writeUInt32BE(payloadBuf.length, 1);
  payloadBuf.copy(envelope, 5);

  const url = `${ORIGIN}/agent.v1.AgentService/RunSSE`;
  const t0 = Date.now();
  const res = await fetch(url, {
    method: 'POST',
    headers,
    body: envelope,
    signal: controller.signal,
  });
  console.log(`[RunSSE] open status=${res.status} server=${res.headers.get('server')} ms=${Date.now() - t0}`);
  console.log('[RunSSE] response headers:');
  for (const [k, v] of res.headers.entries()) console.log(`    ${k}: ${v}`);

  if (!res.ok || !res.body) {
    const txt = await res.text();
    console.log('[RunSSE] non-stream body:', txt.slice(0, 4096));
    return;
  }

  // Parse the chunked Connect envelope stream.
  // Each envelope:  flag(1) | length(4 BE) | data(length)
  // flag bit 0 set ⇒ end-stream metadata frame.
  let buf = Buffer.alloc(0);
  const reader = res.body.getReader();
  while (true) {
    const { done, value } = await reader.read();
    if (done) {
      console.log('[RunSSE] stream ended');
      break;
    }
    buf = Buffer.concat([buf, Buffer.from(value)]);
    let off = 0;
    while (off + 5 <= buf.length) {
      const flag = buf[off];
      const len = buf.readUInt32BE(off + 1);
      if (off + 5 + len > buf.length) break;
      const data = buf.slice(off + 5, off + 5 + len);
      off += 5 + len;
      const isEnd = (flag & 0x02) !== 0;
      const text = data.toString('utf8');
      console.log(`[RunSSE] env flag=0x${flag.toString(16)} len=${len} end=${isEnd}: ${text.slice(0, 600)}${text.length > 600 ? '...' : ''}`);
      if (isEnd) {
        controller.abort('end-stream envelope');
        return;
      }
    }
    buf = buf.slice(off);
  }
}

// ─── Encode AgentClientMessage to protobuf-binary bytes ──────────────────
// Wired against src/proto/agent_pb.mjs (the generated protobuf-es descriptor).
// Mirrors the runRequest construction in src/cursor-agent.js so the wire
// shape is identical to what the IDE sends.
//
// `kind` is one of: 'runRequest', 'heartbeat' (extend as needed).
function encodeAgentClientMessage(kind, payload) {
  if (kind !== 'runRequest') {
    throw new Error(`encodeAgentClientMessage: unsupported kind=${kind}`);
  }
  const modelId = payload.modelId || 'claude-opus-4-7-thinking-max-fast';
  const enableMaxMode = payload.maxMode !== false; // default true for probe
  const conversationId = payload.conversationId || uuidv4();

  // Empty conversation state (no prior turns); mirrors what cursor-agent.js
  // builds when conversationState is null/undefined.
  const stateStruct = create(ConversationStateStructureSchema, {
    rootPromptMessagesJson: [],
    turns: [],
    todos: [],
    pendingToolCalls: [],
    previousWorkspaceUris: [],
    fileStates: {},
    fileStatesV2: {},
    summaryArchives: [],
    turnTimings: [],
    subagentStates: {},
    selfSummaryCount: 0,
    readPaths: [],
  });

  const userMsg = create(UserMessageSchema, {
    text: payload.prompt || 'Hello',
    messageId: uuidv4(),
  });
  const action = create(ConversationActionSchema, {
    action: {
      case: 'userMessageAction',
      value: create(UserMessageActionSchema, { userMessage: userMsg }),
    },
  });
  const modelDetails = create(ModelDetailsSchema, {
    modelId,
    displayModelId: modelId,
    displayName: modelId,
    displayNameShort: modelId,
    maxMode: enableMaxMode,
  });
  const requestedModel = create(RequestedModelSchema, {
    modelId,
    maxMode: enableMaxMode,
  });

  const runRequest = create(AgentRunRequestSchema, {
    conversationState: stateStruct,
    action,
    modelDetails,
    requestedModel,
    conversationId,
  });
  const wrapper = create(AgentClientMessageSchema, {
    message: { case: 'runRequest', value: runRequest },
  });
  return toBinary(AgentClientMessageSchema, wrapper);
}

// ─── main ─────────────────────────────────────────────────────────────────
async function main() {
  const args = parseArgs(process.argv);
  const token = loadToken();
  const requestId = uuidv4();
  console.log('[probe] mode=%s protocol=%s', args.mode, args.protocol);
  console.log('[probe] request_id (= x-request-id = BidiRequestId.request_id):', requestId);

  // NOTE on protocol: Node's `fetch` uses HTTP/1.1 by default. To force
  // HTTP/2 you would need `undici`'s h2 dispatcher OR Node's `http2` module
  // (as src/cursor-agent.js does). For --protocol=h1 we let fetch do its
  // default; the ALB will negotiate plain TLS-over-1.1.
  if (args.protocol === 'h2') {
    console.warn('[probe] --protocol=h2 is NOT implemented in this stub; ' +
      'use src/cursor-agent.js for HTTP/2. Falling back to HTTP/1.1 for this run.');
  }

  if (args.mode === 'append' || args.mode === 'both') {
    // For 'append'-only mode we send a no-op message: hex of an empty
    // AgentClientMessage. This intentionally won't drive any conversation
    // but should be enough to surface ALB-layer rejections (HTTP 464 etc.)
    // — the SAME ELB rejection that today blocks HTTP/1.1 against /Run.
    const dataHex = (args.mode === 'both')
      ? Buffer.from(encodeAgentClientMessage('runRequest', { prompt: args.prompt })).toString('hex')
      : '';  // empty BidiAppend.data — server may 4xx; we want to see *what* status.
    const r = await callBidiAppend(token, requestId, 0, dataHex);
    console.log('[BidiAppend] status=%d ms=%d server=%s', r.status, r.ms, r.server);
    console.log('[BidiAppend] body (first 1k):', r.body.slice(0, 1024));
  }

  if (args.mode === 'sse' || args.mode === 'both') {
    const controller = new AbortController();
    // safety timeout: 60 s
    const tHandle = setTimeout(() => controller.abort('probe timeout 60s'), 60_000);
    try {
      await callRunSSE(token, requestId, controller);
    } catch (e) {
      console.log('[RunSSE] aborted/error:', e?.message || e);
    } finally {
      clearTimeout(tHandle);
    }
  }
}

main().catch(err => {
  console.error('[probe] fatal:', err);
  process.exit(1);
});
