import fs from 'node:fs/promises';
import path from 'node:path';
import os from 'node:os';
import { createHash } from 'node:crypto';

const STORE_DIR = process.env.RATLC_CONTEXT_STORE_DIR || path.join(os.tmpdir(), 'ratlc-context-store');
const CHUNK_BYTES = Math.max(4096, parseInt(process.env.RATLC_CONTEXT_CHUNK_BYTES || '65536', 10));
const MAX_READ_BYTES = Math.max(1024, parseInt(process.env.RATLC_CONTEXT_READ_MAX_BYTES || '131072', 10));
const MAX_SEARCH_MATCHES = Math.max(1, parseInt(process.env.RATLC_CONTEXT_SEARCH_MAX_MATCHES || '50', 10));

function sha256(text) {
  return createHash('sha256').update(String(text || '')).digest('hex');
}

function safeId(id) {
  const s = String(id || '');
  if (!/^[a-f0-9]{16,64}$/i.test(s)) throw new Error('invalid snapshotId');
  return s.toLowerCase();
}

function snapshotDir(snapshotId) {
  return path.join(STORE_DIR, safeId(snapshotId));
}

async function ensureStoreDir() {
  await fs.mkdir(STORE_DIR, { recursive: true });
}

function byteSliceUtf8(text, offset, limit) {
  const buf = Buffer.from(String(text || ''), 'utf8');
  const start = Math.max(0, Math.min(buf.length, Number(offset) || 0));
  const len = Math.max(0, Math.min(MAX_READ_BYTES, Number(limit) || MAX_READ_BYTES));
  return {
    text: buf.subarray(start, Math.min(buf.length, start + len)).toString('utf8'),
    offset: start,
    nextOffset: Math.min(buf.length, start + len),
    totalBytes: buf.length,
  };
}

function renderMessageBody(content, renderContentBlocks) {
  if (typeof content === 'string') return content;
  if (Array.isArray(content) && typeof renderContentBlocks === 'function') return renderContentBlocks(content);
  try { return JSON.stringify(content ?? ''); }
  catch { return String(content ?? ''); }
}

export function shouldUseContextManifest({ mode, textBytes, imageCount, threshold }) {
  return mode === 'full'
    && imageCount === 0
    && threshold > 0
    && textBytes > threshold;
}

export async function createContextSnapshot({ text, messages, system, tools, finalUserText, renderContentBlocks }) {
  await ensureStoreDir();
  const rawText = String(text || '');
  const snapshotId = sha256(rawText);
  const dir = snapshotDir(snapshotId);
  await fs.mkdir(dir, { recursive: true });

  const chunks = [];
  const buf = Buffer.from(rawText, 'utf8');
  for (let offset = 0, idx = 0; offset < buf.length; offset += CHUNK_BYTES, idx++) {
    const chunkBuf = buf.subarray(offset, Math.min(buf.length, offset + CHUNK_BYTES));
    const chunkText = chunkBuf.toString('utf8');
    const chunkId = String(idx).padStart(4, '0');
    chunks.push({
      id: chunkId,
      offset,
      bytes: chunkBuf.length,
      sha256: sha256(chunkText),
    });
    await fs.writeFile(path.join(dir, `${chunkId}.txt`), chunkText, 'utf8');
  }
  await fs.writeFile(path.join(dir, 'full.txt'), rawText, 'utf8');

  const messageIndex = [];
  const arr = Array.isArray(messages) ? messages : [];
  for (let i = 0; i < arr.length; i++) {
    const m = arr[i];
    if (!m || !m.role) continue;
    const body = renderMessageBody(m.content, renderContentBlocks);
    messageIndex.push({
      index: i,
      role: m.role,
      bytes: Buffer.byteLength(body, 'utf8'),
      preview: body.slice(0, 500),
      text: body,
    });
  }

  const manifest = {
    type: 'ratlc_context_manifest',
    version: 1,
    snapshotId,
    createdAt: new Date().toISOString(),
    mode: 'full',
    totalBytes: buf.length,
    chunkBytes: CHUNK_BYTES,
    messageCount: arr.length,
    toolCount: Array.isArray(tools) ? tools.length : 0,
    systemBytes: Buffer.byteLength(typeof system === 'string' ? system : JSON.stringify(system || ''), 'utf8'),
    finalUserTurn: String(finalUserText || '').slice(0, 4000),
    chunks,
  };

  await fs.writeFile(path.join(dir, 'manifest.json'), JSON.stringify(manifest, null, 2), 'utf8');
  await fs.writeFile(path.join(dir, 'messages.json'), JSON.stringify(messageIndex, null, 2), 'utf8');
  return manifest;
}

export function renderContextManifestPrompt(manifest) {
  const chunks = (manifest.chunks || []).slice(0, 12).map((c) =>
    `- ${c.id}: offset=${c.offset} bytes=${c.bytes} sha256=${c.sha256}`
  ).join('\n');
  const omitted = (manifest.chunks || []).length > 12
    ? `\n- ... ${(manifest.chunks || []).length - 12} more chunk(s)`
    : '';
  return [
    '=== RATLC CONTEXT MANIFEST ===',
    'The complete conversation history for this request was stored outside the prompt to avoid oversized Cursor tool_result stalls.',
    'This preserves full-context semantics: use the ratlc_context_* tools whenever details from prior turns, tool results, or the full transcript matter.',
    '',
    `snapshotId: ${manifest.snapshotId}`,
    `messageCount: ${manifest.messageCount}`,
    `totalBytes: ${manifest.totalBytes}`,
    `chunkBytes: ${manifest.chunkBytes}`,
    '',
    'chunks:',
    chunks + omitted,
    '',
    'Final user turn:',
    manifest.finalUserTurn || '(empty)',
    '',
    'Available retrieval tools:',
    '- ratlc_context_read({ snapshotId, offset?, limit?, chunkId? })',
    '- ratlc_context_search({ snapshotId, query, maxMatches? })',
    '- ratlc_context_get_message({ snapshotId, index })',
    '- ratlc_context_get_tool_result({ snapshotId, tool_use_id })',
    '',
    'Respond to the final user turn. If the answer depends on omitted prior context, read or search the snapshot first. Then call bajie_yield to wait for the next request.',
  ].join('\n');
}

async function readManifest(snapshotId) {
  const dir = snapshotDir(snapshotId);
  const raw = await fs.readFile(path.join(dir, 'manifest.json'), 'utf8');
  return JSON.parse(raw);
}

async function readMessages(snapshotId) {
  const dir = snapshotDir(snapshotId);
  const raw = await fs.readFile(path.join(dir, 'messages.json'), 'utf8');
  return JSON.parse(raw);
}

async function readFullText(snapshotId) {
  const dir = snapshotDir(snapshotId);
  return fs.readFile(path.join(dir, 'full.txt'), 'utf8');
}

function normalizeContextToolArgs(args = {}) {
  if (!args || typeof args !== 'object') return {};
  return args;
}

export function isContextToolName(name) {
  return [
    'ratlc_context_read',
    'ratlc_context_search',
    'ratlc_context_get_message',
    'ratlc_context_get_tool_result',
  ].includes(String(name || ''));
}

export async function runContextTool(name, rawArgs = {}) {
  const args = normalizeContextToolArgs(rawArgs);
  const snapshotId = safeId(args.snapshotId || args.snapshot_id || args.id);
  if (name === 'ratlc_context_read') {
    const manifest = await readManifest(snapshotId);
    if (args.chunkId || args.chunk_id) {
      const chunkId = String(args.chunkId || args.chunk_id).padStart(4, '0');
      const chunk = (manifest.chunks || []).find((c) => c.id === chunkId);
      if (!chunk) throw new Error(`chunk not found: ${chunkId}`);
      const text = await fs.readFile(path.join(snapshotDir(snapshotId), `${chunkId}.txt`), 'utf8');
      return JSON.stringify({ snapshotId, chunkId, offset: chunk.offset, bytes: chunk.bytes, text }, null, 2);
    }
    const text = await readFullText(snapshotId);
    const sliced = byteSliceUtf8(text, args.offset, args.limit);
    return JSON.stringify({ snapshotId, ...sliced }, null, 2);
  }
  if (name === 'ratlc_context_search') {
    const query = String(args.query || args.pattern || '');
    if (!query) throw new Error('query required');
    const maxMatches = Math.max(1, Math.min(MAX_SEARCH_MATCHES, parseInt(args.maxMatches || args.max_matches || '20', 10)));
    const text = await readFullText(snapshotId);
    const lower = text.toLowerCase();
    const needle = query.toLowerCase();
    const matches = [];
    let from = 0;
    while (matches.length < maxMatches) {
      const idx = lower.indexOf(needle, from);
      if (idx === -1) break;
      const start = Math.max(0, idx - 240);
      const end = Math.min(text.length, idx + query.length + 240);
      matches.push({ offset: Buffer.byteLength(text.slice(0, idx), 'utf8'), preview: text.slice(start, end) });
      from = idx + Math.max(1, query.length);
    }
    return JSON.stringify({ snapshotId, query, matches }, null, 2);
  }
  if (name === 'ratlc_context_get_message') {
    const index = parseInt(args.index ?? args.messageIndex ?? args.message_index, 10);
    if (!Number.isFinite(index)) throw new Error('index required');
    const messages = await readMessages(snapshotId);
    const msg = messages.find((m) => m.index === index);
    if (!msg) throw new Error(`message not found: ${index}`);
    return JSON.stringify(msg, null, 2);
  }
  if (name === 'ratlc_context_get_tool_result') {
    const toolUseId = String(args.tool_use_id || args.toolUseId || args.id || '');
    if (!toolUseId) throw new Error('tool_use_id required');
    const text = await readFullText(snapshotId);
    const re = new RegExp(`<tool_result tool_use_id="${toolUseId.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}"[^>]*>\\n([\\s\\S]*?)\\n<\\/tool_result>`, 'm');
    const match = text.match(re);
    if (!match) throw new Error(`tool_result not found: ${toolUseId}`);
    return JSON.stringify({ snapshotId, tool_use_id: toolUseId, text: match[1] }, null, 2);
  }
  throw new Error(`unsupported context tool: ${name}`);
}

export function contextToolDefinitions() {
  return [
    {
      name: 'ratlc_context_read',
      description: 'Read bytes from a RATLC full-context snapshot. Use when the manifest omits prior conversation details needed to answer.',
      input_schema: {
        type: 'object',
        properties: {
          snapshotId: { type: 'string' },
          offset: { type: 'number' },
          limit: { type: 'number' },
          chunkId: { type: 'string' },
        },
        required: ['snapshotId'],
      },
    },
    {
      name: 'ratlc_context_search',
      description: 'Search a RATLC full-context snapshot for text and return matching offsets with surrounding previews.',
      input_schema: {
        type: 'object',
        properties: {
          snapshotId: { type: 'string' },
          query: { type: 'string' },
          maxMatches: { type: 'number' },
        },
        required: ['snapshotId', 'query'],
      },
    },
    {
      name: 'ratlc_context_get_message',
      description: 'Fetch one message from a RATLC full-context snapshot by zero-based message index.',
      input_schema: {
        type: 'object',
        properties: {
          snapshotId: { type: 'string' },
          index: { type: 'number' },
        },
        required: ['snapshotId', 'index'],
      },
    },
    {
      name: 'ratlc_context_get_tool_result',
      description: 'Fetch a prior tool_result from a RATLC full-context snapshot by tool_use_id.',
      input_schema: {
        type: 'object',
        properties: {
          snapshotId: { type: 'string' },
          tool_use_id: { type: 'string' },
        },
        required: ['snapshotId', 'tool_use_id'],
      },
    },
  ];
}
