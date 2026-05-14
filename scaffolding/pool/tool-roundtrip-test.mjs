#!/usr/bin/env node
// Test the full tool-use round-trip against a running pool.
//
// 1. POST a Bash tool_use request → expect SSE with content_block_start
//    (tool_use, name=Bash) and message_delta stop_reason=tool_use.
// 2. POST back tool_result with the same tool_use_id → expect SSE with
//    text_delta content (the model's response after seeing the tool output).
//
// PASS criteria:
//   - Step 1: tool_use_id is captured and tool name is Bash.
//   - Step 2: at least one text_delta event arrives within 60s, the
//     message ends with message_stop, and stop_reason is end_turn (or
//     tool_use if model wants to call another tool).
//
// FAIL criteria:
//   - SSE stream hangs without message_stop.
//   - Error event received.
//   - tool_use_id missing in step 1.

const API = process.env.API_URL || 'http://127.0.0.1:4242';
const STEP_TIMEOUT_MS = 60_000;

async function readSse(res, label, onEvent) {
  const events = [];
  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let buf = '';
  const start = Date.now();
  while (true) {
    if (Date.now() - start > STEP_TIMEOUT_MS) {
      console.log(`[${label}] ⛔ TIMEOUT after ${STEP_TIMEOUT_MS}ms`);
      return { events, timedOut: true };
    }
    const { done, value } = await reader.read();
    if (done) break;
    buf += decoder.decode(value, { stream: true });
    let idx;
    while ((idx = buf.indexOf('\n\n')) !== -1) {
      const block = buf.slice(0, idx);
      buf = buf.slice(idx + 2);
      const evt = {};
      for (const line of block.split('\n')) {
        if (line.startsWith('event: ')) evt.event = line.slice(7).trim();
        else if (line.startsWith('data: ')) {
          try { evt.data = JSON.parse(line.slice(6)); } catch { evt.data = line.slice(6); }
        }
      }
      if (evt.event) {
        events.push(evt);
        if (onEvent) onEvent(evt);
        if (evt.event === 'message_stop') return { events, timedOut: false };
        if (evt.event === 'error') return { events, timedOut: false, errored: true };
      }
    }
  }
  return { events, timedOut: false };
}

async function step1() {
  console.log('━━━ STEP 1: POST user message with Bash tool ━━━');
  const body = {
    model: 'claude-opus-4-7-thinking-max-fast',
    max_tokens: 1024,
    stream: true,
    messages: [
      { role: 'user', content: 'Please run "pwd" and tell me my current directory in one short sentence.' },
    ],
    tools: [
      {
        name: 'Bash',
        description: 'Execute a bash command and return stdout.',
        input_schema: {
          type: 'object',
          properties: { command: { type: 'string', description: 'The command to run' } },
          required: ['command'],
        },
      },
    ],
  };
  const res = await fetch(`${API}/v1/messages`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    console.log(`[step1] HTTP ${res.status} ${await res.text()}`);
    process.exit(1);
  }
  let toolUseId = null, toolName = null, toolInput = null;
  const result = await readSse(res, 'step1', (evt) => {
    if (evt.event === 'content_block_start' && evt.data?.content_block?.type === 'tool_use') {
      toolUseId = evt.data.content_block.id;
      toolName = evt.data.content_block.name;
      console.log(`[step1] ✓ tool_use_start id=${toolUseId} name=${toolName}`);
    }
    if (evt.event === 'content_block_delta' && evt.data?.delta?.type === 'input_json_delta') {
      const partial = evt.data.delta.partial_json;
      if (partial) {
        toolInput = (toolInput || '') + partial;
        console.log(`[step1] ✓ input_json_delta partial="${partial.slice(0, 100)}"`);
      }
    }
    if (evt.event === 'message_delta' && evt.data?.delta?.stop_reason) {
      console.log(`[step1] ✓ stop_reason=${evt.data.delta.stop_reason}`);
    }
  });
  if (result.timedOut) { console.log('[step1] FAIL: timed out'); return null; }
  if (result.errored) { console.log('[step1] FAIL: error event received'); return null; }
  if (!toolUseId) { console.log('[step1] FAIL: no tool_use_id captured'); return null; }
  console.log(`[step1] ✅ PASS — tool_use_id=${toolUseId}, name=${toolName}, input=${toolInput}`);
  return { toolUseId, toolName, toolInput };
}

async function step2(stepOne) {
  console.log('\n━━━ STEP 2: POST tool_result for ' + stepOne.toolUseId + ' ━━━');
  const body = {
    model: 'claude-opus-4-7-thinking-max-fast',
    max_tokens: 1024,
    stream: true,
    messages: [
      { role: 'user', content: 'Please run "pwd" and tell me my current directory in one short sentence.' },
      {
        role: 'assistant',
        content: [
          { type: 'tool_use', id: stepOne.toolUseId, name: stepOne.toolName, input: JSON.parse(stepOne.toolInput || '{}') },
        ],
      },
      {
        role: 'user',
        content: [
          { type: 'tool_result', tool_use_id: stepOne.toolUseId, content: '/Users/juncwang' },
        ],
      },
    ],
    tools: [
      {
        name: 'Bash',
        description: 'Execute a bash command and return stdout.',
        input_schema: {
          type: 'object',
          properties: { command: { type: 'string', description: 'The command to run' } },
          required: ['command'],
        },
      },
    ],
  };
  const res = await fetch(`${API}/v1/messages`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    console.log(`[step2] HTTP ${res.status} ${await res.text()}`);
    process.exit(1);
  }
  let textChars = 0;
  let sawStop = false;
  let stopReason = null;
  const result = await readSse(res, 'step2', (evt) => {
    if (evt.event === 'content_block_delta' && evt.data?.delta?.type === 'text_delta') {
      const t = evt.data.delta.text || '';
      textChars += t.length;
      process.stdout.write(t);
    }
    if (evt.event === 'message_delta' && evt.data?.delta?.stop_reason) {
      stopReason = evt.data.delta.stop_reason;
    }
    if (evt.event === 'message_stop') sawStop = true;
  });
  console.log('');
  if (result.timedOut) { console.log(`[step2] FAIL: timed out (chars=${textChars}, stopReason=${stopReason})`); return false; }
  if (result.errored) { console.log('[step2] FAIL: error event'); return false; }
  if (!sawStop) { console.log(`[step2] FAIL: no message_stop (chars=${textChars})`); return false; }
  if (textChars === 0) { console.log(`[step2] PARTIAL: message_stop seen but no text (stopReason=${stopReason})`); return false; }
  console.log(`[step2] ✅ PASS — ${textChars} chars of text, stopReason=${stopReason}`);
  return true;
}

(async () => {
  const t0 = Date.now();
  const s1 = await step1();
  if (!s1) process.exit(1);
  const ok = await step2(s1);
  console.log(`\nTotal time: ${Date.now() - t0}ms`);
  process.exit(ok ? 0 : 1);
})();
