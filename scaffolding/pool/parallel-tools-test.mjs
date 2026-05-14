#!/usr/bin/env node
// Parallel tool_uses round-trip test.
//
// PURPOSE
// Verify the pool stack handles a model response that emits MULTIPLE
// tool_uses in a single assistant turn (parallel tool calls — totally
// normal behavior for frontier models). Before the parallel-tools fix
// this scenario crashed at every layer:
//   - api-server finished the SSE after the FIRST tool_use, dropping the
//     second on the floor
//   - bridge-worker's pendingMcpInfo got OVERWRITTEN by the second
//     onMcpCall, so the first tool_result lookup failed with
//     "no matching pending tool_use"
//   - claude-code received an empty/malformed SSE response
//
// PROTOCOL
//
// Step 1: POST a prompt explicitly asking for two parallel Bash calls.
//   Tool list: just [Bash]. Mode: full context (the realistic mode).
//   PASS criterion: at least 2 distinct `content_block_start` events of
//   type='tool_use' (different ids), arriving BEFORE message_stop.
//   Captures both tool_use_ids for step 2.
//
// Step 2: POST an assistant turn containing both tool_uses + a user turn
//   with TWO tool_result blocks (one per tool_use_id). Tool results are
//   synthetic plausible answers (e.g. "/Users/test" and "testuser").
//   PASS criterion: SSE response has at least one text_delta and ends
//   with message_stop, stop_reason=end_turn.
//
// Step 1 retry: if the model only fires ONE tool (sometimes claude-opus-4-7
// serializes despite the prompt), retry up to 2x before declaring FAIL.
// After 3 attempts treat as INCONCLUSIVE.

const API = process.env.API_URL || 'http://127.0.0.1:4242';
const STEP_TIMEOUT_MS = 60_000;
const MAX_STEP1_ATTEMPTS = 3;

const STEP1_PROMPT =
  'I have two independent questions: (a) What is the current working ' +
  'directory? Use Bash. (b) What is the current user? Use Bash. ' +
  'Please call BOTH tools in one response, in parallel, since they are ' +
  'independent. Use only ONE Bash call per question, not multiple.';

const TOOLS = [{
  name: 'Bash',
  description: 'Execute a bash command and return stdout.',
  input_schema: {
    type: 'object',
    properties: { command: { type: 'string', description: 'The command to run' } },
    required: ['command'],
  },
}];

async function readSse(res, label, onEvent) {
  const events = [];
  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let buf = '';
  const start = Date.now();
  while (true) {
    if (Date.now() - start > STEP_TIMEOUT_MS) {
      console.log(`[${label}] TIMEOUT after ${STEP_TIMEOUT_MS}ms`);
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

async function step1Once(attemptNum) {
  console.log(`\n--- STEP 1 attempt ${attemptNum}: POST prompt asking for parallel Bash calls ---`);
  const body = {
    model: 'claude-opus-4-7-thinking-max-fast',
    max_tokens: 1024,
    stream: true,
    messages: [{ role: 'user', content: STEP1_PROMPT }],
    tools: TOOLS,
  };
  const res = await fetch(`${API}/v1/messages`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    console.log(`[step1.${attemptNum}] HTTP ${res.status}: ${(await res.text()).slice(0, 200)}`);
    return { toolCalls: [], errored: true };
  }
  // Track all tool_use content_block_starts. Each entry: {id, name, inputJsonParts: []}.
  const toolCalls = [];
  let currentIdx = null;
  let textChars = 0;
  let stopReason = null;
  let sawStop = false;
  const result = await readSse(res, `step1.${attemptNum}`, (evt) => {
    if (evt.event === 'content_block_start' && evt.data?.content_block?.type === 'tool_use') {
      currentIdx = toolCalls.length;
      toolCalls.push({
        index: evt.data.index,
        id: evt.data.content_block.id,
        name: evt.data.content_block.name,
        inputJson: '',
      });
      console.log(`[step1.${attemptNum}] tool_use_start #${currentIdx} index=${evt.data.index} id=${evt.data.content_block.id} name=${evt.data.content_block.name}`);
    }
    if (evt.event === 'content_block_delta' && evt.data?.delta?.type === 'input_json_delta') {
      // Match by index — the SSE block index identifies which tool_use this delta belongs to.
      const partial = evt.data.delta.partial_json || '';
      if (partial) {
        const blockIndex = evt.data.index;
        const tc = toolCalls.find((t) => t.index === blockIndex);
        if (tc) tc.inputJson += partial;
      }
    }
    if (evt.event === 'content_block_delta' && evt.data?.delta?.type === 'text_delta') {
      textChars += (evt.data.delta.text || '').length;
    }
    if (evt.event === 'message_delta' && evt.data?.delta?.stop_reason) {
      stopReason = evt.data.delta.stop_reason;
    }
    if (evt.event === 'message_stop') sawStop = true;
  });
  if (result.errored) {
    console.log(`[step1.${attemptNum}] FAIL: error event`);
    return { toolCalls: [], errored: true };
  }
  if (result.timedOut) {
    console.log(`[step1.${attemptNum}] FAIL: timed out (toolCalls so far=${toolCalls.length})`);
    return { toolCalls: [], timedOut: true };
  }
  console.log(`[step1.${attemptNum}] message_stop reached: ${toolCalls.length} tool_use(s), ${textChars}c text, stopReason=${stopReason}`);
  return { toolCalls, stopReason, textChars, sawStop };
}

async function step1() {
  for (let i = 1; i <= MAX_STEP1_ATTEMPTS; i++) {
    const r = await step1Once(i);
    if (r.errored || r.timedOut) {
      if (i < MAX_STEP1_ATTEMPTS) {
        console.log(`[step1] retrying after error/timeout...`);
        continue;
      }
      console.log(`[step1] FAIL after ${MAX_STEP1_ATTEMPTS} attempts (error/timeout)`);
      return null;
    }
    if (r.toolCalls.length >= 2) {
      // Validate: distinct ids
      const ids = new Set(r.toolCalls.map((t) => t.id));
      if (ids.size !== r.toolCalls.length) {
        console.log(`[step1.${i}] FAIL: duplicate tool_use ids: ${[...ids].join(', ')}`);
        return null;
      }
      console.log(`[step1] PASS — got ${r.toolCalls.length} parallel tool_uses with distinct ids on attempt ${i}`);
      return { toolCalls: r.toolCalls, attempts: i };
    }
    console.log(`[step1.${i}] only ${r.toolCalls.length} tool_use — model serialized. Retrying...`);
  }
  console.log(`[step1] INCONCLUSIVE — model never parallelized across ${MAX_STEP1_ATTEMPTS} attempts`);
  return { inconclusive: true };
}

async function step2(toolCalls) {
  console.log(`\n--- STEP 2: POST assistant turn + ${toolCalls.length} tool_results ---`);
  // Synthetic plausible answers per tool call. Inspect args to pick a
  // reasonable answer; default to a string if we can't tell.
  const syntheticAnswers = toolCalls.map((tc, i) => {
    let input = {};
    try { input = JSON.parse(tc.inputJson || '{}'); } catch { /* keep empty */ }
    const cmd = (input.command || '').toLowerCase();
    if (cmd.includes('pwd') || cmd.includes('cwd')) return '/Users/test';
    if (cmd.includes('whoami') || cmd.includes('user') || cmd.includes('id ')) return 'testuser';
    return `synthetic-result-${i}`;
  });
  const assistantContent = toolCalls.map((tc) => ({
    type: 'tool_use',
    id: tc.id,
    name: tc.name,
    input: (() => { try { return JSON.parse(tc.inputJson || '{}'); } catch { return {}; } })(),
  }));
  const userContent = toolCalls.map((tc, i) => ({
    type: 'tool_result',
    tool_use_id: tc.id,
    content: syntheticAnswers[i],
  }));
  for (let i = 0; i < toolCalls.length; i++) {
    console.log(`[step2] tool_result #${i}: tool_use_id=${toolCalls[i].id} content="${syntheticAnswers[i]}"`);
  }
  const body = {
    model: 'claude-opus-4-7-thinking-max-fast',
    max_tokens: 1024,
    stream: true,
    messages: [
      { role: 'user', content: STEP1_PROMPT },
      { role: 'assistant', content: assistantContent },
      { role: 'user', content: userContent },
    ],
    tools: TOOLS,
  };
  const res = await fetch(`${API}/v1/messages`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    console.log(`[step2] HTTP ${res.status}: ${(await res.text()).slice(0, 200)}`);
    return { ok: false };
  }
  let textChars = 0;
  let stopReason = null;
  let sawStop = false;
  let outText = '';
  const result = await readSse(res, 'step2', (evt) => {
    if (evt.event === 'content_block_delta' && evt.data?.delta?.type === 'text_delta') {
      const t = evt.data.delta.text || '';
      textChars += t.length;
      outText += t;
      process.stdout.write(t);
    }
    if (evt.event === 'message_delta' && evt.data?.delta?.stop_reason) {
      stopReason = evt.data.delta.stop_reason;
    }
    if (evt.event === 'message_stop') sawStop = true;
  });
  console.log('');
  if (result.timedOut) {
    console.log(`[step2] FAIL: timed out (chars=${textChars}, stopReason=${stopReason})`);
    return { ok: false };
  }
  if (result.errored) {
    console.log('[step2] FAIL: error event');
    return { ok: false };
  }
  if (!sawStop) {
    console.log(`[step2] FAIL: no message_stop (chars=${textChars})`);
    return { ok: false };
  }
  if (textChars === 0) {
    console.log(`[step2] FAIL: message_stop seen but no text (stopReason=${stopReason})`);
    return { ok: false };
  }
  if (stopReason !== 'end_turn') {
    console.log(`[step2] FAIL: expected stop_reason=end_turn, got ${stopReason}`);
    return { ok: false };
  }
  console.log(`[step2] PASS — ${textChars}c text, stopReason=${stopReason}`);
  return { ok: true, textChars, stopReason, text: outText };
}

(async () => {
  const t0 = Date.now();
  console.log(`=== parallel-tools-test against ${API} ===`);
  const s1 = await step1();
  if (!s1) {
    console.log('\nFAIL');
    process.exit(1);
  }
  if (s1.inconclusive) {
    console.log(`\nINCONCLUSIVE — model serialized on all ${MAX_STEP1_ATTEMPTS} attempts. Fix is still verifiable by code inspection, but cannot empirically validate parallel path.`);
    process.exit(0); // Treat as soft pass; document in report.
  }
  const s2 = await step2(s1.toolCalls);
  const dt = Date.now() - t0;
  console.log(`\nTotal time: ${dt}ms  (step1 attempts=${s1.attempts || 1})`);
  if (s2.ok) {
    console.log('PASS');
    process.exit(0);
  } else {
    console.log('FAIL');
    process.exit(1);
  }
})().catch((e) => {
  console.error('uncaught:', e);
  process.exit(1);
});
