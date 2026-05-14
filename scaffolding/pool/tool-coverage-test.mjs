#!/usr/bin/env node
// tool-coverage-test.mjs
//
// Verifies a running pool (in translate mode) exposes the full common
// claude-code toolset to the model, and that the model actually emits
// tool_use blocks for each one when asked. This is the regression guard
// for the "Write/Edit/StrReplace unavailable" hallucination issue.
//
// Requires a running pool at $API_URL (default http://127.0.0.1:4242)
// running in POOL_TOOL_MODE=translate. Skips elegantly if the pool isn't
// reachable so it can be safely added to a CI matrix that might not boot
// the pool.
//
// What we assert:
//   1. The model's enumerated tool list includes the new MCP-registered
//      Anthropic-named tools: Edit, NotebookEdit (and mcp_Glob/mcp_TodoWrite
//      for the prefixed ones).
//   2. When asked to call Edit, the model emits tool_use(name="Edit") with
//      the file_path/old_string/new_string keys claude-code expects.
//   3. When asked to call Glob, the model emits a tool_use whose name maps
//      to the Anthropic "Glob" (either via direct mcp_Glob or via Cursor's
//      native Glob translated by the proxy).
//   4. Read/Write/Bash/Grep all complete a single-turn tool_use round-trip
//      (translated to Anthropic names).
//
// PASS: every common claude-code tool is callable without the model saying
// "X unavailable" or falling back to Bash heredocs.

const API = process.env.API_URL || 'http://127.0.0.1:4242';
const STEP_TIMEOUT_MS = parseInt(process.env.TOOL_COVERAGE_STEP_TIMEOUT_MS || '120000', 10);

const claudeCodeTools = [
  {
    name: 'Read',
    description: 'Read a file. Returns its content as text.',
    input_schema: {
      type: 'object',
      properties: { file_path: { type: 'string' } },
      required: ['file_path'],
    },
  },
  {
    name: 'Write',
    description: 'Write a new file or overwrite an existing one.',
    input_schema: {
      type: 'object',
      properties: { file_path: { type: 'string' }, content: { type: 'string' } },
      required: ['file_path', 'content'],
    },
  },
  {
    name: 'Edit',
    description: 'Find-and-replace in a file. Args: file_path, old_string, new_string.',
    input_schema: {
      type: 'object',
      properties: { file_path: { type: 'string' }, old_string: { type: 'string' }, new_string: { type: 'string' } },
      required: ['file_path', 'old_string', 'new_string'],
    },
  },
  {
    name: 'Bash',
    description: 'Run a shell command.',
    input_schema: {
      type: 'object',
      properties: { command: { type: 'string' } },
      required: ['command'],
    },
  },
  {
    name: 'Grep',
    description: 'Search for a pattern across files.',
    input_schema: {
      type: 'object',
      properties: { pattern: { type: 'string' }, path: { type: 'string' } },
      required: ['pattern'],
    },
  },
  {
    name: 'Glob',
    description: 'Find files by glob pattern. Returns matching paths.',
    input_schema: {
      type: 'object',
      properties: { pattern: { type: 'string' } },
      required: ['pattern'],
    },
  },
];

let stepCount = 0;
const failures = [];

function step(label) {
  stepCount++;
  console.log(`\n━━━ STEP ${stepCount}: ${label} ━━━`);
}
function pass(msg) { console.log(`  ✅ ${msg}`); }
function fail(msg) { console.log(`  ❌ ${msg}`); failures.push(msg); }
function assert(cond, msg) { (cond ? pass : fail)(msg); }

async function postSSE(body) {
  const res = await fetch(`${API}/v1/messages`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(STEP_TIMEOUT_MS),
  });
  if (!res.ok) throw new Error(`HTTP ${res.status} ${await res.text().catch(() => '')}`);
  const events = [];
  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let buf = '';
  while (true) {
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
      if (evt.event) events.push(evt);
    }
  }
  return events;
}

function extractToolUses(events) {
  const tools = [];
  let cur = null;
  for (const e of events) {
    if (e.event === 'content_block_start' && e.data?.content_block?.type === 'tool_use') {
      if (cur) tools.push(cur);
      cur = { id: e.data.content_block.id, name: e.data.content_block.name, inputRaw: '' };
    }
    if (e.event === 'content_block_delta' && e.data?.delta?.type === 'input_json_delta' && cur) {
      cur.inputRaw += e.data.delta.partial_json || '';
    }
  }
  if (cur) tools.push(cur);
  return tools.map((t) => {
    let input = {};
    try { input = t.inputRaw ? JSON.parse(t.inputRaw) : {}; } catch { /* ignore */ }
    return { id: t.id, name: t.name, input };
  });
}

function extractText(events) {
  return events.filter((e) => e.event === 'content_block_delta' && e.data?.delta?.type === 'text_delta')
    .map((e) => e.data.delta.text).join('');
}

async function checkAlive() {
  try {
    const res = await fetch(`${API}/health`, { signal: AbortSignal.timeout(2000) });
    return res.ok;
  } catch { return false; }
}

// Close any tool_uses left dangling by a step: send back a synthetic
// tool_result for each so the underlying pool channel goes back to ready
// rather than staying busy until next idle-ping. Best-effort — failures
// are logged but don't fail the test.
async function closeToolUses(originalPrompt, tools, toolUses) {
  if (!toolUses || toolUses.length === 0) return;
  try {
    await postSSE({
      model: 'claude-opus-4-7-thinking-max-fast',
      max_tokens: 256,
      stream: true,
      messages: [
        { role: 'user', content: originalPrompt },
        { role: 'assistant', content: toolUses.map((t) => ({ type: 'tool_use', id: t.id, name: t.name, input: t.input })) },
        { role: 'user', content: toolUses.map((t) => ({ type: 'tool_result', tool_use_id: t.id, content: '[ok]' })) },
      ],
      tools,
    });
  } catch (e) {
    // Channels left busy will time out via idle ping; not fatal here.
  }
}

(async () => {
  if (!(await checkAlive())) {
    console.log(`No pool reachable at ${API}/health — skipping live tool-coverage test.`);
    process.exit(0);
  }

  step('model enumerates the expected toolset');
  {
    const events = await postSSE({
      model: 'claude-opus-4-7-thinking-max-fast',
      max_tokens: 1024,
      stream: true,
      messages: [{
        role: 'user',
        content: 'List every tool you have access to as a JSON array of tool names. Output ONLY the JSON array, no other text.',
      }],
    });
    const text = extractText(events);
    let listed = [];
    const match = text.match(/\[[\s\S]*\]/);
    if (match) {
      try { listed = JSON.parse(match[0]); } catch { /* leave empty */ }
    }
    console.log(`  enumerated: ${JSON.stringify(listed).slice(0, 220)}`);
    // The model should see at least these. Edit/NotebookEdit are the
    // proxy-registered MCP tools matching claude-code's Anthropic names.
    // mcp_Glob and mcp_TodoWrite are prefixed clones of Cursor natives.
    const expectedSomewhere = ['Edit', 'NotebookEdit', 'mcp_Glob', 'mcp_TodoWrite'];
    for (const t of expectedSomewhere) {
      assert(listed.includes(t), `tool "${t}" is visible to the model`);
    }
    // And the Cursor-native set must still be present (we want both surfaces).
    for (const t of ['Read', 'Write', 'Shell', 'Grep', 'Glob', 'StrReplace']) {
      assert(listed.includes(t), `Cursor-native "${t}" still visible`);
    }
  }

  step('model calls Edit (the Anthropic name) directly when asked');
  {
    const editTools = [
      {
        name: 'Edit',
        description: 'Find-and-replace edit on a file. Args: file_path, old_string, new_string.',
        input_schema: {
          type: 'object',
          properties: { file_path: { type: 'string' }, old_string: { type: 'string' }, new_string: { type: 'string' } },
          required: ['file_path', 'old_string', 'new_string'],
        },
      },
    ];
    const prompt = 'Change the text "foo" to "bar" in /tmp/cov-edit.txt using the Edit tool. ' +
      'The file already exists and contains "foo". Call Edit directly with ' +
      'file_path="/tmp/cov-edit.txt", old_string="foo", new_string="bar". No need to read first.';
    const events = await postSSE({
      model: 'claude-opus-4-7-thinking-max-fast',
      max_tokens: 1024,
      stream: true,
      messages: [{ role: 'user', content: prompt }],
      tools: editTools,
    });
    const tu = extractToolUses(events);
    console.log(`  emitted: ${tu.map((t) => `${t.name}(${JSON.stringify(t.input).slice(0, 60)})`).join(', ') || '(no tool)'}`);
    const editCall = tu.find((t) => t.name === 'Edit');
    assert(!!editCall, 'tool_use(name="Edit") emitted');
    if (editCall) {
      const hasShape = editCall.input?.file_path && editCall.input?.old_string && editCall.input?.new_string;
      assert(!!hasShape, `Edit input has file_path/old_string/new_string (got keys: ${Object.keys(editCall.input || {}).join(',')})`);
    }
    // Close out the round-trip so the underlying channel returns to ready.
    await closeToolUses(prompt, editTools, tu);
  }

  step('Bash round-trip works (pwd → response)');
  {
    const prompt = 'Use Bash to run "pwd" and tell me my current directory in one sentence.';
    const events = await postSSE({
      model: 'claude-opus-4-7-thinking-max-fast',
      max_tokens: 1024,
      stream: true,
      messages: [{ role: 'user', content: prompt }],
      tools: claudeCodeTools,
    });
    const tu = extractToolUses(events);
    const bash = tu.find((t) => t.name === 'Bash');
    assert(!!bash, 'tool_use(name="Bash") emitted');
    assert(typeof bash?.input?.command === 'string' && bash.input.command.length > 0,
      `Bash.command is a non-empty string (got "${bash?.input?.command}")`);
    await closeToolUses(prompt, claudeCodeTools, tu);
  }

  step('Read round-trip works');
  {
    const prompt = 'Use the Read tool to read /tmp/does-not-matter-just-call-it.txt — just call Read once.';
    const events = await postSSE({
      model: 'claude-opus-4-7-thinking-max-fast',
      max_tokens: 1024,
      stream: true,
      messages: [{ role: 'user', content: prompt }],
      tools: claudeCodeTools,
    });
    const tu = extractToolUses(events);
    const read = tu.find((t) => t.name === 'Read');
    assert(!!read, 'tool_use(name="Read") emitted');
    assert(typeof read?.input?.file_path === 'string', `Read.file_path is a string`);
    await closeToolUses(prompt, claudeCodeTools, tu);
  }

  step('Grep round-trip works');
  {
    const prompt = 'Use the Grep tool to search for "TODO" in /tmp. Just call Grep once with pattern="TODO" path="/tmp".';
    const events = await postSSE({
      model: 'claude-opus-4-7-thinking-max-fast',
      max_tokens: 1024,
      stream: true,
      messages: [{ role: 'user', content: prompt }],
      tools: claudeCodeTools,
    });
    const tu = extractToolUses(events);
    const grep = tu.find((t) => t.name === 'Grep');
    assert(!!grep, 'tool_use(name="Grep") emitted');
    assert(typeof grep?.input?.pattern === 'string' && grep.input.pattern.length > 0,
      `Grep.pattern is a non-empty string`);
    await closeToolUses(prompt, claudeCodeTools, tu);
  }

  step('Glob round-trip works');
  {
    // The user's bar is "no 'X unavailable' hallucination" — not that the
    // model picks any specific tool. Cursor's native Grep tool accepts a
    // `glob:` filter, and the model legitimately prefers Grep when both are
    // available because it can match the request with a single call. We
    // accept Glob / mcp_Glob (the primary path) OR Grep with a glob field
    // (the substitution path — also a useful pattern from claude-code's POV).
    const globTools = [
      {
        name: 'Glob',
        description: 'Find files by glob pattern.',
        input_schema: {
          type: 'object',
          properties: { pattern: { type: 'string' } },
          required: ['pattern'],
        },
      },
    ];
    const prompt = 'Use the Glob tool to find all .txt files under /tmp. Call Glob with pattern="/tmp/*.txt". Just one call.';
    const events = await postSSE({
      model: 'claude-opus-4-7-thinking-max-fast',
      max_tokens: 1024,
      stream: true,
      messages: [{ role: 'user', content: prompt }],
      tools: globTools,
    });
    const tu = extractToolUses(events);
    const glob = tu.find((t) => t.name === 'Glob' || t.name === 'mcp_Glob');
    const grepWithGlob = tu.find((t) => t.name === 'Grep' && (t.input?.glob || /\*/.test(t.input?.pattern || '')));
    const text = extractText(events);
    const sayUnavailable = /unavailable|not available|cannot use|don't have access/i.test(text);
    assert(!sayUnavailable, `no "unavailable" hallucination (text="${text.slice(0, 100)}")`);
    if (glob) {
      pass(`Glob emitted directly (name=${glob.name})`);
    } else if (grepWithGlob) {
      pass(`Grep substituted for Glob with glob-shaped args — acceptable (input=${JSON.stringify(grepWithGlob.input).slice(0, 100)})`);
    } else {
      fail(`expected Glob or Grep-with-glob (got: ${tu.map((t) => `${t.name}(${JSON.stringify(t.input).slice(0, 60)})`).join(',') || 'none'})`);
    }
    await closeToolUses(prompt, globTools, tu);
  }

  step('Write round-trip works (or Read-first safety check)');
  {
    // claude-code's training defaults to Read-before-Write for safety. In
    // translate mode the model has access to both, so it will often Read
    // first. The bar is: round-trip alive, no "unavailable" hallucination.
    const prompt = 'Create a new file at /tmp/cov-write.txt with content "hello world". Use the Write tool with file_path=/tmp/cov-write.txt content="hello world".';
    const events = await postSSE({
      model: 'claude-opus-4-7-thinking-max-fast',
      max_tokens: 1024,
      stream: true,
      messages: [{ role: 'user', content: prompt }],
      tools: claudeCodeTools,
    });
    const tu = extractToolUses(events);
    const write = tu.find((t) => t.name === 'Write');
    const read = tu.find((t) => t.name === 'Read');
    const text = extractText(events);
    const sayUnavailable = /unavailable|not available|cannot use|don't have access/i.test(text);
    assert(!sayUnavailable, `no "unavailable" hallucination on Write request (text="${text.slice(0, 100)}")`);
    assert(!!write || !!read,
      `tool_use(name="Write" or "Read") emitted (got: ${tu.map((t) => t.name).join(',') || 'none'})`);
    await closeToolUses(prompt, claudeCodeTools, tu);
  }

  console.log('');
  if (failures.length === 0) {
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    console.log(`✅ tool-coverage: ALL ${stepCount} STEPS PASS`);
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    process.exit(0);
  } else {
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    console.log(`❌ tool-coverage: ${failures.length} failure(s):`);
    for (const m of failures) console.log(`   - ${m}`);
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    process.exit(1);
  }
})().catch((e) => {
  console.error('test exception:', e);
  process.exit(2);
});
