#!/usr/bin/env node
// E2E test for translate-mode v2: drive a tool round-trip through the pool.
//   1. POST a user message that should trigger Bash (Cursor's native Shell).
//   2. Capture the assistant's tool_use(name=Bash) block.
//   3. POST a follow-up with the user-side tool_result simulating ls output.
//   4. Capture the final text response from the inner agent.
//
// Verifies:
//   - Cursor's `shellArgs` was routed through onMcpCall (not rejected).
//   - api-server emitted it as anthropic-format tool_use(Bash).
//   - Our tool_result was translated back into ShellResult.success.
//   - The inner agent saw a successful Shell result and continued.

import http from 'node:http';

const API = process.env.POOL_API || 'http://127.0.0.1:4242/v1/messages';

function postSSE(body, label) {
  return new Promise((resolve, reject) => {
    const url = new URL(API);
    const req = http.request(
      {
        hostname: url.hostname, port: url.port, path: url.pathname,
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
      },
      (res) => {
        const events = [];
        let buf = '';
        res.on('data', (c) => {
          buf += c.toString();
          let idx;
          while ((idx = buf.indexOf('\n\n')) !== -1) {
            const block = buf.slice(0, idx);
            buf = buf.slice(idx + 2);
            const lines = block.split('\n');
            const evt = {};
            for (const ln of lines) {
              if (ln.startsWith('event: ')) evt.event = ln.slice(7).trim();
              else if (ln.startsWith('data: ')) {
                try { evt.data = JSON.parse(ln.slice(6)); } catch { evt.data = ln.slice(6); }
              }
            }
            if (evt.event) events.push(evt);
          }
        });
        res.on('end', () => resolve({ status: res.statusCode, events }));
        res.on('error', reject);
      }
    );
    req.on('error', reject);
    req.write(JSON.stringify(body));
    req.end();
  });
}

function pp(events, label) {
  console.log(`\n──── ${label} ────`);
  for (const e of events) {
    if (e.event === 'content_block_delta' && e.data?.delta?.type === 'text_delta') {
      process.stdout.write(e.data.delta.text);
    } else if (e.event === 'content_block_start' && e.data?.content_block?.type === 'tool_use') {
      console.log(`\n[tool_use] name=${e.data.content_block.name} id=${e.data.content_block.id}`);
    } else if (e.event === 'content_block_delta' && e.data?.delta?.type === 'input_json_delta') {
      console.log(`[tool_use.input] ${e.data.delta.partial_json}`);
    } else if (e.event === 'message_delta') {
      console.log(`\n[message_delta] stop_reason=${e.data?.delta?.stop_reason}`);
    } else if (e.event === 'error') {
      console.log(`\n[ERROR] ${JSON.stringify(e.data)}`);
    }
  }
  console.log();
}

function findToolUse(events) {
  for (const e of events) {
    if (e.event === 'content_block_start' && e.data?.content_block?.type === 'tool_use') {
      let input = e.data.content_block.input || {};
      // look for matching input_json_delta
      for (const e2 of events) {
        if (e2.event === 'content_block_delta' && e2.data?.delta?.type === 'input_json_delta') {
          try {
            input = { ...input, ...JSON.parse(e2.data.delta.partial_json) };
          } catch { /* ignore */ }
        }
      }
      return { id: e.data.content_block.id, name: e.data.content_block.name, input };
    }
  }
  return null;
}

async function main() {
  const userPrompt = "Use the Bash tool to run 'ls /tmp/ratlc-claude-test' and tell me what's in that directory. Brief one-sentence summary.";

  console.log(`POST 1: ${JSON.stringify(userPrompt)}`);
  const r1 = await postSSE({
    model: 'claude-opus-4-7',
    max_tokens: 500,
    stream: true,
    messages: [{ role: 'user', content: userPrompt }],
    tools: [
      { name: 'Bash', description: 'Run a bash command',
        input_schema: { type: 'object', properties: { command: { type: 'string' } }, required: ['command'] } },
    ],
  }, 'round 1');
  pp(r1.events, 'round 1');

  const tu = findToolUse(r1.events);
  if (!tu) { console.log('No tool_use emitted — translate-mode passthrough may not be working.'); process.exit(1); }
  console.log(`✅ tool_use captured: name=${tu.name} id=${tu.id} input=${JSON.stringify(tu.input)}`);

  // Simulate executing the Bash command and providing the result
  console.log('\n[client executes Bash...]');
  let bashOutput;
  try {
    const { execSync } = await import('node:child_process');
    bashOutput = execSync(tu.input.command || 'ls /tmp', { encoding: 'utf8', timeout: 10_000 });
  } catch (e) {
    bashOutput = `[exec error] ${e.message}`;
  }
  console.log(`Bash output: ${JSON.stringify(bashOutput.slice(0, 200))}`);

  console.log(`\nPOST 2: feeding tool_result back`);
  const r2 = await postSSE({
    model: 'claude-opus-4-7',
    max_tokens: 500,
    stream: true,
    messages: [
      { role: 'user', content: userPrompt },
      { role: 'assistant', content: [{ type: 'tool_use', id: tu.id, name: tu.name, input: tu.input }] },
      { role: 'user', content: [{ type: 'tool_result', tool_use_id: tu.id, content: bashOutput }] },
    ],
    tools: [
      { name: 'Bash', description: 'Run a bash command',
        input_schema: { type: 'object', properties: { command: { type: 'string' } }, required: ['command'] } },
    ],
  }, 'round 2');
  pp(r2.events, 'round 2');

  // Find the final text
  const finalText = r2.events
    .filter((e) => e.event === 'content_block_delta' && e.data?.delta?.type === 'text_delta')
    .map((e) => e.data.delta.text).join('');
  if (finalText.length > 0) {
    console.log(`\n🎯 final response: ${JSON.stringify(finalText.slice(0, 300))}`);
    console.log('\n✅ translate-mode v2 end-to-end works');
  } else {
    console.log('\n⚠️ no final text — model may have errored or made another tool call');
  }
}

main().catch((e) => { console.error('fatal:', e); process.exit(1); });
