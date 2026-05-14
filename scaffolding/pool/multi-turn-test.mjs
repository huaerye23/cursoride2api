#!/usr/bin/env node
// Multi-turn conversation coherence test against the RATLC pool.
//
// PURPOSE
// Demonstrate the gap between POOL_CONTEXT_MODE=last (current default —
// only the last user message is forwarded) and POOL_CONTEXT_MODE=full
// (the new mode — entire messages[] history is rendered into the prompt
// each turn). With pool_size >= 2 and concurrent_opens >= 1, turn 2 of
// the same conversation can land on a different channel than turn 1;
// in `last` mode that channel has zero context from turn 1 and the
// model cannot answer the follow-up.
//
// PROTOCOL
// Turn 1: POST [{user:'I have three apples.'}]
//          — expect an acknowledgment (any text response)
// Turn 2: POST [user1, asst1, {user:'How many do I have?'}]
//          — expect "three" or "3" to appear in the response
//
// SEMANTICS
//   - In `full` mode: PASS is required (the fix works)
//   - In `last` mode: FAIL is expected (we're demonstrating the
//     limitation). Use --expect-fail to invert the exit code.
//
// USAGE
//   node multi-turn-test.mjs                # current mode, expect PASS
//   node multi-turn-test.mjs --expect-fail  # expect FAIL (last mode)
//
// Exits 0 on the outcome matching `--expect-fail` (or absence thereof),
// 1 otherwise.

const API = process.env.API_URL || 'http://127.0.0.1:4242';
const TURN_TIMEOUT_MS = 90_000;
const EXPECT_FAIL = process.argv.includes('--expect-fail');

async function readSseText(res, label) {
  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let buf = '';
  let text = '';
  let stopReason = null;
  let sawStop = false;
  let errored = false;
  const start = Date.now();
  while (true) {
    if (Date.now() - start > TURN_TIMEOUT_MS) {
      console.log(`[${label}] TIMEOUT after ${TURN_TIMEOUT_MS}ms (text so far=${text.length}c)`);
      return { text, stopReason, sawStop, timedOut: true, errored };
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
      if (!evt.event) continue;
      if (evt.event === 'content_block_delta' && evt.data?.delta?.type === 'text_delta') {
        text += evt.data.delta.text || '';
      }
      if (evt.event === 'message_delta' && evt.data?.delta?.stop_reason) {
        stopReason = evt.data.delta.stop_reason;
      }
      if (evt.event === 'message_stop') { sawStop = true; return { text, stopReason, sawStop, timedOut: false, errored }; }
      if (evt.event === 'error') { errored = true; console.log(`[${label}] error event:`, JSON.stringify(evt.data)); return { text, stopReason, sawStop: false, timedOut: false, errored }; }
    }
  }
  return { text, stopReason, sawStop, timedOut: false, errored };
}

async function postTurn(label, messages) {
  const body = {
    model: 'claude-opus-4-7-thinking-max-fast',
    max_tokens: 1024,
    stream: true,
    messages,
  };
  const res = await fetch(`${API}/v1/messages`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const errText = await res.text();
    console.log(`[${label}] HTTP ${res.status}: ${errText.slice(0, 200)}`);
    return { ok: false };
  }
  const r = await readSseText(res, label);
  return { ok: !r.timedOut && !r.errored && r.sawStop, ...r };
}

// Look for "three", "3", or "III" (case-insensitive) as standalone tokens
// surrounded by non-word boundaries — avoids matching "threeshold" or "13"
// or "thirteen".
function mentionsThree(text) {
  if (!text) return false;
  const lower = text.toLowerCase();
  if (/\bthree\b/.test(lower)) return true;
  if (/(^|\D)3(\D|$)/.test(lower)) return true;
  if (/\biii\b/.test(lower)) return true;
  return false;
}

(async () => {
  const t0 = Date.now();
  console.log(`━━━ multi-turn coherence test against ${API} ━━━`);
  console.log(`expect-fail=${EXPECT_FAIL}  (set --expect-fail when running in last mode)`);

  // Turn 1
  console.log('\n[turn 1] POST: "I have three apples."');
  const r1 = await postTurn('turn1', [
    { role: 'user', content: 'I have three apples.' },
  ]);
  if (!r1.ok) {
    console.log(`❌ turn 1 failed: timedOut=${r1.timedOut} errored=${r1.errored} sawStop=${r1.sawStop}`);
    process.exit(1);
  }
  console.log(`[turn 1] response: "${(r1.text || '').slice(0, 200).replace(/\n/g, ' ')}"`);
  const asst1Text = (r1.text || '').trim();
  if (!asst1Text) {
    console.log('❌ turn 1: empty response — cannot continue');
    process.exit(1);
  }

  // Turn 2
  console.log('\n[turn 2] POST: full prior context + "How many do I have?"');
  const r2 = await postTurn('turn2', [
    { role: 'user', content: 'I have three apples.' },
    { role: 'assistant', content: asst1Text },
    { role: 'user', content: 'How many do I have?' },
  ]);
  if (!r2.ok) {
    console.log(`❌ turn 2 failed: timedOut=${r2.timedOut} errored=${r2.errored} sawStop=${r2.sawStop}`);
    process.exit(1);
  }
  console.log(`[turn 2] response: "${(r2.text || '').slice(0, 400).replace(/\n/g, ' ')}"`);

  const coherent = mentionsThree(r2.text || '');
  const dt = Date.now() - t0;
  console.log(`\nTotal time: ${dt}ms`);

  if (EXPECT_FAIL) {
    // We expect the model to NOT mention three / 3 (because it has no context).
    if (!coherent) {
      console.log(`✅ PASS (expected fail) — model did NOT mention three/3, demonstrating the context gap.`);
      process.exit(0);
    } else {
      console.log(`❌ FAIL — model mentioned three/3 even in last mode. Test premise broken or model guessed.`);
      process.exit(1);
    }
  } else {
    if (coherent) {
      console.log(`✅ PASS — turn 2 response mentions three/3, model has prior context.`);
      process.exit(0);
    } else {
      console.log(`❌ FAIL — turn 2 response does NOT mention three/3. Model lost the prior context.`);
      process.exit(1);
    }
  }
})().catch((e) => {
  console.error('uncaught:', e);
  process.exit(1);
});
