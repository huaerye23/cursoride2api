#!/usr/bin/env node
// E2E test for POOL_REINJECT_THINKING=1 against a live RATLC pool.
//
// PURPOSE
// Verify proxy-side thinking re-injection ships end-to-end:
//   - bridge-worker forwards thinking_delta via IPC
//   - pool-manager passes it through
//   - api-server appends to a per-convKey buffer
//   - on turn 2 the captured text appears as <thinking>...</thinking>
//     in the rendered prompt sent to the bridge.
//
// LOAD-BEARING ASSERTION
// The text the api-server WOULD send to the pool on turn 2 contains
// `<thinking>` content from turn 1's captured buffer. The model's downstream
// answer quality is noisy and not asserted — we test the data flow, not
// model behavior.
//
// HOW
// We use the `/v1/_debug/render` endpoint (gated by
// POOL_REINJECT_THINKING_DEBUG=1) to inspect the rendered prompt without
// re-sending it. Run turn 1 normally (which captures thinking into the
// buffer), then ask the api-server to render what turn 2 would look like
// — and grep for `<thinking>`.
//
// PREREQUISITES
//   - Pool running with POOL_REINJECT_THINKING=1 and
//     POOL_REINJECT_THINKING_DEBUG=1 in api-server's env.
//   - At least one ready channel; model is whatever the pool defaults to.
//
// USAGE
//   node scaffolding/pool/reinject-thinking-test.mjs
//
// EXIT
//   0 = PASS; 1 = FAIL with a printed reason.

const API = process.env.API_URL || 'http://127.0.0.1:4242';
const TURN_TIMEOUT_MS = 180_000;
const MODEL = 'claude-opus-4-7-thinking-max-fast';

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

// Use a stable per-test claude-code-session-id so convKey is deterministic
// across both turns. (extractClientSessionId reads this header first.)
function sessionHeaderId() {
  // UUID v4 shape; one per process run.
  return 'aaaaaaaa-bbbb-4ccc-8ddd-' + Math.random().toString(16).slice(2, 14).padEnd(12, '0');
}

async function postTurn(label, messages, sid) {
  const body = {
    model: MODEL,
    max_tokens: 1024,
    stream: true,
    messages,
  };
  const res = await fetch(`${API}/v1/messages`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-claude-code-session-id': sid,
    },
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

async function fetchJson(url, opts = {}) {
  const r = await fetch(url, opts);
  const t = await r.text();
  try { return { ok: r.ok, status: r.status, body: JSON.parse(t) }; }
  catch { return { ok: r.ok, status: r.status, body: t }; }
}

async function renderDebug(messages, sid, mode) {
  const body = { model: MODEL, messages };
  if (mode) body.mode = mode;
  return fetchJson(`${API}/v1/_debug/render`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-claude-code-session-id': sid,
    },
    body: JSON.stringify(body),
  });
}

async function getBufferDebug(convKey) {
  return fetchJson(`${API}/v1/_debug/thinking_buffer?convKey=${encodeURIComponent(convKey)}`);
}

(async () => {
  const t0 = Date.now();
  console.log(`━━━ POOL_REINJECT_THINKING E2E test against ${API} ━━━`);

  // 0. Sanity: debug endpoint must be enabled, buffer must be enabled.
  const cfg = await fetchJson(`${API}/v1/_debug/thinking_buffer`);
  if (!cfg.ok || cfg.status === 404) {
    console.log(`❌ debug endpoint disabled — start api-server with POOL_REINJECT_THINKING_DEBUG=1`);
    process.exit(1);
  }
  if (!cfg.body || !cfg.body.enabled) {
    console.log(`❌ buffer disabled — start with POOL_REINJECT_THINKING=1. cfg=${JSON.stringify(cfg.body)}`);
    process.exit(1);
  }
  console.log(`[cfg] ${JSON.stringify(cfg.body)}`);

  const sid = sessionHeaderId();
  console.log(`[cfg] using x-claude-code-session-id=${sid}`);

  // 1. Turn 1: thinking-heavy prompt.
  const turn1User = 'Compute step-by-step what 17 times 23 equals. Think through it carefully, then state the final answer in one short sentence.';
  console.log(`\n[turn 1] POST: "${turn1User.slice(0, 80)}..."`);
  const r1 = await postTurn('turn1', [
    { role: 'user', content: turn1User },
  ], sid);
  if (!r1.ok) {
    console.log(`❌ turn 1 failed: timedOut=${r1.timedOut} errored=${r1.errored} sawStop=${r1.sawStop}`);
    process.exit(1);
  }
  console.log(`[turn 1] response: "${(r1.text || '').slice(0, 240).replace(/\n/g, ' ')}"`);
  const asst1Text = (r1.text || '').trim();

  // 2. Render-debug after turn 1: turn-2 prompt MUST include <thinking>.
  const turn2Messages = [
    { role: 'user', content: turn1User },
    { role: 'assistant', content: asst1Text },
    { role: 'user', content: 'What number did you compute?' },
  ];
  const rendered = await renderDebug(turn2Messages, sid);
  if (!rendered.ok) {
    console.log(`❌ /v1/_debug/render failed: status=${rendered.status} body=${JSON.stringify(rendered.body).slice(0, 200)}`);
    process.exit(1);
  }
  const convKey = rendered.body.convKey;
  const thinkingCount = rendered.body.thinkingTurnCount;
  const text = rendered.body.rendered || '';
  console.log(`\n[render] mode=${rendered.body.mode} convKey=${convKey} thinkingTurnCount=${thinkingCount} renderedBytes=${text.length}`);

  // Direct buffer dump for the same convKey:
  const bufDump = await getBufferDebug(convKey);
  console.log(`[buffer] turns=${bufDump.body.turns?.length ?? '?'} `
    + `firstTurnLen=${bufDump.body.turns?.[0]?.text?.length ?? '?'}`);

  // Load-bearing assertion: rendered prompt contains <thinking>...</thinking>.
  const hasOpenTag = text.includes('<thinking>');
  const hasCloseTag = text.includes('</thinking>');
  if (!hasOpenTag || !hasCloseTag) {
    console.log(`❌ FAIL — rendered turn-2 prompt does NOT contain <thinking> markers.`);
    console.log(`        hasOpenTag=${hasOpenTag} hasCloseTag=${hasCloseTag}`);
    console.log(`        first 400 chars of rendered:`);
    console.log(text.slice(0, 400));
    process.exit(1);
  }
  if (thinkingCount === 0) {
    console.log(`❌ FAIL — thinkingTurnCount=0 even though tags present. Capture path broken?`);
    process.exit(1);
  }

  // Snippet around the first <thinking> block.
  const tagIdx = text.indexOf('<thinking>');
  const excerpt = text.slice(tagIdx, tagIdx + 400);
  console.log(`\n[excerpt] first <thinking> block (400c):`);
  console.log(excerpt);

  // 3. Negative control: render the same turn-2 with a DIFFERENT
  //    session-id — convKey is different, so the buffer lookup misses and
  //    no <thinking> block should appear.
  const otherSid = sessionHeaderId();
  const otherRendered = await renderDebug(turn2Messages, otherSid);
  if (otherRendered.ok) {
    const otherText = otherRendered.body.rendered || '';
    const otherHasTag = otherText.includes('<thinking>');
    if (otherHasTag) {
      console.log(`❌ FAIL — different session-id ALSO yields <thinking>; convKey isolation broken?`);
      console.log(`        original convKey=${convKey}  other convKey=${otherRendered.body.convKey}`);
      process.exit(1);
    } else {
      console.log(`\n[isolation] OK — different convKey yields no <thinking> (other convKey=${otherRendered.body.convKey})`);
    }
  }

  // 4. `last`-mode coverage: same convKey, render with mode='last'.
  //    The captured thinking turns should appear as a leading preamble
  //    BEFORE the user-message text, not embedded inside an <assistant>
  //    wrapper.
  const lastRendered = await renderDebug(turn2Messages, sid, 'last');
  if (!lastRendered.ok) {
    console.log(`❌ FAIL — /v1/_debug/render?mode=last failed: status=${lastRendered.status}`);
    process.exit(1);
  }
  const lastText = lastRendered.body.rendered || '';
  const lastHasTag = lastText.includes('<thinking>') && lastText.includes('</thinking>');
  if (!lastHasTag) {
    console.log(`❌ FAIL — last-mode render lacks <thinking> markers.`);
    console.log(`        body[0..400]: ${lastText.slice(0, 400)}`);
    process.exit(1);
  }
  const closeIdx = lastText.indexOf('</thinking>');
  const tail = lastText.slice(closeIdx + '</thinking>'.length).replace(/^\s+/, '');
  if (!tail.startsWith('What number did you compute?')) {
    console.log(`❌ FAIL — last-mode preamble not followed by the user text.`);
    console.log(`        tail[0..120]: ${tail.slice(0, 120)}`);
    process.exit(1);
  }
  console.log(`[last-mode] OK — <thinking> preamble precedes user text (rendered=${lastText.length}c)`);

  // 5. Negative-control for the FEATURE-OFF path: ensure that with the
  //    SAME convKey but the buffer disabled, no <thinking> shows up.
  //    We can't toggle env mid-run; instead we run the test with a
  //    convKey we know has no captured turns (use the otherSid path).
  const offRendered = await renderDebug([
    { role: 'user', content: 'fresh, no captured thinking' },
  ], otherSid);
  if (offRendered.ok) {
    const offText = offRendered.body.rendered || '';
    if (offText.includes('<thinking>')) {
      console.log(`❌ FAIL — render with empty-buffer convKey still has <thinking> markers.`);
      process.exit(1);
    } else {
      console.log(`[off-path] OK — empty-buffer convKey yields no <thinking> markers (count=${offRendered.body.thinkingTurnCount})`);
    }
  }

  const dt = Date.now() - t0;
  console.log(`\nTotal time: ${dt}ms`);
  console.log(`\n✅ PASS — turn-2 outbound prompt contains <thinking> content captured from turn 1 (thinkingTurnCount=${thinkingCount}).`);
  process.exit(0);
})().catch((e) => {
  console.error('uncaught:', e);
  process.exit(1);
});
