#!/usr/bin/env node
// Unit tests for scaffolding/pool/thinking-buffer.mjs.
//
// Tests buffer mechanics in isolation:
//   - append respects MAX_BYTES_PER_TURN truncation
//   - commitTurn pushes accumulated text + resets the current-turn buffer
//   - turn-count eviction (FIFO once MAX_TURNS exceeded)
//   - TTL eviction
//   - multi-convKey isolation (no cross-talk)
//   - [Tool call: ...] scrubbing
//   - disabled mode is a no-op
//   - discardCurrent drops in-progress without committing
//
// Run with:
//   POOL_REINJECT_THINKING=1 POOL_REINJECT_THINKING_MAX_BYTES_PER_TURN=64 \
//     POOL_REINJECT_THINKING_MAX_TURNS=3 \
//     node scaffolding/pool/thinking-buffer-test.mjs
//
// Or just `node scaffolding/pool/thinking-buffer-test.mjs` — defaults are
// picked to keep the test compact.

import { fileURLToPath } from 'node:url';
import path from 'node:path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// thinking-buffer.mjs reads env on module load. We freshModule() each
// section with a cache-busting query string so the new env takes effect
// without spawning a child process.

let total = 0;
let passed = 0;
let failed = 0;
const failures = [];

function eq(name, actual, expected) {
  total += 1;
  const a = JSON.stringify(actual);
  const e = JSON.stringify(expected);
  if (a === e) {
    passed += 1;
    console.log(`  PASS  ${name}`);
  } else {
    failed += 1;
    failures.push({ name, actual, expected });
    console.log(`  FAIL  ${name}`);
    console.log(`        expected: ${e}`);
    console.log(`        actual:   ${a}`);
  }
}

function truthy(name, value) {
  total += 1;
  if (value) {
    passed += 1;
    console.log(`  PASS  ${name}`);
  } else {
    failed += 1;
    failures.push({ name, actual: value, expected: 'truthy' });
    console.log(`  FAIL  ${name} (got ${JSON.stringify(value)})`);
  }
}

// Use dynamic import so we don't load it at the top with a fixed env.
function freshModule(env) {
  // Override env, then import via a query-string fresh-load trick.
  for (const k of Object.keys(env)) {
    if (env[k] == null) delete process.env[k];
    else process.env[k] = env[k];
  }
  const url = path.join(__dirname, 'thinking-buffer.mjs') + `?t=${Math.random()}`;
  return import(url);
}

async function main() {
  console.log('thinking-buffer unit tests\n');

  // ── 1. Disabled mode is a no-op ─────────────────────────────────────
  console.log('Section: disabled mode (POOL_REINJECT_THINKING unset)');
  {
    const m = await freshModule({ POOL_REINJECT_THINKING: '' });
    eq('isEnabled() returns false', m.isEnabled(), false);
    m.append('k1', 'hello');
    m.commitTurn('k1');
    eq('getForConvKey returns [] when disabled', m.getForConvKey('k1'), []);
    eq('size() is 0 when disabled', m.size(), 0);
  }

  // ── 2. Enabled mode: basic append + commit ───────────────────────────
  console.log('\nSection: enabled mode (basic append + commit)');
  {
    const m = await freshModule({
      POOL_REINJECT_THINKING: '1',
      POOL_REINJECT_THINKING_MAX_BYTES_PER_TURN: '64',
      POOL_REINJECT_THINKING_MAX_TURNS: '3',
    });
    m._resetForTests();
    eq('isEnabled() returns true', m.isEnabled(), true);
    eq('maxBytesPerTurn=64', m.maxBytesPerTurn(), 64);
    eq('maxTurns=3', m.maxTurns(), 3);

    m.append('A', 'first half ');
    m.append('A', 'second half');
    m.commitTurn('A');
    const out = m.getForConvKey('A');
    eq('one stored turn after one commit', out.length, 1);
    eq('turn 0 has turnIndex=0', out[0].turnIndex, 0);
    eq('turn 0 text concatenated', out[0].text, 'first half second half');
  }

  // ── 3. MAX_BYTES_PER_TURN truncation ─────────────────────────────────
  console.log('\nSection: MAX_BYTES_PER_TURN truncation');
  {
    const m = await freshModule({
      POOL_REINJECT_THINKING: '1',
      POOL_REINJECT_THINKING_MAX_BYTES_PER_TURN: '20',
      POOL_REINJECT_THINKING_MAX_TURNS: '5',
    });
    m._resetForTests();
    m.append('K', 'x'.repeat(15));
    m.append('K', 'y'.repeat(50));  // only first 5 'y's fit
    m.append('K', 'z'.repeat(10));  // entirely dropped
    m.commitTurn('K');
    const out = m.getForConvKey('K');
    eq('truncated to 20 bytes', out[0].text.length, 20);
    eq('truncation kept first 15 x then 5 y', out[0].text, 'x'.repeat(15) + 'y'.repeat(5));
  }

  // ── 4. MAX_TURNS FIFO eviction ───────────────────────────────────────
  console.log('\nSection: MAX_TURNS FIFO eviction');
  {
    const m = await freshModule({
      POOL_REINJECT_THINKING: '1',
      POOL_REINJECT_THINKING_MAX_BYTES_PER_TURN: '256',
      POOL_REINJECT_THINKING_MAX_TURNS: '3',
    });
    m._resetForTests();
    for (let i = 0; i < 5; i++) {
      m.append('Q', `turn-${i}-content`);
      m.commitTurn('Q');
    }
    const out = m.getForConvKey('Q');
    eq('only 3 turns retained after 5 commits', out.length, 3);
    eq('oldest dropped, turn 2 first', out[0].turnIndex, 2);
    eq('turn 3 second', out[1].turnIndex, 3);
    eq('turn 4 last', out[2].turnIndex, 4);
    eq('content of oldest retained', out[0].text, 'turn-2-content');
    eq('content of newest retained', out[2].text, 'turn-4-content');
  }

  // ── 5. Multi-convKey isolation ───────────────────────────────────────
  console.log('\nSection: multi-convKey isolation');
  {
    const m = await freshModule({
      POOL_REINJECT_THINKING: '1',
      POOL_REINJECT_THINKING_MAX_BYTES_PER_TURN: '256',
      POOL_REINJECT_THINKING_MAX_TURNS: '5',
    });
    m._resetForTests();
    m.append('Alice', 'aliceA');
    m.append('Bob', 'bobA');
    m.commitTurn('Alice');
    m.commitTurn('Bob');
    m.append('Alice', 'aliceB');
    m.commitTurn('Alice');
    const a = m.getForConvKey('Alice');
    const b = m.getForConvKey('Bob');
    eq('Alice has 2 turns', a.length, 2);
    eq('Bob has 1 turn', b.length, 1);
    eq('Alice turn 0 text', a[0].text, 'aliceA');
    eq('Alice turn 1 text', a[1].text, 'aliceB');
    eq('Bob turn 0 text', b[0].text, 'bobA');
    eq('size() reflects 2 conversations', m.size(), 2);
  }

  // ── 6. [Tool call: ...] scrubbing ────────────────────────────────────
  console.log('\nSection: [Tool call: ...] scrubbing');
  {
    const m = await freshModule({
      POOL_REINJECT_THINKING: '1',
      POOL_REINJECT_THINKING_MAX_BYTES_PER_TURN: '512',
      POOL_REINJECT_THINKING_MAX_TURNS: '5',
    });
    m._resetForTests();
    m.append('S', 'reasoning before. [Tool call: Bash({"cmd":"ls"})] reasoning after.');
    m.commitTurn('S');
    const out = m.getForConvKey('S');
    eq('scrubbed text omits Tool-call marker', out[0].text.includes('[Tool call:'), false);
    truthy('scrubbed text retains "reasoning before"', out[0].text.includes('reasoning before'));
    truthy('scrubbed text retains "reasoning after"', out[0].text.includes('reasoning after'));
  }

  // ── 7. discardCurrent drops in-progress ──────────────────────────────
  console.log('\nSection: discardCurrent');
  {
    const m = await freshModule({
      POOL_REINJECT_THINKING: '1',
      POOL_REINJECT_THINKING_MAX_BYTES_PER_TURN: '256',
      POOL_REINJECT_THINKING_MAX_TURNS: '5',
    });
    m._resetForTests();
    m.append('D', 'oops partial');
    m.discardCurrent('D');
    m.commitTurn('D');
    eq('discardCurrent prevents partial from being committed', m.getForConvKey('D'), []);
    m.append('D', 'real turn');
    m.commitTurn('D');
    const out = m.getForConvKey('D');
    eq('subsequent commit still works, turnIndex=1', out[0].turnIndex, 1);
    eq('subsequent commit text', out[0].text, 'real turn');
  }

  // ── 8. TTL eviction via _evictForTests ───────────────────────────────
  console.log('\nSection: TTL eviction');
  {
    const m = await freshModule({
      POOL_REINJECT_THINKING: '1',
      POOL_REINJECT_THINKING_MAX_BYTES_PER_TURN: '256',
      POOL_REINJECT_THINKING_MAX_TURNS: '5',
    });
    m._resetForTests();
    m.append('T', 'will go stale');
    m.commitTurn('T');
    eq('one conv before TTL', m.size(), 1);
    const future = Date.now() + m._ttlMs() + 60_000;
    m._evictForTests(future);
    eq('TTL eviction clears stale conv', m.size(), 0);
    eq('getForConvKey returns [] after eviction', m.getForConvKey('T'), []);
  }

  // ── 9. Empty append / commit on never-seen convKey ───────────────────
  console.log('\nSection: empty cases');
  {
    const m = await freshModule({
      POOL_REINJECT_THINKING: '1',
      POOL_REINJECT_THINKING_MAX_BYTES_PER_TURN: '64',
      POOL_REINJECT_THINKING_MAX_TURNS: '3',
    });
    m._resetForTests();
    m.commitTurn('never-touched');  // no-op
    eq('commit on never-touched is safe (no entry stored)', m.getForConvKey('never-touched'), []);
    m.append('E', '');  // empty string
    m.commitTurn('E');
    eq('commit of empty text stores no turn', m.getForConvKey('E'), []);
  }

  // ── Summary ──────────────────────────────────────────────────────────
  console.log('\n──────────────────────────────────────────────');
  console.log(`Total: ${total}  Pass: ${passed}  Fail: ${failed}`);
  if (failed > 0) {
    console.log('Failures:');
    for (const f of failures) {
      console.log(`  - ${f.name}`);
    }
    process.exit(1);
  } else {
    console.log('ALL UNIT TESTS PASS');
    process.exit(0);
  }
}

main().catch((e) => {
  console.error('TEST CRASHED:', e);
  process.exit(2);
});
