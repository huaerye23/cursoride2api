#!/usr/bin/env node
// Needle-in-the-haystack effective-context test for any ratlc model group.
//
// Runs against the local pool proxy at http://127.0.0.1:4242. Generates
// a haystack of N tokens, inserts a unique "OPAL-XX-NNNNN-WREN" code at
// a configurable depth, then asks the model to retrieve the code. Counts
// hits across a (size, depth) grid.
//
// See NIAH_RESULTS.md for the 2026-05-15 baseline run on
// claude-opus-4-7-max-fast.
//
// Env vars (all optional):
//   NIAH_API_URL   default http://127.0.0.1:4242/v1/messages
//   NIAH_API_KEY   default ratlc-pool
//   NIAH_MODEL     default claude-opus-4-7-max-fast
//   NIAH_SIZES     comma-separated target-token counts.
//                  default 10000,50000,200000,500000
//   NIAH_DEPTHS    comma-separated fractions (0..1).
//                  default 0.1,0.5,0.9

const API_URL = process.env.NIAH_API_URL || 'http://127.0.0.1:4242/v1/messages';
const API_KEY = process.env.NIAH_API_KEY || 'ratlc-pool';
const MODEL   = process.env.NIAH_MODEL   || 'claude-opus-4-7-max-fast';
const SIZES_TOKENS = (process.env.NIAH_SIZES || '10000,50000,200000,500000')
  .split(',').map((s) => parseInt(s.trim(), 10)).filter((n) => Number.isFinite(n) && n > 0);
const DEPTHS = (process.env.NIAH_DEPTHS || '0.1,0.5,0.9')
  .split(',').map((s) => parseFloat(s.trim())).filter((n) => Number.isFinite(n) && n >= 0 && n <= 1);

// 50 varied English sentences. Filler is intentionally non-repeating-sounding
// so the model can't pattern-match the needle by uniqueness alone.
const FILLERS = [
  "The library was filled with the scent of old leather and parchment.",
  "She walked through the museum and noticed brushwork on each painting.",
  "The mountain trail wound steeply upward with views of the valley below.",
  "Coffee shops in the old quarter served espresso in chipped porcelain cups.",
  "The conductor raised her baton and the orchestra fell into expectant silence.",
  "Migrating birds traced lazy arcs across the autumn sky each evening.",
  "Wooden crates stamped with faded customs marks lined the warehouse walls.",
  "The lighthouse keeper trimmed the wicks twice each night during winter storms.",
  "Children chased fireflies through the long grass behind the schoolhouse.",
  "She studied the old map carefully, looking for the river that no longer existed.",
  "The bakery opened at four in the morning, scenting the alley with yeast.",
  "Snow piled in soft drifts against the wooden fence of the small cottage.",
  "Sailing ships once anchored where the highway now ran along the coast.",
  "The watchmaker bent over his loupe, replacing a hairspring thinner than thread.",
  "Stones in the riverbed were polished smooth by centuries of patient water.",
  "Lanterns swayed gently above the night market as vendors called their prices.",
  "Field crickets chirped in unsteady rhythm during the warm August nights.",
  "The cartographer added a small notation in red ink beside the disputed border.",
  "Steam rose from the kettle as she poured tea into two mismatched cups.",
  "The detective set down his notebook and stared at the window for a long while.",
  "Lavender grew in untidy rows along the south wall of the kitchen garden.",
  "Boys raced bicycles down the steep hill toward the dusty cricket pitch.",
  "Faded posters peeled from the bricks above the entrance to the cinema.",
  "Reeds bent low across the pond when the wind shifted at dusk each evening.",
  "She kept her mother's pearls in a velvet box on the highest shelf.",
  "The bridge had been rebuilt three times since the great flood of the prior century.",
  "Spices in burlap sacks lined the merchant's stall in the spice quarter.",
  "Pigeons gathered in the plaza whenever the old man tossed crumbs from his bench.",
  "The clocktower struck the hour and a flock of starlings exploded from its eaves.",
  "Apples in the orchard ripened late that year due to a cold and rainy spring.",
  "The seamstress measured the cuff carefully before reaching for her sharpest scissors.",
  "Telegraph wires hummed faintly when the wind blew across the empty prairie.",
  "Children pressed their faces to the bakery window to watch the pastry chef work.",
  "Owls hunted over the meadow each night, their wings silent in the cold air.",
  "The blacksmith's forge glowed orange long after the sun had set behind the hills.",
  "Books were stacked in precarious towers beside the chair near the fireplace.",
  "Boats returning from the morning catch unloaded their crates on the wooden quay.",
  "The schoolmistress read aloud from a worn copy of an Edwardian adventure novel.",
  "Vines climbed the brickwork of the old hotel where her grandfather once stayed.",
  "Frost traced delicate patterns on the windowpane each morning in late autumn.",
  "The astronomer adjusted the small mirror and waited for the cloud to drift past.",
  "Bells rang from the abbey at first light to summon the village to morning prayer.",
  "Apprentices learned to weld in the dim cavernous space beneath the train yard.",
  "She kept her grandmother's recipes on index cards inside a tin biscuit box.",
  "The harbor pilot guided each visiting vessel through the treacherous narrow channel.",
  "Stained glass panels cast colored squares across the marble floor of the chapel.",
  "Apricots ripened on the south-facing wall where the bricks held the day's heat.",
  "She traced the constellations slowly with her finger and named each one in turn.",
  "Coal smoke from the engine drifted lazily over the rooftops of the river town.",
  "Old men played dominoes in the shaded courtyard of the cafe each afternoon.",
];

function generateHaystack(targetChars, needle, depth) {
  let text = '';
  let i = 0;
  while (text.length < targetChars) {
    text += FILLERS[i % FILLERS.length] + ' ';
    i++;
  }
  text = text.slice(0, targetChars);
  const pos = Math.floor(text.length * depth);
  let insertPos = pos;
  while (insertPos < text.length && text[insertPos] !== ' ') insertPos++;
  return text.slice(0, insertPos) + ' ' + needle + ' ' + text.slice(insertPos);
}

function makeNeedle(idx) {
  const tag = String.fromCharCode(65 + (idx % 26)) + String.fromCharCode(65 + ((idx * 7) % 26));
  const num = (idx * 73 + 1009) % 90000 + 10000;
  return {
    phrase: `IMPORTANT: The hidden one-time access code for this document is OPAL-${tag}-${num}-WREN.`,
    code: `OPAL-${tag}-${num}-WREN`,
  };
}

const QUESTION =
  'What is the hidden one-time access code mentioned in the document above? ' +
  'Reply with ONLY the code (it starts with OPAL- and ends with -WREN). Nothing else.';

async function runOne(size, depth, idx) {
  const { phrase, code } = makeNeedle(idx);
  const haystack = generateHaystack(size * 4, phrase, depth);
  const userMessage =
    `Read the document below carefully, then answer the question that follows.\n\n` +
    `===== DOCUMENT BEGIN =====\n${haystack}\n===== DOCUMENT END =====\n\n` +
    `QUESTION: ${QUESTION}`;

  const start = Date.now();
  let res;
  try {
    res = await fetch(API_URL, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': API_KEY,
        'anthropic-version': '2023-06-01',
      },
      body: JSON.stringify({
        model: MODEL,
        max_tokens: 128,
        messages: [{ role: 'user', content: userMessage }],
      }),
      signal: AbortSignal.timeout(420_000),
    });
  } catch (e) {
    return { size, depth, code, error: `fetch: ${e.message}`, latencyMs: Date.now() - start };
  }
  if (!res.ok) {
    const body = await res.text().catch(() => '');
    return { size, depth, code, error: `http ${res.status}: ${body.slice(0, 200)}`, latencyMs: Date.now() - start };
  }

  let body = '';
  let inputTokens = 0;
  let outputTokens = 0;
  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let buf = '';
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    buf += decoder.decode(value, { stream: true });
    let idx2;
    while ((idx2 = buf.indexOf('\n')) >= 0) {
      const line = buf.slice(0, idx2);
      buf = buf.slice(idx2 + 1);
      if (!line.startsWith('data: ')) continue;
      try {
        const ev = JSON.parse(line.slice(6));
        if (ev.type === 'content_block_delta' && ev.delta?.type === 'text_delta') body += ev.delta.text;
        if (ev.message?.usage?.input_tokens) inputTokens = ev.message.usage.input_tokens;
        if (ev.usage?.output_tokens) outputTokens = ev.usage.output_tokens;
      } catch { /* ignore mid-stream JSON noise */ }
    }
  }

  return {
    size,
    depth,
    code,
    latencyMs: Date.now() - start,
    inputTokens,
    outputTokens,
    response: body.trim(),
    match: body.includes(code),
  };
}

async function main() {
  if (SIZES_TOKENS.length === 0 || DEPTHS.length === 0) {
    console.error('niah-test: empty SIZES or DEPTHS; check NIAH_SIZES / NIAH_DEPTHS env vars');
    process.exit(1);
  }
  console.log(`niah-test for ${MODEL} via ${API_URL}`);
  console.log(`sizes (target tokens): ${SIZES_TOKENS.join(', ')}`);
  console.log(`depths: ${DEPTHS.map((d) => `${(d * 100).toFixed(0)}%`).join(', ')}`);
  console.log('');

  const results = [];
  let idx = 0;
  for (const size of SIZES_TOKENS) {
    for (const depth of DEPTHS) {
      idx++;
      process.stdout.write(`#${idx} size=${size} depth=${(depth * 100).toFixed(0)}%… `);
      const r = await runOne(size, depth, idx);
      results.push(r);
      if (r.error) {
        console.log(`ERR ${r.latencyMs}ms ${r.error}`);
      } else {
        const flag = r.match ? '✓' : '✗';
        console.log(`${flag} input=${r.inputTokens}t out=${r.outputTokens}t ${r.latencyMs}ms reply="${r.response.slice(0, 70)}"`);
      }
    }
  }

  console.log('');
  console.log('=== retrieval grid (✓=hit ✗=miss !=error) ===');
  const header = 'size (target →actual)'.padEnd(28) + DEPTHS.map((d) => `${(d * 100).toFixed(0)}%`.padStart(8)).join('');
  console.log(header);
  for (const size of SIZES_TOKENS) {
    const sample = results.find((r) => r.size === size && !r.error);
    const actual = sample ? `→${sample.inputTokens}` : '';
    let row = `${size.toLocaleString()} ${actual}`.padEnd(28);
    for (const depth of DEPTHS) {
      const r = results.find((x) => x.size === size && x.depth === depth);
      const mark = r?.error ? '!' : r?.match ? '✓' : '✗';
      row += mark.padStart(8);
    }
    console.log(row);
  }

  console.log('');
  console.log('=== latency (ms) ===');
  console.log(header);
  for (const size of SIZES_TOKENS) {
    let row = size.toLocaleString().padEnd(28);
    for (const depth of DEPTHS) {
      const r = results.find((x) => x.size === size && x.depth === depth);
      row += (r ? String(r.latencyMs) : '-').padStart(8);
    }
    console.log(row);
  }

  const hits = results.filter((r) => r.match).length;
  const errs = results.filter((r) => r.error).length;
  console.log('');
  console.log(`SUMMARY: ${hits}/${results.length} hits, ${errs} errors`);
}

main().catch((e) => { console.error('fatal:', e); process.exit(1); });
