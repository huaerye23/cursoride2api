# Needle-in-Haystack — effective context for `claude-opus-4-7-max-fast`

Measured 2026-05-15 via the ratlc pool's local proxy
(`http://127.0.0.1:4242/v1/messages`) against the
`claude-opus-4-7-max-fast` model group. 24 calls across 8 context
sizes × 3 needle-depth positions (10% / 50% / 90% through the
haystack).

## TL;DR

**Effective context window ≈ 600k tokens.** Despite the model
identifier suggesting 1M context, Cursor's `max-fast` variant has a
hard cliff between 650k and 700k tokens — above that, the model
returns the literal string `READY` (the bridge-worker priming
acknowledgment from `bridge-worker.mjs:131`) instead of answering the
user's question.

## Methodology

- **Haystack:** 50 unique varied English sentences cycled in order
  until a target character count is reached (~4 chars/token). The
  filler is intentionally non-repetitive enough that the model can't
  pattern-match the needle by uniqueness alone.
- **Needle:** an "IMPORTANT" sentence containing a code of the form
  `OPAL-XX-NNNNN-WREN`. Different code per test so cache effects can't
  contaminate the result.
- **Question:** "What is the hidden one-time access code mentioned in
  the document above? Reply with ONLY the code (it starts with OPAL-
  and ends with -WREN). Nothing else."
- **Grader:** exact substring match — the response must contain the
  full `OPAL-XX-NNNNN-WREN` literal.
- **Streaming SSE parsed for `input_tokens` (reported by upstream),
  `output_tokens`, and the concatenated text response.

## Results

| target tokens | actual input_tokens | depth 10% | depth 50% | depth 90% | latency (s) |
|---:|---:|:-:|:-:|:-:|:-:|
| 10,000 | 10,094 | ✓ | ✓ | ✓ | 6.7 / 7.1 / 6.8 |
| 50,000 | 50,094 | ✓ | ✓ | ✓ | 8.2 / 8.4 / 8.7 |
| 200,000 | 200,094 | ✓ | ✓ | ✓ | 16.8 / 20.5 / 16.6 |
| 500,000 | 500,094 | ✓ | ✓ | ✓ | 34.5 / 27.3 / 40.9 |
| **600,000** | **600,094** | **✓** | **✓** | **✓** | **34.5 / 30.2 / 30.6** |
| 650,000 | 650,094 | ✓ | ✓ | **✗** | 31.8 / 32.2 / 30.9 |
| 700,000 | 700,094 | ✗ | ✗ | ✗ | 138.8 / 117.2 / 36.5 |
| 950,000 | 950,094 | ✗ | ✗ | ✗ | 22.1 / 91.5 / 19.5 |

21/24 hits overall. The wall is **between 650k and 700k**:

- ≤ 600k: 100% retrieval at every position tested.
- 650k: edge failure — the needle at depth 90% (near the end of the
  haystack) gets cut off, the other two positions still succeed.
- ≥ 700k: complete failure at every position.

## Failure-mode analysis

Above the cliff every miss returns the same response: the literal
string `READY`. That string is what the channel is primed to emit at
open time. Looking at `bridge-worker.mjs:131`:

```js
lines.push('Reply with exactly "READY" to acknowledge, then call bajie_yield.');
```

The priming context lives at the START of the conversation; the user's
haystack+question is what gets truncated. The model is literally
answering the priming prompt instead of the user message because
that's what's left after Cursor truncates the input.

The latency pattern corroborates this hypothesis:

- ≤ 600k: ~30s response time, scales roughly linearly with input
  tokens (~55 µs/token marginal).
- 700k+: erratic 19s–139s, consistent with Cursor handling oversize
  inputs through different code paths depending on internal state.

Position-sensitivity also matches: 650k @ depth 90% fails first
because a needle near the *end* of the haystack is exactly what gets
chopped if Cursor truncates from the tail to fit a fixed window.

## Implications

- **Practical context budget: ~600k tokens** including priming + tool
  definitions overhead (~94 tokens in this pool's configuration). For
  caller workloads, treat **~598k as the safe payload ceiling**.
- **The model identifier `[1m]` reflects the underlying Claude
  capability, not what Cursor's max-fast variant actually delivers.**
  This may differ for other Cursor model variants (e.g.
  `claude-opus-4-7-thinking-max-fast`) — those would need their own
  NIAH runs.
- **Silent failure is the danger:** the model doesn't error out, it
  just answers a different (the priming) question. Callers above the
  cliff would see "model returned a one-word response that doesn't
  address my prompt." Worth a length-guard at the pool's api-server
  if this becomes a problem in practice.

## Reproducing

The test script lives at `scaffolding/pool/niah-test.mjs`. Configure
sizes/depths via env vars:

```bash
# default run — 4 sizes (10k/50k/200k/500k) × 3 depths (10/50/90%)
node scaffolding/pool/niah-test.mjs

# custom sizes (comma-separated target tokens)
NIAH_SIZES=600000,650000,700000 node scaffolding/pool/niah-test.mjs

# custom model
NIAH_MODEL=claude-opus-4-7-thinking-max-fast node scaffolding/pool/niah-test.mjs

# custom depths (comma-separated fractions)
NIAH_DEPTHS=0.05,0.25,0.5,0.75,0.95 node scaffolding/pool/niah-test.mjs
```

Requires the pool to be up with at least one ready channel on the
target model group (`ratlc up`, wait for `ready >= 1`).
