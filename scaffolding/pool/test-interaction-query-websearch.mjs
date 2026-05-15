#!/usr/bin/env node
// Unit test for handleInteractionQuery WebSearch approval.
//
// Verifies the two branches that translate-mode added:
//   passthroughNativeTools=false → Rejected (legacy behavior preserved)
//   passthroughNativeTools=true  → Approved (Cursor backend will do the search)
//
// Also confirms CURSOR_LOG_INTERACTION=1 emits the expected trace line so
// we can grep for "action=approve" / "action=reject" in real proxy logs.

import { createRequire } from 'node:module';
const require = createRequire(import.meta.url);

const ca = require('/root/git_farm/cursoride2api_ratlc/cursoride2api/src/cursor-agent.js');

async function run() {
  // Load the proto module the same way startConversation does.
  await ca.loadProto();
  const { create, toBinary, fromBinary } = require('@bufbuild/protobuf');
  const agent = await import('/root/git_farm/cursoride2api_ratlc/cursoride2api/src/proto/agent_pb.mjs');

  // Build a synthetic webSearchRequestQuery.
  const args = create(agent.WebSearchArgsSchema, { searchTerm: 'latest Claude 5 release', toolCallId: 'tc_test_123' });
  const query = create(agent.WebSearchRequestQuerySchema, { args });
  const iq = create(agent.InteractionQuerySchema, {
    id: 4242,
    query: { case: 'webSearchRequestQuery', value: query },
  });

  // Run each scenario, capturing what gets sent on the wire.
  function runOnce(passthroughNativeTools) {
    const captured = [];
    const sendBinaryFrame = (bytes) => captured.push(bytes);
    // Capture console.log to verify the trace line.
    const realLog = console.log;
    const lines = [];
    console.log = (...a) => lines.push(a.join(' '));
    process.env.CURSOR_LOG_INTERACTION = '1';
    try {
      ca.handleInteractionQuery(iq, sendBinaryFrame, { passthroughNativeTools });
    } finally {
      console.log = realLog;
      delete process.env.CURSOR_LOG_INTERACTION;
    }
    if (captured.length !== 1) throw new Error(`expected 1 frame, got ${captured.length}`);
    const wrapper = fromBinary(agent.AgentClientMessageSchema, captured[0]);
    if (wrapper.message?.case !== 'interactionResponse') {
      throw new Error(`expected interactionResponse wrapper, got ${wrapper.message?.case}`);
    }
    const ir = wrapper.message.value;
    if (ir.id !== iq.id) throw new Error(`response.id mismatch: ${ir.id} !== ${iq.id}`);
    const resultCase = ir.result?.case;
    if (resultCase !== 'webSearchRequestResponse') {
      throw new Error(`expected webSearchRequestResponse, got ${resultCase}`);
    }
    const innerCase = ir.result.value?.result?.case;
    return { innerCase, trace: lines.find((l) => l.startsWith('[cursor-agent] interactionQuery')) };
  }

  // Scenario 1: passthrough off → reject.
  const off = runOnce(false);
  if (off.innerCase !== 'rejected') throw new Error(`passthrough=off should reject, got ${off.innerCase}`);
  if (!off.trace || !off.trace.includes('action=reject')) {
    throw new Error(`passthrough=off trace missing 'action=reject': ${off.trace}`);
  }
  console.log('OK passthrough=false → rejected; trace:', off.trace);

  // Scenario 2: passthrough on → approve.
  const on = runOnce(true);
  if (on.innerCase !== 'approved') throw new Error(`passthrough=on should approve, got ${on.innerCase}`);
  if (!on.trace || !on.trace.includes('action=approve')) {
    throw new Error(`passthrough=on trace missing 'action=approve': ${on.trace}`);
  }
  if (!on.trace.includes('search_term="latest Claude 5 release"')) {
    throw new Error(`passthrough=on trace missing search_term: ${on.trace}`);
  }
  console.log('OK passthrough=true → approved; trace:', on.trace);

  // Scenario 3: ExaSearch (passthrough on) should still reject — we only
  // approve WebSearch.
  const exaIq = create(agent.InteractionQuerySchema, {
    id: 9999,
    query: { case: 'exaSearchRequestQuery', value: create(agent.ExaSearchRequestQuerySchema, {}) },
  });
  const captured = [];
  ca.handleInteractionQuery(exaIq, (b) => captured.push(b), { passthroughNativeTools: true });
  const exaWrap = fromBinary(agent.AgentClientMessageSchema, captured[0]);
  const exaInner = exaWrap.message?.value?.result?.value?.result?.case;
  if (exaInner !== 'rejected') throw new Error(`exa with passthrough=on should still reject, got ${exaInner}`);
  console.log('OK exaSearch passthrough=true → still rejected (scope guard)');

  console.log('\nAll assertions passed.');
}

run().catch((e) => {
  console.error('FAIL:', e.message);
  console.error(e.stack);
  process.exit(1);
});
