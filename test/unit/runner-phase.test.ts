import assert from "node:assert/strict";
import { it } from "node:test";
import { observeRunnerPhase } from "../../src/runs/shared/runner-phase.ts";

it("distinguishes observed provider, stream, tool and supervisor phases without treating silence as failure", () => {
	assert.equal(observeRunnerPhase({ type: "turn_start" }), "model_request");
	assert.equal(observeRunnerPhase({ type: "message_update" }, "model_request"), "model_stream");
	assert.equal(observeRunnerPhase({ type: "tool_execution_start" }, "model_stream", { awaitingInput: false }), "tool_in_flight");
	assert.equal(observeRunnerPhase({ type: "tool_execution_start" }, "tool_in_flight", { awaitingInput: true }), "awaiting_input");
	assert.equal(observeRunnerPhase({ type: "diagnostic" }, "model_request"), "model_request");
	assert.equal(observeRunnerPhase({ type: "tool_execution_end" }, "tool_in_flight"), "unknown");
	assert.equal(observeRunnerPhase({ type: "agent_end" }, "model_stream"), "unknown");
});
