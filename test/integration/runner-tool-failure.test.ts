import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as path from "node:path";
import { setImmediate } from "node:timers/promises";
import { it } from "node:test";
import { runSubagent } from "../../src/runs/background/subagent-runner.ts";
import { buildAsyncRunnerSteps } from "../../src/runs/background/async-execution.ts";
import { createFakeChildSessions } from "../support/fake-child-session.ts";
import { createTempDir, events, makeAgent, removeTempDir } from "../support/helpers.ts";

it("persists confirmed SDK tool failure across later success without inferring failure from text or silence", async (t) => {
	const dir = createTempDir("runner-tool-failure-");
	t.after(() => removeTempDir(dir));
	t.mock.timers.enable({ apis: ["Date", "setTimeout"], now: Date.now() });
	const successGate = path.join(dir, "success-gate");
	const finishGate = path.join(dir, "finish-gate");
	fs.writeFileSync(path.join(dir, "default-response.json"), JSON.stringify({ steps: [
		{ jsonl: [
			{ type: "tool_execution_start", toolCallId: "success-1", toolName: "bash", args: { command: "echo status" } },
			{ type: "tool_execution_end", toolCallId: "success-1", toolName: "bash", isError: false, result: { content: [{ type: "text", text: "ERROR: this is successful tool output" }] } },
		] },
		{ delay: 31 * 60 * 1000 + 1000, jsonl: [
			{ type: "tool_execution_start", toolCallId: "failed-call-stable-identity", toolName: "bash", args: { command: "missing-command" } },
			{ type: "tool_execution_end", toolCallId: "failed-call-stable-identity", toolName: "bash", isError: true, result: { content: [{ type: "text", text: "\u001b[31mcommand failed\u001b[0m\n" + "details ".repeat(200) }] } },
		] },
		{ waitForPath: successGate, jsonl: [
			{ type: "tool_execution_start", toolCallId: "success-2", toolName: "bash", args: { command: "echo recovered" } },
			{ type: "tool_execution_end", toolCallId: "success-2", toolName: "bash", isError: false, result: { content: [{ type: "text", text: "recovered" }] } },
		] },
		{ waitForPath: finishGate, jsonl: [events.assistantMessage("completed after recovery")] },
	] }));
	const children = createFakeChildSessions(() => dir);
	const built = buildAsyncRunnerSteps("tool-failure", {
		chain: [{ agent: "worker", task: "Run", acceptance: false }], agents: [makeAgent("worker")],
		ctx: { cwd: dir, currentSessionId: "tool-failure-session" }, asyncDir: dir, maxSubagentDepth: 2, executionLifetime: { mode: "unbounded" },
	});
	assert.ok("steps" in built);
	const resultPath = path.join(dir, "result.json");
	let settled = false;
	const running = runSubagent({ id: "tool-failure", sessionId: "tool-failure-session", steps: built.steps, cwd: dir, asyncDir: dir, resultPath, placeholder: "", artifactConfig: { enabled: false }, executionLifetime: { mode: "unbounded" } }, children.factory).finally(() => { settled = true; });
	const read = () => JSON.parse(fs.readFileSync(path.join(dir, "status.json"), "utf8"));
	const flushUntil = async (predicate: () => boolean) => {
		for (let attempt = 0; attempt < 1000 && !predicate(); attempt++) { t.mock.timers.tick(100); await setImmediate(); }
		assert.ok(predicate(), "runner did not reach expected state");
	};
	await flushUntil(() => fs.existsSync(path.join(dir, "status.json")) && read().toolCount === 1);
	assert.equal(read().lastToolFailure, undefined);
	t.mock.timers.tick(31 * 60 * 1000);
	await setImmediate();
	assert.equal(read().lastToolFailure, undefined);
	assert.equal(children.sessions[0]?.aborted, false);
	t.mock.timers.tick(1000);
	await flushUntil(() => Boolean(read().lastToolFailure));
	const failure = read().lastToolFailure;
	assert.equal(failure.kind, "tool-execution-error");
	assert.equal(failure.toolCallId, "failed-call-stable-identity");
	assert.equal(failure.toolName, "bash");
	assert.ok(Number.isFinite(failure.observedAt));
	assert.ok(failure.message.length <= 512);
	assert.match(failure.message, /^command failed/);
	assert.equal(failure.message.includes(String.fromCharCode(27)), false);
	assert.equal(failure.message.includes("\n"), false);
	assert.deepEqual(read().steps[0].lastToolFailure, failure);
	assert.equal(read().state, "running");
	assert.equal(read().deadlineAt, undefined);
	assert.equal(children.sessions[0]?.aborted, false);
	fs.writeFileSync(successGate, "go");
	await flushUntil(() => read().toolCount === 3);
	assert.deepEqual(read().lastToolFailure, failure);
	fs.writeFileSync(finishGate, "go");
	await flushUntil(() => settled);
	await running;
	assert.equal(JSON.parse(fs.readFileSync(resultPath, "utf8")).success, true);
	const restored = JSON.parse(fs.readFileSync(path.join(dir, "status.json"), "utf8"));
	assert.deepEqual(restored.lastToolFailure, failure);
	assert.deepEqual(restored.steps[0].lastToolFailure, failure);
	assert.equal(restored.timedOut, undefined);
});
