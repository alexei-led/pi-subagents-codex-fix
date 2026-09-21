import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { it } from "node:test";
import { readWorkflowTerminalProof, writeWorkflowDispatchClosed } from "../../src/runs/background/workflow-terminal.ts";
import type { AsyncStatus } from "../../src/shared/types.ts";

it("requires a durable closed scheduler and every child process-tree exit", () => {
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "workflow-terminal-"));
	const directory = path.join(root, "workflow");
	try {
		assert.equal(readWorkflowTerminalProof(directory, "workflow").state, "pending");
		const status: AsyncStatus = { runId: "workflow", state: "complete", startedAt: 1, steps: [{ agent: "worker", runId: "child", async: true, status: "complete" }] };
		writeWorkflowDispatchClosed(directory, "workflow", status);
		assert.equal(readWorkflowTerminalProof(directory, "workflow").state, "unknown");
		fs.mkdirSync(path.join(root, "child"));
		const proof = { version: 1, state: "observed", runId: "child", runnerProcessInstanceId: "runner", observedAt: 3, instances: [{ kind: "runner", processInstanceId: "runner", closeObservedAt: 3, exitCode: 0, signal: null }, { kind: "pi-writer", processInstanceId: "writer", attempt: 0, closeObservedAt: 2, exitCode: 0, signal: null, processTree: { state: "observed", mechanism: "posix-process-group", processGroupId: 123, verifiedAt: 2 } }] };
		fs.writeFileSync(path.join(root, "child", "process-terminal.json"), JSON.stringify(proof));
		const observed = readWorkflowTerminalProof(directory, "workflow");
		assert.equal(observed.state, "unknown");
		assert.equal(observed.dispatchClosed, true);
		assert.equal(readWorkflowTerminalProof(directory, "another-workflow").state, "unknown");
		fs.writeFileSync(path.join(root, "child", "process-terminal.json"), JSON.stringify({ ...proof, instances: [{ ...proof.instances[1], processTree: { state: "unknown", reason: "verification-failed" } }, proof.instances[0]] }));
		assert.equal(readWorkflowTerminalProof(directory, "workflow").state, "unknown");
		fs.writeFileSync(path.join(root, "child", "process-terminal.json"), JSON.stringify({ ...proof, instances: [proof.instances[0]] }));
		assert.equal(readWorkflowTerminalProof(directory, "workflow").state, "unknown");
	} finally { fs.rmSync(root, { recursive: true, force: true }); }
});

it("observes an empty workflow only after durable scheduler closure", () => {
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "workflow-empty-proof-"));
	try {
		writeWorkflowDispatchClosed(root, "workflow", { runId: "workflow", state: "complete", startedAt: 1, steps: [] });
		const observed = readWorkflowTerminalProof(root, "workflow");
		assert.equal(observed.state, "observed");
		if (observed.state === "observed") assert.deepEqual(observed.children, []);
	} finally { fs.rmSync(root, { recursive: true, force: true }); }
});

it("does not mistake a host-command workflow for an empty workflow", () => {
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "workflow-host-proof-"));
	try {
		writeWorkflowDispatchClosed(root, "workflow", { runId: "workflow", mode: "workflow", state: "complete", startedAt: 1, steps: [], workflow: { trace: [{ key: "check", operation: "host", state: "completed" }], emits: [], console: [] } });
		const proof = readWorkflowTerminalProof(root, "workflow");
		assert.equal(proof.state, "unknown");
		assert.equal(proof.dispatchClosed, true);
	} finally { fs.rmSync(root, { recursive: true, force: true }); }
});

it("does not equate a settled foreground child with durable process exit", () => {
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "workflow-foreground-proof-"));
	try {
		writeWorkflowDispatchClosed(root, "workflow", { runId: "workflow", state: "complete", startedAt: 1, steps: [{ agent: "worker", async: false, status: "complete" }] });
		assert.equal(readWorkflowTerminalProof(root, "workflow").state, "unknown");
	} finally { fs.rmSync(root, { recursive: true, force: true }); }
});
