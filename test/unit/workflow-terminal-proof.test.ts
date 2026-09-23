import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { describe, it } from "node:test";
import type { ProcessTerminal, WorkflowChildSummary } from "../../src/shared/types.ts";
import { readWorkflowTerminalProof } from "../../src/runs/background/workflow-terminal-proof.ts";

function summary(overrides: Partial<WorkflowChildSummary> = {}): WorkflowChildSummary {
	return {
		version: 1,
		parentToolCallId: "parent-tool-call",
		workflowRunId: "workflow-run",
		inventoryComplete: true,
		workflowState: "completed",
		children: [{ childId: "main", runId: "child-run", state: "completed" }],
		...overrides,
	};
}

function observedChild(): ProcessTerminal {
	return {
		version: 1,
		state: "observed",
		runId: "child-run",
		runnerProcessInstanceId: "runner-1",
		observedAt: 1_234,
		instances: [{ kind: "runner", processInstanceId: "runner-1", closeObservedAt: 1_234, exitCode: 0, signal: null }],
	};
}

function fixture(): { root: string; asyncDir: string } {
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "workflow-terminal-proof-"));
	const asyncDir = path.join(root, "workflow-run");
	fs.mkdirSync(asyncDir, { recursive: true });
	return { root, asyncDir };
}

describe("readWorkflowTerminalProof", () => {
	it("keeps an open inventory pending", () => {
		const { root, asyncDir } = fixture();
		try {
			assert.deepEqual(readWorkflowTerminalProof(asyncDir, summary({ inventoryComplete: false, workflowState: "running" }), 0, 2_000), {
				version: 1,
				kind: "workflow",
				runId: "workflow-run",
				state: "pending",
				dispatchClosed: false,
				reason: "Workflow dispatch is still open.",
			});
		} finally {
			fs.rmSync(root, { recursive: true, force: true });
		}
	});

	it("keeps closed dispatch pending until every child proof exists", () => {
		const { root, asyncDir } = fixture();
		try {
			assert.deepEqual(readWorkflowTerminalProof(asyncDir, summary(), 0, 2_000), {
				version: 1,
				kind: "workflow",
				runId: "workflow-run",
				state: "pending",
				dispatchClosed: true,
				reason: "Child child-run has no observed process-terminal proof.",
			});
		} finally {
			fs.rmSync(root, { recursive: true, force: true });
		}
	});

	it("returns observed after every child has writer-exit evidence", () => {
		const { root, asyncDir } = fixture();
		try {
			const proof = observedChild();
			const childDir = path.join(root, "child-run");
			fs.mkdirSync(childDir);
			fs.writeFileSync(path.join(childDir, "process-terminal.json"), JSON.stringify(proof));
			assert.deepEqual(readWorkflowTerminalProof(asyncDir, summary({ workflowState: "stopped" }), 0, 1_000), {
				version: 1,
				kind: "workflow",
				runId: "workflow-run",
				state: "observed",
				dispatchClosed: true,
				observedAt: 1_234,
				children: [proof],
			});
		} finally {
			fs.rmSync(root, { recursive: true, force: true });
		}
	});

	it("reports host commands as unknown", () => {
		const { root, asyncDir } = fixture();
		try {
			assert.deepEqual(readWorkflowTerminalProof(asyncDir, summary({ children: [] }), 1, 2_000), {
				version: 1,
				kind: "workflow",
				runId: "workflow-run",
				state: "unknown",
				dispatchClosed: true,
				reason: "Workflow host commands have no process-terminal proof.",
			});
		} finally {
			fs.rmSync(root, { recursive: true, force: true });
		}
	});
});
