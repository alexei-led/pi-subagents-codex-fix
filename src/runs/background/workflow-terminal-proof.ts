import * as path from "node:path";
import type { WorkflowChildSummary, WorkflowTerminalProof } from "../../shared/types.ts";
import { readProcessTerminal } from "./process-terminal.ts";

const TERMINAL_WORKFLOW_STATES = new Set<WorkflowChildSummary["workflowState"]>(["completed", "failed", "stopped"]);

function unresolved(runId: string, state: "pending" | "unknown", dispatchClosed: boolean, reason: string): WorkflowTerminalProof {
	return { version: 1, kind: "workflow", runId, state, dispatchClosed, reason };
}

/** A persistent workflow host is terminal only after dispatch closes and every child has writer-exit evidence. */
export function readWorkflowTerminalProof(asyncDir: string, summary: WorkflowChildSummary, hostCommandCount: number, closedAt: number): WorkflowTerminalProof {
	const runId = summary.workflowRunId;
	if (!summary.inventoryComplete || !TERMINAL_WORKFLOW_STATES.has(summary.workflowState)) {
		return unresolved(runId, "pending", false, "Workflow dispatch is still open.");
	}
	if (hostCommandCount > 0) {
		return unresolved(runId, "unknown", true, "Workflow host commands have no process-terminal proof.");
	}
	const children = [];
	let observedAt = closedAt;
	for (const child of summary.children) {
		if (!child.runId || path.basename(child.runId) !== child.runId) {
			return unresolved(runId, "unknown", true, `Workflow child ${child.childId} has no durable run identity.`);
		}
		const proof = readProcessTerminal(path.join(path.dirname(asyncDir), child.runId), { runId: child.runId });
		if (!proof || proof.state === "pending") {
			return unresolved(runId, "pending", true, `Child ${child.runId} has no observed process-terminal proof.`);
		}
		if (proof.state !== "observed") {
			return unresolved(runId, "unknown", true, `Child ${child.runId} process-terminal proof is ${proof.state}.`);
		}
		observedAt = Math.max(observedAt, proof.observedAt);
		children.push(proof);
	}
	return { version: 1, kind: "workflow", runId, state: "observed", dispatchClosed: true, observedAt, children };
}
