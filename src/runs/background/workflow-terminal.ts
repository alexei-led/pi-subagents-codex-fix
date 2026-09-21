import * as fs from "node:fs";
import * as path from "node:path";
import { Type } from "typebox";
import { Compile } from "typebox/compile";
import { writePrivateAtomicJson } from "../../shared/atomic-json.ts";
import type { AsyncStatus, ProcessTerminal } from "../../shared/types.ts";
import { readProcessTerminal } from "./process-terminal.ts";
import { validHostStepNodes } from "../shared/host-step-status.ts";

const closureValidator = Compile(Type.Object({
	version: Type.Literal(1), runId: Type.String(), dispatchClosed: Type.Literal(true), closedAt: Type.Number(),
	hostCommands: Type.Integer({ minimum: 0 }),
	children: Type.Array(Type.Object({ runId: Type.Optional(Type.String()), async: Type.Optional(Type.Boolean()), key: Type.Optional(Type.String()) })),
}));

export type WorkflowTerminalProof = {
	version: 1;
	kind: "workflow";
	runId: string;
	state: "observed";
	dispatchClosed: true;
	observedAt: number;
	children: ProcessTerminal[];
} | {
	version: 1;
	kind: "workflow";
	runId: string;
	state: "pending" | "unknown";
	dispatchClosed: boolean;
	reason: string;
};

/** Called only after the workflow promise settles and its dispatch controls close. */
export function writeWorkflowDispatchClosed(asyncDir: string, runId: string, status: AsyncStatus): void {
	writePrivateAtomicJson(path.join(asyncDir, "workflow-dispatch-closed.json"), {
		version: 1,
		runId,
		dispatchClosed: true,
		closedAt: Date.now(),
		hostCommands: Math.max(validHostStepNodes(status.workflowGraph).length, (status.workflow?.trace ?? []).filter((entry) => entry.operation === "host").length),
		children: (status.steps ?? []).map((step) => ({ runId: step.runId, async: step.async, key: step.workflowKey })),
	});
}

/** A settled script is not quiescence until every owned child has exit evidence. */
export function readWorkflowTerminalProof(asyncDir: string, runId: string): WorkflowTerminalProof {
	const unknown = (reason: string, dispatchClosed = false): WorkflowTerminalProof => ({ version: 1, kind: "workflow", runId, state: "unknown", dispatchClosed, reason });
	let value: unknown;
	try { value = JSON.parse(fs.readFileSync(path.join(asyncDir, "workflow-dispatch-closed.json"), "utf8")); }
	catch (error) {
		if (error instanceof Error && "code" in error && error.code === "ENOENT") return { version: 1, kind: "workflow", runId, state: "pending", dispatchClosed: false, reason: "Workflow dispatch closure has not been observed." };
		return unknown(`Workflow closure could not be read: ${String(error)}`);
	}
	if (!closureValidator.Check(value) || value.runId !== runId) return unknown("Invalid workflow dispatch closure record.");
	if (value.hostCommands > 0) return unknown("Host command process-tree containment remains unverified.", true);
	const children: ProcessTerminal[] = [];
	for (const child of value.children) {
		if (child.async !== true || !child.runId || path.basename(child.runId) !== child.runId) return unknown("A workflow child lacks a durable runner identity.", true);
		const proof = readProcessTerminal(path.join(path.dirname(asyncDir), child.runId), { runId: child.runId });
		if (proof?.state !== "observed" || proof.instances.some((instance) => instance.kind === "pi-writer" && (instance.processTree.state !== "observed" || instance.processTree.mechanism === "posix-process-group"))) return unknown(`Child ${child.runId} process-tree containment remains unverified.`, true);
		children.push(proof);
	}
	if (children.length > 0) return unknown("The runtime does not provide full descendant containment evidence for workflow children.", true);
	return { version: 1, kind: "workflow", runId, state: "observed", dispatchClosed: true, observedAt: Date.now(), children };
}
