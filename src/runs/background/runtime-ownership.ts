import * as fs from "node:fs";
import * as path from "node:path";
import { Type } from "typebox";
import { Compile } from "typebox/compile";
import {
	preflightKernelOwnedProcess, observeKernelOwnedProcess, reconcileKernelOwnedProcess, cancelKernelOwnedProcess,
	type KernelOperationBinding, type KernelOwnedProcessCapability, type KernelOwnedProcessObservation,
} from "../../api/kernel-owned-process.mjs";
import { writePrivateAtomicJson } from "../../shared/atomic-json.ts";
import { readStatus } from "../../shared/utils.ts";

export const FULL_PROCESS_TREE_OWNERSHIP = { version: 1, scope: "owned-process-tree", escapedDescendants: "contained" } as const;
const bindingSchema = Type.Object({ operationId: Type.String(), requestDigest: Type.String(), hostId: Type.String(), bootId: Type.String() });
const nativeOperationSchema = Type.Object({ operationId: Type.String(), digest: Type.String() });
const nativeIntentValidator = Compile(Type.Object({ operationId: Type.String(), digest: Type.String(), runId: Type.String(), kind: Type.Literal("launch") }));
const nativeStopValidator = Compile(Type.Object({ runId: Type.String(), requestedAt: Type.Number() }));
const mappingValidator = Compile(Type.Object({ version: Type.Literal(1), runId: Type.String(), runnerProcessInstanceId: Type.String(), asyncDir: Type.String(), kernelBinding: bindingSchema, nativeOperation: Type.Optional(nativeOperationSchema) }));
const processIdentitySchema = Type.Object({ pid: Type.Integer({ minimum: 1 }), uniqueId: Type.String({ minLength: 1 }), pidVersion: Type.Integer({ minimum: 0 }) });
const identitySchema = Type.Object({ ...bindingSchema.properties, version: Type.Literal(1), backend: Type.Literal("darwin-resource-coalition-v1"), coalitionId: Type.String({ minLength: 1 }), leader: processIdentitySchema });
const kernelTerminalValidator = Compile(Type.Object({
	version: Type.Literal(1), state: Type.Literal("observed"), runId: Type.String({ minLength: 1 }), runnerProcessInstanceId: Type.String({ minLength: 1 }), observedAt: Type.Number(),
	processTreeOwnership: Type.Object({ version: Type.Literal(1), scope: Type.Literal("owned-process-tree"), escapedDescendants: Type.Literal("contained") }),
	kernelBinding: bindingSchema,
	kernelProof: Type.Object({ status: Type.Literal("retired"), operationDirectory: Type.String(), binding: bindingSchema, identity: identitySchema, proof: Type.Object({ ...bindingSchema.properties, kind: Type.Literal("darwin-coalition-retired"), observedAt: Type.String(), identity: identitySchema }) }),
}));

function sameBinding(left: KernelOperationBinding, right: KernelOperationBinding): boolean {
	return left.operationId === right.operationId && left.requestDigest === right.requestDigest && left.hostId === right.hostId && left.bootId === right.bootId;
}

/** Validate persisted kernel evidence before treating it as terminal observation. */
// oxlint-disable-next-line anti-slop/no-unknown-parameters -- This validates the persisted process-terminal boundary.
export function validKernelTerminalProof(value: unknown): boolean {
	if (!kernelTerminalValidator.Check(value)) return false;
	const { kernelBinding, kernelProof } = value;
	return [kernelProof.binding, kernelProof.identity, kernelProof.proof, kernelProof.proof.identity].every((binding) => sameBinding(kernelBinding, binding))
		&& kernelProof.identity.coalitionId === kernelProof.proof.identity.coalitionId
		&& kernelProof.identity.leader.uniqueId === kernelProof.proof.identity.leader.uniqueId
		&& kernelProof.identity.leader.pidVersion === kernelProof.proof.identity.leader.pidVersion
		&& kernelProof.identity.leader.pid === kernelProof.proof.identity.leader.pid
		&& Number.isFinite(Date.parse(kernelProof.proof.observedAt)) && Number.isFinite(value.observedAt);
}

export interface NativeKernelMapping {
	version: 1;
	runId: string;
	runnerProcessInstanceId: string;
	asyncDir: string;
	kernelBinding: KernelOperationBinding;
	nativeOperation?: { operationId: string; digest: string };
}

export type NativeKernelTerminalProof = {
	version: 1;
	state: "observed";
	runId: string;
	runnerProcessInstanceId: string;
	observedAt: number;
	processTreeOwnership: typeof FULL_PROCESS_TREE_OWNERSHIP;
	kernelBinding: KernelOperationBinding;
	kernelProof: KernelOwnedProcessObservation;
	nativeOperation?: { operationId: string; digest: string };
	instances: [];
} | {
	version: 1;
	state: "pending" | "unknown";
	runId: string;
	runnerProcessInstanceId: string;
	reason: string;
};

const probes = new Map<string, { startedAt: number; promise: Promise<KernelOwnedProcessCapability> }>();

/** Bound the control request; a background compiler/probe cannot time out a worker. */
export async function probeRuntimeOwnership(artifactDirectory: string): Promise<KernelOwnedProcessCapability> {
	let probe = probes.get(artifactDirectory);
	if (!probe || Date.now() - probe.startedAt > 60_000) {
		probe = { startedAt: Date.now(), promise: preflightKernelOwnedProcess({ artifactDirectory }).catch((cause) => ({ supported: false, reason: String(cause) })) };
		probes.set(artifactDirectory, probe);
	}
	let timeout: ReturnType<typeof setTimeout> | undefined;
	try {
		return await Promise.race([probe.promise, new Promise<KernelOwnedProcessCapability>((resolve) => {
			timeout = setTimeout(() => resolve({ supported: false, reason: "Kernel ownership preflight is still pending." }), 1_250);
		})]);
	} finally { if (timeout) clearTimeout(timeout); }
}

export function writeNativeKernelMapping(operationDirectory: string, mapping: NativeKernelMapping): void {
	try {
		const intent: unknown = JSON.parse(fs.readFileSync(path.join(path.dirname(operationDirectory), "intent.json"), "utf8"));
		if (nativeIntentValidator.Check(intent) && intent.runId === mapping.runId) mapping.nativeOperation = { operationId: intent.operationId, digest: intent.digest };
	} catch {}
	writePrivateAtomicJson(path.join(operationDirectory, "native-run.json"), mapping);
}

export function readNativeKernelMapping(operationDirectory: string, runId: string): NativeKernelMapping | undefined {
	try {
		const value: unknown = JSON.parse(fs.readFileSync(path.join(operationDirectory, "native-run.json"), "utf8"));
		return mappingValidator.Check(value) && value.runId === runId ? value : undefined;
	} catch { return undefined; }
}

export async function observeNativeKernelRun(operationDirectory: string, runId: string) {
	const mapping = readNativeKernelMapping(operationDirectory, runId);
	let observation = await observeKernelOwnedProcess(operationDirectory);
	const status = mapping ? readStatus(mapping.asyncDir) : null;
	const bindingVerified = Boolean(mapping && observation.binding && sameBinding(mapping.kernelBinding, observation.binding));
	let nativeStopRequested = false;
	try {
		const stop: unknown = JSON.parse(fs.readFileSync(path.join(operationDirectory, "native-stop.json"), "utf8"));
		nativeStopRequested = nativeStopValidator.Check(stop) && stop.runId === runId;
	} catch {}
	const outerStopRequested = fs.existsSync(path.join(path.dirname(operationDirectory), "cancel.json"));
	const runnerFailed = Boolean(observation.signal) || (observation.exitCode !== undefined && observation.exitCode !== null && observation.exitCode !== 0)
		|| (observation.exitCode === 0 && (!status || status.state === "running" || status.state === "queued"));
	if (bindingVerified && (runnerFailed || outerStopRequested || nativeStopRequested || status?.stopped === true) && (observation.status === "active" || observation.status === "pending")) observation = await cancelKernelOwnedProcess(operationDirectory, { deadlineMs: 1_000 });
	else if (mapping && bindingVerified && observation.exitCode === undefined && (observation.status === "pending" || observation.status === "active")) {
		const permissionPath = path.join(mapping.asyncDir, "runner-startup-proceed.json");
		if (!fs.existsSync(permissionPath) && !fs.existsSync(path.join(path.dirname(operationDirectory), "cancel.json")) && !fs.existsSync(path.join(operationDirectory, "native-stop.json"))) {
			writePrivateAtomicJson(permissionPath, { action: "proceed", token: mapping.runnerProcessInstanceId });
		}
		if (observation.status === "pending" && !fs.existsSync(path.join(path.dirname(operationDirectory), "cancel.json")) && !fs.existsSync(path.join(operationDirectory, "native-stop.json"))) observation = await reconcileKernelOwnedProcess(operationDirectory);
	}
	let proof: NativeKernelTerminalProof = { version: 1, state: "unknown", runId, runnerProcessInstanceId: mapping?.runnerProcessInstanceId ?? "unknown", reason: "Kernel ownership mapping or retirement evidence is unavailable." };
	const binding = mapping?.kernelBinding;
	const kernelProof = observation.proof;
	let neverStarted = false;
	if (binding && kernelProof && kernelProof.operationId === binding.operationId && kernelProof.requestDigest === binding.requestDigest && kernelProof.hostId === binding.hostId && kernelProof.bootId === binding.bootId) {
		neverStarted = kernelProof.kind === "never-started" && observation.status === "never-started";
		if (kernelProof.kind === "darwin-coalition-retired" && observation.status === "retired") {
			const observedAt = Date.parse(kernelProof.observedAt);
			if (Number.isFinite(observedAt)) {
				proof = { version: 1, state: "observed", runId, runnerProcessInstanceId: mapping.runnerProcessInstanceId, observedAt, processTreeOwnership: FULL_PROCESS_TREE_OWNERSHIP, kernelBinding: binding, kernelProof: observation, instances: [] };
				if (mapping.nativeOperation) proof.nativeOperation = mapping.nativeOperation;
				if (!validKernelTerminalProof(proof)) proof = { version: 1, state: "unknown", runId, runnerProcessInstanceId: mapping.runnerProcessInstanceId, reason: "Kernel retirement evidence has inconsistent identity bindings." };
			}
		}
	} else if (mapping && bindingVerified && (observation.status === "active" || observation.status === "pending")) {
		proof = { version: 1, state: "pending", runId, runnerProcessInstanceId: mapping.runnerProcessInstanceId, reason: "Owned runner or descendants remain active." };
	}
	if (proof.state === "observed" && mapping) {
		writePrivateAtomicJson(path.join(mapping.asyncDir, "process-terminal.json"), proof);
		if (status) {
			const stopRequested = nativeStopRequested || status.stopped === true || fs.existsSync(path.join(path.dirname(operationDirectory), "cancel.json"));
			writePrivateAtomicJson(path.join(mapping.asyncDir, "status.json"), {
				...status, processTerminal: proof,
				state: stopRequested ? "stopped" : runnerFailed ? "failed" : status.state,
				stopped: stopRequested || status.stopped,
				steps: stopRequested ? status.steps?.map((step) => step.status === "running" || step.status === "pending" ? { ...step, status: "stopped", stopped: true, endedAt: Date.now() } : step) : status.steps,
				endedAt: status.endedAt ?? Date.now(),
				lastUpdate: Date.now(),
			});
		}
	}
	return { mapping, observation, processTerminalProof: proof, neverStarted, runnerFailed, bindingVerified };
}
