import * as path from "node:path";
import { createHash } from "node:crypto";
import type { AgentToolResult } from "@earendil-works/pi-agent-core";
import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { Compile } from "typebox/compile";
import { Type } from "typebox";
import { resolveAsyncRunLocation } from "../runs/background/async-resume.ts";
import { deliverStopRequest, requestAsyncSteer } from "../runs/background/control-channel.ts";
import { reconcileAsyncRun } from "../runs/background/stale-run-reconciler.ts";
import type { SubagentParamsLike } from "../runs/foreground/subagent-executor.ts";
import { resolveCurrentSessionId } from "../shared/session-identity.ts";
import {
	type AsyncJobStep,
	type Details,
	type SubagentState,
	type TokenUsage,
	DIRS,
	SUBAGENT_ASYNC_COMPLETE_EVENT,
	SUBAGENT_CHILD_STATUS_EVENT,
	SUBAGENT_PROCESS_TERMINAL_EVENT,
	SUBAGENT_LIFECYCLE_ARTIFACT_VERSION,
	type SubagentChildStatusEvent,
} from "../shared/types.ts";
import { sanitizeDisplayText, truncateDisplayText } from "../shared/display-text.ts";
import { getAgentDir, readStatus } from "../shared/utils.ts";
import { SubagentParams } from "./schemas.ts";
import { normalizePublicSubagentExecution } from "./public-execution.ts";
import { ASYNC_STATUS_SNAPSHOT_KIND, ASYNC_STATUS_SNAPSHOT_VERSION, buildAsyncStatusSnapshotForState } from "../runs/background/async-status-snapshot.ts";
import { isStoppableAsyncStatusStep, resolveAsyncStatusChild, stopStoppableAsyncStatusChildren, type ResolvedAsyncStatusChild } from "../runs/shared/child-identity.ts";
import { DurableOperation, operationRequestHash } from "../runs/background/durable-operation.ts";
import { readProcessTerminal } from "../runs/background/process-terminal.ts";
import { readWorkflowTerminalProof } from "../runs/background/workflow-terminal.ts";
import { FULL_PROCESS_TREE_OWNERSHIP, observeNativeKernelRun, readNativeKernelMapping, probeRuntimeOwnership } from "../runs/background/runtime-ownership.ts";
import { cancelKernelOwnedProcess, type KernelOwnedProcessCapability } from "../api/kernel-owned-process.mjs";
import { parseOwnedWorkflow, validateOwnedWorkflowPublicFields } from "../runs/shared/owned-workflow.ts";

export const SUBAGENT_RPC_PROTOCOL_VERSION = 1;
export const SUBAGENT_RPC_REQUEST_EVENT = "subagents:rpc:v1:request";
export const SUBAGENT_RPC_READY_EVENT = "subagents:rpc:v1:ready";
export const SUBAGENT_RPC_REPLY_EVENT_PREFIX = "subagents:rpc:v1:reply:";

export const SUBAGENT_RPC_METHODS = ["ping", "status", "manage", "spawn", "lookup", "cancel", "diagnose", "steer", "interrupt", "stop", "resume"] as const;
export type SubagentRpcMethod = typeof SUBAGENT_RPC_METHODS[number];

export interface SubagentRpcRequestEnvelope {
	version: typeof SUBAGENT_RPC_PROTOCOL_VERSION;
	requestId: string;
	method: SubagentRpcMethod;
	params?: unknown;
	source?: {
		extension?: string;
		[key: string]: unknown;
	};
}

export type SubagentRpcReplyEnvelope<T = unknown> = {
	version: typeof SUBAGENT_RPC_PROTOCOL_VERSION;
	requestId: string;
	method?: SubagentRpcMethod;
	success: true;
	data: T;
} | {
	version: typeof SUBAGENT_RPC_PROTOCOL_VERSION;
	requestId: string;
	method?: SubagentRpcMethod;
	success: false;
	error: {
		code: SubagentRpcErrorCode;
		message: string;
	};
};

export const SUBAGENT_RPC_MANAGEMENT_ACTIONS = [
	"schedule.list",
	"schedule.show",
	"schedule.history",
	"schedule.pause",
	"schedule.resume",
	"schedule.run",
	"schedule.delete",
] as const;

type SubagentRpcManagementAction = typeof SUBAGENT_RPC_MANAGEMENT_ACTIONS[number];

type SubagentRpcErrorCode =
	| "invalid_request"
	| "invalid_params"
	| "unsupported_version"
	| "unsupported_method"
	| "no_active_session"
	| "execution_failed"
	| "not_found"
	| "invalid_state";

interface EventBus {
	on(event: string, handler: (data: unknown) => void): (() => void) | void;
	emit(event: string, data: unknown): void;
}

export interface SubagentRpcFleetEntry {
	/** Opaque key for client-side reconciliation; never a run or async identifier. */
	key: string;
	/** Resolved child agent/role name. */
	agent: string;
	role?: string;
	model?: string;
	effort?: string;
	startedAt: number;
	tokens: TokenUsage;
	goal?: string;
}

export interface SubagentRpcFleetStatus {
	version: 1;
	entries: SubagentRpcFleetEntry[];
	/** Total active children before the bounded entries window. */
	totalActive: number;
	topLevelAsyncCapacity: { used: number; limit: number };
	omitted: number;
}

const MAX_FLEET_ENTRIES = 16;
const MAX_FLEET_CANDIDATES = 256;
const MAX_AGENT_LENGTH = 96;
const MAX_GOAL_LENGTH = 512;
const MAX_METADATA_LENGTH = 128;

function displayText(value: unknown, maxLength: number): string | undefined {
	if (typeof value !== "string") return undefined;
	const normalized = sanitizeDisplayText(value.slice(0, 4_096));
	return normalized ? truncateDisplayText(normalized, maxLength) : undefined;
}

function publicTokens(value: unknown): TokenUsage {
	const record = isRecord(value) ? value : {};
	const count = (field: "input" | "output" | "total") => {
		const raw = record[field];
		return typeof raw === "number" && Number.isFinite(raw) && raw >= 0
			? Math.min(Number.MAX_SAFE_INTEGER, Math.floor(raw))
			: 0;
	};
	const input = count("input");
	const output = count("output");
	const sum = Math.min(Number.MAX_SAFE_INTEGER, input + output);
	const optionalCount = (field: "window" | "windowPeak") => {
		const raw = record[field];
		return typeof raw === "number" && Number.isFinite(raw) && raw >= 0
			? Math.min(Number.MAX_SAFE_INTEGER, Math.floor(raw))
			: undefined;
	};
	const window = optionalCount("window");
	const windowPeak = optionalCount("windowPeak");
	return {
		input,
		output,
		total: Math.max(sum, count("total")),
		...(window !== undefined ? { window } : {}),
		...(windowPeak !== undefined ? { windowPeak } : {}),
	};
}

function activeState(value: unknown): boolean {
	return value === "running" || value === "queued" || value === "pending";
}

interface FleetKeyState {
	sessionId: string | null;
	next: number;
	keys: Map<string, string>;
}

interface FleetCandidate {
	internalKey: string;
	agent: unknown;
	role?: unknown;
	model?: unknown;
	effort?: unknown;
	startedAt: unknown;
	tokens?: unknown;
	goal?: unknown;
}

type StatusRpcParams = Pick<SubagentParamsLike, "id" | "runId" | "dir" | "index" | "view" | "lines">;

function buildFleetStatus(
	state: SubagentState | undefined,
	keyState: FleetKeyState,
	sessionId: string | null | undefined,
): SubagentRpcFleetStatus {
	const authoritativeSessionId = sessionId ?? null;
	if (keyState.sessionId !== authoritativeSessionId) {
		keyState.sessionId = authoritativeSessionId;
		keyState.next = 0;
		keyState.keys.clear();
	}
	if (!state || !authoritativeSessionId || state.currentSessionId !== authoritativeSessionId) {
		keyState.keys.clear();
		return { version: 1, entries: [], totalActive: 0, topLevelAsyncCapacity: { used: 0, limit: 0 }, omitted: 0 };
	}

	let totalActive = 0;
	const candidates: FleetCandidate[] = [];
	const addCandidate = (candidate: FleetCandidate) => {
		totalActive += 1;
		if (candidates.length < MAX_FLEET_CANDIDATES) candidates.push(candidate);
	};
	for (const control of state.foregroundControls.values()) {
		if (control.sessionId !== authoritativeSessionId) continue;
		if (control.activeChildren?.size) {
			for (const child of control.activeChildren.values()) addCandidate({
				internalKey: `foreground:${control.runId}:${child.index}`,
				agent: child.agent,
				model: child.model,
				effort: child.thinking,
				startedAt: child.startedAt,
				tokens: { input: child.inputTokens ?? 0, output: child.outputTokens ?? 0, total: child.tokens ?? 0, ...(child.window !== undefined ? { window: child.window } : {}), ...(child.windowPeak !== undefined ? { windowPeak: child.windowPeak } : {}) },
			});
		} else {
			addCandidate({
				internalKey: `foreground:${control.runId}:${control.currentIndex ?? 0}`,
				agent: control.currentAgent ?? control.mode,
				model: control.model,
				effort: control.thinking,
				startedAt: control.startedAt,
				tokens: { input: control.inputTokens ?? 0, output: control.outputTokens ?? 0, total: control.tokens ?? 0, ...(control.window !== undefined ? { window: control.window } : {}), ...(control.windowPeak !== undefined ? { windowPeak: control.windowPeak } : {}) },
			});
		}
	}
	for (const job of state.asyncJobs.values()) {
		if (job.sessionId !== authoritativeSessionId || !activeState(job.status)) continue;
		const startedAt = job.startedAt ?? job.updatedAt;
		if (job.mode === "workflow") {
			addCandidate({
				internalKey: `async:${job.asyncId}`,
				agent: "workflow",
				startedAt,
				tokens: job.totalTokens,
			});
			continue;
		}
		const steps: AsyncJobStep[] | undefined = job.steps?.length
			? job.steps
			: job.agents?.map((agent, index) => ({
				agent,
				index,
				status: job.status === "queued" ? "pending" : "running",
			}));
		if (!steps?.length) {
			addCandidate({
				internalKey: `async:${job.asyncId}`,
				agent: job.mode ?? "subagent",
				startedAt,
				tokens: job.totalTokens,
			});
			continue;
		}
		for (const [offset, step] of steps.entries()) {
			if (!activeState(step.status)) continue;
			const index = step.index ?? offset;
			if (step.status === "pending" && job.mode === "chain" && !job.activeParallelGroup && index !== (job.currentStep ?? 0)) continue;
			addCandidate({
				internalKey: `async:${job.asyncId}:${index}`,
				agent: step.agent,
				role: step.label,
				model: step.model,
				effort: step.thinking,
				startedAt: step.startedAt ?? startedAt,
				tokens: step.tokens ?? (steps.length === 1 ? job.totalTokens : undefined),
			});
		}
	}

	candidates.sort((left, right) => {
		const leftStarted = typeof left.startedAt === "number" ? left.startedAt : Number.MAX_SAFE_INTEGER;
		const rightStarted = typeof right.startedAt === "number" ? right.startedAt : Number.MAX_SAFE_INTEGER;
		return leftStarted - rightStarted || left.internalKey.localeCompare(right.internalKey);
	});
	const activeKeys = new Set(candidates.map((candidate) => candidate.internalKey));
	const entries: SubagentRpcFleetEntry[] = [];
	for (const candidate of candidates) {
		if (entries.length >= MAX_FLEET_ENTRIES) break;
		const agent = displayText(candidate.agent, MAX_AGENT_LENGTH);
		const startedAt = candidate.startedAt;
		if (!agent || typeof startedAt !== "number" || !Number.isSafeInteger(startedAt) || startedAt < 0) continue;
		let key = keyState.keys.get(candidate.internalKey);
		if (!key) {
			key = `fleet-${++keyState.next}`;
			keyState.keys.set(candidate.internalKey, key);
		}
		const role = displayText(candidate.role, MAX_AGENT_LENGTH);
		const model = displayText(candidate.model, MAX_METADATA_LENGTH);
		const effort = displayText(candidate.effort, MAX_METADATA_LENGTH);
		const goal = displayText(candidate.goal, MAX_GOAL_LENGTH);
		entries.push({
			key,
			agent,
			...(role ? { role } : {}),
			...(model ? { model } : {}),
			...(effort ? { effort } : {}),
			startedAt,
			tokens: publicTokens(candidate.tokens),
			...(goal ? { goal } : {}),
		});
	}
	for (const internalKey of keyState.keys.keys()) {
		if (!activeKeys.has(internalKey)) keyState.keys.delete(internalKey);
	}
	const omitted = Math.max(0, totalActive - entries.length);
	return { version: 1, entries, totalActive, topLevelAsyncCapacity: state.activeAsyncCapacity ?? { used: 0, limit: 0 }, omitted };
}

interface RegisterSubagentRpcBridgeOptions {
	events: EventBus;
	getContext: () => ExtensionContext | null;
	execute: (
		id: string,
		params: SubagentParamsLike,
		signal: AbortSignal,
		onUpdate: ((result: AgentToolResult<Details>) => void) | undefined,
		ctx: ExtensionContext,
	) => Promise<AgentToolResult<Details>>;
	asyncDirRoot?: string;
	operationDirRoot?: string;
	resultsDir?: string;
	kill?: (pid: number, signal?: NodeJS.Signals | 0) => boolean;
	now?: () => number;
	/** Native live state, projected into the optional public fleet-status capability. */
	state?: SubagentState;
}

class SubagentRpcError extends Error {
	readonly code: SubagentRpcErrorCode;

	constructor(code: SubagentRpcErrorCode, message: string) {
		super(message);
		this.name = "SubagentRpcError";
		this.code = code;
	}
}

const subagentParamsValidator = Compile(SubagentParams);

export function subagentRpcReplyEvent(requestId: string): string {
	return `${SUBAGENT_RPC_REPLY_EVENT_PREFIX}${requestId}`;
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function assertRequestId(value: unknown): string {
	if (typeof value !== "string" || value.trim().length === 0 || /[\r\n]/.test(value)) {
		throw new SubagentRpcError("invalid_request", "RPC requestId must be a non-empty string without newlines.");
	}
	return value;
}

function assertRecordParams(params: unknown, method: SubagentRpcMethod): Record<string, unknown> {
	if (params === undefined) return {};
	if (!isRecord(params)) throw new SubagentRpcError("invalid_params", `RPC ${method} params must be an object.`);
	return params;
}

function assertSubagentParams(params: SubagentParamsLike, label: string): void {
	if (subagentParamsValidator.Check(params)) return;
	const messages = [...subagentParamsValidator.Errors(params)]
		.slice(0, 4)
		.map((error) => error.message);
	throw new SubagentRpcError("invalid_params", `${label}: ${messages.join("; ") || "invalid subagent parameters"}`);
}

function textFromToolResult(result: AgentToolResult<Details>): string {
	return result.content
		.filter((part): part is { type: "text"; text: string } => part.type === "text")
		.map((part) => part.text)
		.join("\n");
}

type ToolResultWithError = AgentToolResult<Details> & { isError?: boolean };

function dataFromToolResult(result: ToolResultWithError): { text: string; details?: Details; isError?: boolean } {
	return {
		text: textFromToolResult(result),
		...(result.details ? { details: result.details } : {}),
		...(result.isError ? { isError: true } : {}),
	};
}

function failIfToolError(result: ToolResultWithError): void {
	if (!result.isError) return;
	throw new SubagentRpcError("execution_failed", textFromToolResult(result) || "Subagent RPC execution failed.");
}

function normalizeTargetParamsFromRecord(input: Record<string, unknown>): Pick<SubagentParamsLike, "id" | "runId" | "dir" | "index"> {
	const output: Pick<SubagentParamsLike, "id" | "runId" | "dir" | "index"> = {};
	if (input.id !== undefined) output.id = input.id as string;
	if (input.runId !== undefined) output.runId = input.runId as string;
	if (input.dir !== undefined) output.dir = input.dir as string;
	if (input.index !== undefined) output.index = input.index as number;
	return output;
}

function normalizeTargetParams(params: unknown, method: SubagentRpcMethod): Pick<SubagentParamsLike, "id" | "runId" | "dir" | "index"> {
	return normalizeTargetParamsFromRecord(assertRecordParams(params, method));
}

function normalizeStatusParams(params: unknown): StatusRpcParams {
	const input = assertRecordParams(params, "status");
	const output: StatusRpcParams = normalizeTargetParamsFromRecord(input);
	if (input.view !== undefined) output.view = input.view as StatusRpcParams["view"];
	if (input.lines !== undefined) output.lines = input.lines as number;
	return output;
}

function hasStatusTarget(params: StatusRpcParams): boolean {
	return params.id !== undefined
		|| params.runId !== undefined
		|| params.dir !== undefined
		|| params.index !== undefined
		|| params.view !== undefined
		|| params.lines !== undefined;
}

function canUseInMemoryStatus(state: SubagentState | undefined, sessionId: string | undefined): state is SubagentState {
	return Boolean(
		state
			&& sessionId
			&& state.currentSessionId === sessionId
			&& state.statusProjectionSessionId === sessionId
			&& state.foregroundControls instanceof Map
			&& state.asyncJobs instanceof Map,
	);
}

function inMemoryStatusSummary(fleet: SubagentRpcFleetStatus): string {
	const noun = fleet.totalActive === 1 ? "child" : "children";
	return `In-memory subagent status: ${fleet.totalActive} active ${noun}.`;
}

function sessionData(ctx: ExtensionContext | null): { cwd?: string; sessionId?: string; sessionFile?: string | null } {
	if (!ctx) return {};
	return {
		cwd: ctx.cwd,
		sessionId: ctx.sessionManager.getSessionId() ?? undefined,
		sessionFile: ctx.sessionManager.getSessionFile() ?? null,
	};
}

function pingData(ctx: ExtensionContext | null, ownership?: KernelOwnedProcessCapability) {
	return {
		version: SUBAGENT_RPC_PROTOCOL_VERSION,
		methods: [...SUBAGENT_RPC_METHODS],
		capabilities: {
			executionLifetime: { version: 1, modes: ["unbounded", "bounded"] },
			durableOperations: { version: 1, lookup: true, replay: true, cancelFence: true, scope: "runtime" },
			status: true,
			statusProjection: { version: 1, untargeted: "in-memory-when-ready", targeted: "executor" },
			managementActions: [...SUBAGENT_RPC_MANAGEMENT_ACTIONS],
			fleetStatus: { version: 1 },
			asyncStatusSnapshot: { kind: ASYNC_STATUS_SNAPSHOT_KIND, version: ASYNC_STATUS_SNAPSHOT_VERSION },
			asyncSpawn: true,
			steer: true,
			nonRecoveringSteer: true,
			interrupt: true,
			stop: true,
			resume: true,
			launchResolvedExtensions: { version: 1, source: "launch-resolved" },
			runtimeAcknowledgedExtensions: { version: 1, source: "child-runtime", event: "subagent:acknowledge-extension" },
			processTerminalProof: { version: 1, lifecycleArtifactVersion: SUBAGENT_LIFECYCLE_ARTIFACT_VERSION },
			workflowTerminalProof: { version: 1 },
			processTreeOwnership: ownership?.supported
				? { ...FULL_PROCESS_TREE_OWNERSHIP, routes: ["single-async", "parallel-data"], requestMode: "kernel" }
				: { version: 1, scope: process.platform === "win32" ? "unsupported" : "posix-process-group", escapedDescendants: "unverified", reason: ownership?.reason ?? "Kernel preflight has not completed." },
			diagnosticGuidance: { version: 1, idempotent: true, mode: "follow_up", confirmedToolFailure: true },
		},
		events: {
			ready: SUBAGENT_RPC_READY_EVENT,
			request: SUBAGENT_RPC_REQUEST_EVENT,
			replyPrefix: SUBAGENT_RPC_REPLY_EVENT_PREFIX,
			asyncComplete: SUBAGENT_ASYNC_COMPLETE_EVENT,
			childStatus: SUBAGENT_CHILD_STATUS_EVENT,
			processTerminal: SUBAGENT_PROCESS_TERMINAL_EVENT,
		},
		session: sessionData(ctx),
	};
}

async function executeChecked(
	options: RegisterSubagentRpcBridgeOptions,
	ctx: ExtensionContext,
	requestId: string,
	method: SubagentRpcMethod,
	params: SubagentParamsLike,
): Promise<{ text: string; details?: Details; isError?: boolean }> {
	assertSubagentParams(params, `RPC ${method} params`);
	const controller = new AbortController();
	const result = await options.execute(`rpc-${method}-${requestId}`, params, controller.signal, undefined, ctx);
	failIfToolError(result);
	return dataFromToolResult(result);
}

function manageParams(params: unknown): SubagentParamsLike {
	const input = assertRecordParams(params, "manage");
	if (typeof input.action !== "string" || !(SUBAGENT_RPC_MANAGEMENT_ACTIONS as readonly string[]).includes(input.action)) {
		throw new SubagentRpcError(
			"invalid_params",
			`RPC manage action must be one of: ${SUBAGENT_RPC_MANAGEMENT_ACTIONS.join(", ")}.`,
		);
	}
	if (input.id !== undefined && (typeof input.id !== "string" || !input.id.trim())) {
		throw new SubagentRpcError("invalid_params", "RPC manage id must be a non-empty string.");
	}
	const action = input.action as SubagentRpcManagementAction;
	const requiresId = action !== "schedule.list";
	if (requiresId && typeof input.id !== "string") {
		throw new SubagentRpcError("invalid_params", `RPC manage ${action} requires id.`);
	}
	if (action === "schedule.run" && input.quiet !== undefined && typeof input.quiet !== "boolean") {
		throw new SubagentRpcError("invalid_params", "RPC manage quiet must be a boolean.");
	}
	const output: SubagentParamsLike = {
		action,
		...(typeof input.id === "string" ? { id: input.id.trim() } : {}),
		...(action === "schedule.run" && input.quiet === true ? { quiet: true } : {}),
	};
	assertSubagentParams(output, "RPC manage params");
	return output;
}

function spawnParams(params: unknown): SubagentParamsLike {
	const input = assertRecordParams(params, "spawn");
	if (input.rpcOperationRunId !== undefined) throw new SubagentRpcError("invalid_params", "rpcOperationRunId is an internal field.");
	if (input.rpcKernelOperationDirectory !== undefined) throw new SubagentRpcError("invalid_params", "rpcKernelOperationDirectory is an internal field.");
	if (isRecord(input.executionOwnership) && input.executionOwnership.mode === "kernel" && (input.workflowScript !== undefined || input.workflowScriptPath !== undefined || input.workflow !== undefined || input.machine !== undefined || input.foregroundOnly === true || input.clarify !== undefined)) throw new SubagentRpcError("invalid_params", "Kernel ownership supports only local async single-agent or structured parallel-data launches.");
	if (input.ownedWorkflow !== undefined) {
		if (!isRecord(input.executionOwnership) || input.executionOwnership.mode !== "kernel") throw new SubagentRpcError("invalid_params", "ownedWorkflow requires executionOwnership.mode kernel.");
		if (input.workflowScript !== undefined || input.workflow !== undefined || input.workflowScriptPath !== undefined || input.action !== undefined || input.agent !== undefined || input.task !== undefined || input.async === false || input.tasks !== undefined || input.chain !== undefined) throw new SubagentRpcError("invalid_params", "ownedWorkflow accepts only its structured tasks, not executable workflow or control fields.");
		const parsed = parseOwnedWorkflow(input.ownedWorkflow);
		if (!parsed.ok) throw new SubagentRpcError("invalid_params", parsed.error);
		// SAFETY: root fields are checked before dispatch; preserve the validated public data route through executePublic.
		const request = { ...input, ownedWorkflowKeys: parsed.keys, async: true } as SubagentParamsLike;
		const error = validateOwnedWorkflowPublicFields(request);
		if (error) throw new SubagentRpcError("invalid_params", error);
		return request;
	}
	const normalized = normalizePublicSubagentExecution(input);
	if (!normalized.ok) throw new SubagentRpcError("invalid_params", normalized.error);
	if (normalized.params.action !== undefined) {
		throw new SubagentRpcError("invalid_params", "RPC spawn does not accept management/control actions. Use status or interrupt RPC methods instead.");
	}
	if (input.async === false) {
		throw new SubagentRpcError("invalid_params", "RPC spawn only supports detached async launches; omit async or set async: true.");
	}
	return { ...(normalized.params as SubagentParamsLike), async: true };
}

function steerParams(params: unknown): SubagentParamsLike {
	const input = assertRecordParams(params, "steer");
	if (typeof input.message !== "string" || !input.message.trim())
		throw new SubagentRpcError("invalid_params", "RPC steer requires a non-empty message.");
	const target = normalizeTargetParams(input, "steer");
	if (!target.id && !target.runId && !target.dir) throw new SubagentRpcError("invalid_params", "RPC steer requires id, runId, or dir.");
	if (input.mode !== undefined && input.mode !== "steer" && input.mode !== "follow_up" && input.mode !== "auto") throw new SubagentRpcError("invalid_params", "RPC steer mode must be steer, follow_up, or auto.");
	return {
		action: "steer",
		...target,
		message: input.message.trim(),
		...(typeof input.mode === "string" ? { mode: input.mode as "steer" | "follow_up" | "auto" } : {}),
		steeringRecovery: false,
	};
}

function resumeParams(params: unknown): SubagentParamsLike {
	const input = assertRecordParams(params, "resume");
	if (typeof input.message !== "string" || !input.message.trim())
		throw new SubagentRpcError("invalid_params", "RPC resume requires a non-empty message.");
	const target = normalizeTargetParams(input, "resume");
	if (!target.id && !target.runId && !target.dir) throw new SubagentRpcError("invalid_params", "RPC resume requires id, runId, or dir.");
	if (input.output !== undefined && (typeof input.output !== "string" || !input.output.trim()))
		throw new SubagentRpcError("invalid_params", "RPC resume output must be a non-empty path.");
	if (input.outputMode !== undefined && input.outputMode !== "file-only")
		throw new SubagentRpcError("invalid_params", "RPC resume supports only file-only output mode.");
	return {
		action: "resume",
		...target,
		message: input.message.trim(),
		...(typeof input.output === "string" ? { output: input.output.trim(), outputMode: "file-only" } : {}),
	};
}

function stopAsyncRun(
	params: unknown,
	options: RegisterSubagentRpcBridgeOptions,
	ctx: ExtensionContext,
	operationSessionId?: string,
): { runId: string; asyncDir: string; previousState: string; state: "stopping"; message: string; childId?: string } {
	const input = assertRecordParams(params, "stop");
	const rawChildId = input.childId;
	if (rawChildId !== undefined && (typeof rawChildId !== "string" || !rawChildId.trim() || /[\r\n]/.test(rawChildId) || rawChildId.length > 256)) {
		throw new SubagentRpcError("invalid_params", "RPC stop childId must be a non-empty string without newlines and at most 256 characters.");
	}
	const childId = typeof rawChildId === "string" ? rawChildId : undefined;
	const target = normalizeTargetParams(input, "stop");
	assertSubagentParams({ action: "status", ...target }, "RPC stop target params");
	const asyncDirRoot = options.asyncDirRoot ?? DIRS.async;
	const resultsDir = options.resultsDir ?? DIRS.results;
	let location;
	try {
		location = resolveAsyncRunLocation(target, asyncDirRoot, resultsDir);
	} catch (error) {
		throw new SubagentRpcError("invalid_params", error instanceof Error ? error.message : String(error));
	}
	if (!location.asyncDir) {
		throw new SubagentRpcError("not_found", "Async run not found or already completed; stop requires a live async run directory.");
	}

	const currentSessionId = operationSessionId ?? resolveCurrentSessionId(ctx.sessionManager);
	const initialStatus = readStatus(location.asyncDir);
	const initialRunId = initialStatus?.runId ?? location.resolvedId ?? path.basename(location.asyncDir);
	if (!initialStatus) throw new SubagentRpcError("not_found", `Status file not found for async run '${initialRunId}'.`);
	if (!currentSessionId || initialStatus.sessionId !== currentSessionId) {
		throw new SubagentRpcError("not_found", `Async run '${initialRunId}' was not found in the active session.`);
	}

	let child: ResolvedAsyncStatusChild | undefined;
	const emitChildStopping = (runId: string, asyncDir: string, stoppedChild: ResolvedAsyncStatusChild, ts = options.now?.() ?? Date.now()): void => {
		options.events.emit(SUBAGENT_CHILD_STATUS_EVENT, {
			type: "subagent.child-status",
			version: 1,
			runId,
			childId: stoppedChild.id,
			status: "stopping",
			ts,
			reason: "rpc",
			source: "rpc",
			asyncDir,
			stepIndex: stoppedChild.index,
			agent: stoppedChild.step.agent,
			...(stoppedChild.step.runId ? { childRunId: stoppedChild.step.runId } : {}),
			...(stoppedChild.step.workflowKey ? { workflowKey: stoppedChild.step.workflowKey } : {}),
			...(stoppedChild.step.phase ? { phase: stoppedChild.step.phase } : {}),
			...(stoppedChild.step.label ? { label: stoppedChild.step.label } : {}),
		} satisfies SubagentChildStatusEvent);
	};
	if (childId !== undefined) {
		const resolution = resolveAsyncStatusChild(initialStatus, childId);
		if (!resolution.ok) throw new SubagentRpcError(resolution.code === "not_found" ? "not_found" : "invalid_params", resolution.message);
		child = resolution.child;
		if (!isStoppableAsyncStatusStep(child.step)) {
			throw new SubagentRpcError("invalid_state", `Child '${childId}' in async run '${initialRunId}' is ${child.step.status}; stop only supports pending or running children.`);
		}
	}
	if (initialStatus.mode === "workflow" && initialStatus.state === "running") {
		const stopChild = options.state?.workflowChildStops?.get(initialRunId);
		if (child) {
			if (stopChild) {
				if (!stopChild(child.id, `Workflow child '${child.id}' stopped by RPC.`)) throw new SubagentRpcError("invalid_state", `Child '${childId}' in workflow ${initialRunId} is not available to stop.`);
				emitChildStopping(initialRunId, location.asyncDir, child);
				return {
					runId: initialRunId,
					asyncDir: location.asyncDir,
					previousState: initialStatus.state,
					state: "stopping",
					childId: child.id,
					message: `Stop requested for child ${child.id} in async run ${initialRunId}.`,
				};
			}
		}
		const workflowController = options.state?.workflowControllers?.get(initialRunId);
		if (workflowController && !child) {
			stopStoppableAsyncStatusChildren(initialStatus, stopChild, "Workflow stopped by RPC.");
			workflowController.abort(new Error("Workflow stopped by RPC."));
			return {
				runId: initialRunId,
				asyncDir: location.asyncDir,
				previousState: initialStatus.state,
				state: "stopping",
				message: `Stop requested for async run ${initialRunId}.`,
			};
		}
		// Workflow controls live in-process; a persisted run directory cannot restore them.
		throw new SubagentRpcError("invalid_state", child
			? `Child '${child.id}' in workflow ${initialRunId} has no live stop callback available.`
			: `Workflow ${initialRunId} has no live run controller available to stop.`);
	}

	let status;
	try {
		status = reconcileAsyncRun(location.asyncDir, { resultsDir, kill: options.kill, now: options.now }).status;
	} catch (error) {
		throw new SubagentRpcError("execution_failed", error instanceof Error ? error.message : String(error));
	}
	const runId = status?.runId ?? initialRunId;
	if (!status) throw new SubagentRpcError("not_found", `Status file not found for async run '${runId}'.`);
	if (status.sessionId !== currentSessionId) {
		throw new SubagentRpcError("not_found", `Async run '${runId}' was not found in the active session.`);
	}
	if (status.state !== "running") {
		throw new SubagentRpcError("invalid_state", `Async run ${runId} is ${status.state}; stop only supports running async runs.`);
	}
	if (childId !== undefined) {
		const resolution = resolveAsyncStatusChild(status, childId);
		if (!resolution.ok) throw new SubagentRpcError(resolution.code === "not_found" ? "not_found" : "invalid_params", resolution.message);
		child = resolution.child;
		if (!isStoppableAsyncStatusStep(child.step)) {
			throw new SubagentRpcError("invalid_state", `Child '${childId}' in async run '${runId}' is ${child.step.status}; stop only supports pending or running children.`);
		}
	}

	try {
		deliverStopRequest({
			asyncDir: location.asyncDir,
			pid: status.pid,
			kill: options.kill,
			now: options.now,
			source: "rpc-stop",
			...(child ? { targetIndex: child.index, childId: child.id } : {}),
		});
	} catch (error) {
		throw new SubagentRpcError("execution_failed", error instanceof Error ? error.message : String(error));
	}
	if (child) emitChildStopping(runId, location.asyncDir, child);

	return {
		runId,
		asyncDir: location.asyncDir,
		previousState: status.state,
		state: "stopping",
		...(child ? { childId: child.id } : {}),
		message: child ? `Stop requested for child ${child.id} in async run ${runId}.` : `Stop requested for async run ${runId}.`,
	};
}

const operationIdentityValidator = Compile(Type.Object({ operationId: Type.String({ minLength: 1, maxLength: 512, pattern: "^[^\\r\\n]+$" }), digest: Type.Optional(Type.String({ minLength: 1, maxLength: 512 })), cwd: Type.Optional(Type.String()) }));
const operationResponseValidator = Compile(Type.Object({ text: Type.Optional(Type.String()), details: Type.Optional(Type.Object({ asyncDir: Type.Optional(Type.String()) })), isError: Type.Optional(Type.Boolean()) }));
const diagnosticValidator = Compile(Type.Object({ diagnosticId: Type.String({ minLength: 1, maxLength: 256 }), toolCallId: Type.String({ minLength: 1, maxLength: 512 }), message: Type.String({ minLength: 1, maxLength: 4096 }) }));
const diagnosticReceiptValidator = Compile(Type.Object({ state: Type.Union([Type.Literal("queued"), Type.Literal("cancelled"), Type.Literal("rejected")]), reason: Type.Optional(Type.String()) }));

function operationStorageRoot(options: RegisterSubagentRpcBridgeOptions): string {
	return options.operationDirRoot ?? options.asyncDirRoot ?? path.join(getAgentDir(), "subagent-runtime");
}

function operationInput(params: SubagentRpcRequestEnvelope["params"], method: SubagentRpcMethod) {
	const input = assertRecordParams(params, method);
	if (!operationIdentityValidator.Check(input) || !input.operationId.trim() || (method !== "lookup" && !input.digest?.trim()) || input.digest?.trim() === "") throw new SubagentRpcError("invalid_params", "operationId and digest must be non-empty strings of at most 512 characters without identity newlines.");
	return { operationId: input.operationId, digest: input.digest, input };
}

async function observeOperation(operation: DurableOperation, options: RegisterSubagentRpcBridgeOptions) {
	const intent = operation.intent();
	if (!intent) return { operationId: operation.operationId, state: "absent", safeToReplay: true };
	const response = operation.result();
	const responseRecord = operationResponseValidator.Check(response) ? response : {};
	const kernel = intent.effectiveExecutionOwnership?.mode === "kernel" ? await observeNativeKernelRun(path.join(operation.directory, "owned"), intent.runId) : undefined;
	const asyncDir = kernel?.mapping?.asyncDir ?? responseRecord.details?.asyncDir ?? path.join(options.asyncDirRoot ?? DIRS.async, intent.runId);
	const status = readStatus(asyncDir);
	if (responseRecord.isError && kernel?.bindingVerified && (kernel.observation.status === "pending" || kernel.observation.status === "active" || (kernel.observation.status === "retired" && status?.state === "complete"))) {
		responseRecord.isError = false;
		responseRecord.text = "The original owned execution is being reconciled.";
	}
	const resultPath = resolveAsyncRunLocation({ runId: intent.runId, dir: asyncDir }, options.asyncDirRoot ?? DIRS.async, options.resultsDir ?? DIRS.results).resultPath;
	let processTerminalProof = kernel?.processTerminalProof ?? readProcessTerminal(asyncDir, { runId: intent.runId });
	if (kernel?.processTerminalProof.state === "observed" && (kernel.mapping?.nativeOperation?.operationId !== intent.operationId || kernel.mapping.nativeOperation.digest !== intent.digest)) processTerminalProof = { version: 1, state: "unknown", runId: intent.runId, runnerProcessInstanceId: "unknown", reason: "Kernel mapping does not match the durable native operation." };
	const workflowTerminalProof = status?.mode === "workflow" ? readWorkflowTerminalProof(asyncDir, intent.runId) : undefined;
	const cancellationRequested = operation.cancelled();
	const stopped = cancellationRequested || status?.stopped === true;
	return {
		...responseRecord,
		operationId: intent.operationId,
		digest: intent.digest,
		runId: intent.runId,
		asyncDir,
		resultPath,
		state: cancellationRequested ? "cancelled" : response || status ? "found" : "pending",
		safeToReplay: true,
		cancellationRequested,
		neverStarted: intent.kind === "cancel" || kernel?.neverStarted === true,
		effectiveExecutionLifetime: intent.effectiveExecutionLifetime,
		effectiveExecutionOwnership: intent.effectiveExecutionOwnership,
		executionRoute: intent.executionRoute,
		ownedWorkflowKeys: intent.ownedWorkflowKeys,
		terminationReason: kernel?.observation.timedOut || (intent.effectiveExecutionLifetime?.mode === "bounded" && status?.timedOut) ? "execution_lifetime_expired" : undefined,
		processTerminalProof,
		workflowTerminalProof,
		status: kernel?.runnerFailed ? stopped ? "stopped" : "failed" : status?.state,
		statusPayload: status ? { ...status, state: kernel?.runnerFailed ? stopped ? "stopped" : "failed" : status.state, error: kernel?.runnerFailed ? status.error ?? "Owned runner exited without a successful terminal result." : status.error, processTerminalProof, workflowTerminalProof, effectiveExecutionLifetime: intent.effectiveExecutionLifetime, effectiveExecutionOwnership: intent.effectiveExecutionOwnership, executionRoute: intent.executionRoute } : undefined,
		activity: status ? {
				state: status.activityState ?? "unknown",
				phase: processTerminalProof?.state === "observed" || workflowTerminalProof?.state === "observed" ? "exited" : status.runnerPhase ?? "unknown",
				lastActivityAt: status.lastActivityAt,
				lastModelActivityAt: status.lastModelActivityAt,
				lastToolActivityAt: status.lastToolActivityAt,
				lastToolFailure: status.lastToolFailure,
				currentTool: status.currentTool,
				currentToolStartedAt: status.currentToolStartedAt,
				runnerPid: status.pid,
				steps: status.steps?.map((step) => ({ agent: step.agent, activityState: step.activityState, phase: step.processTerminal?.state === "observed" ? "exited" : step.runnerPhase ?? "unknown", currentTool: step.currentTool, currentToolStartedAt: step.currentToolStartedAt, lastActivityAt: step.lastActivityAt, lastModelActivityAt: step.lastModelActivityAt, lastToolActivityAt: step.lastToolActivityAt, processTerminal: step.processTerminal })),
			} : undefined,
	};
}

function stopOwnedRunTree(runId: string, asyncDir: string, sessionId: string | undefined, options: RegisterSubagentRpcBridgeOptions, ctx: ExtensionContext, visited: Set<string>): void {
	if (visited.has(runId)) return;
	visited.add(runId);
	try {
		stopAsyncRun({ runId, dir: asyncDir }, options, ctx, sessionId);
	} catch (error) {
		if (!(error instanceof SubagentRpcError) || !["not_found", "invalid_state"].includes(error.code)) throw error;
	}
	const status = readStatus(asyncDir);
	if (status?.runId !== runId || status.sessionId !== sessionId || status.mode !== "workflow") return;
	for (const child of status.steps ?? []) {
		if (!child.async || !child.runId || path.basename(child.runId) !== child.runId) continue;
		stopOwnedRunTree(child.runId, path.join(path.dirname(asyncDir), child.runId), sessionId, options, ctx, visited);
	}
}

async function stopOperation(operation: DurableOperation, options: RegisterSubagentRpcBridgeOptions, ctx: ExtensionContext): Promise<void> {
	const intent = operation.intent();
	if (!intent || intent.kind === "cancel") return;
	if (intent.effectiveExecutionOwnership?.mode === "kernel") {
		const ownedDirectory = path.join(operation.directory, "owned");
		const mapping = readNativeKernelMapping(ownedDirectory, intent.runId);
		if (mapping) deliverStopRequest({ asyncDir: mapping.asyncDir, source: "owned-operation-cancel" });
		await cancelKernelOwnedProcess(ownedDirectory, { deadlineMs: 1_000 });
		return;
	}
	const observation = await observeOperation(operation, options);
	if (!("asyncDir" in observation)) return;
	// A persisted workflow can outlive its in-process controller after restart.
	stopOwnedRunTree(intent.runId, observation.asyncDir, intent.sessionId, options, ctx, new Set());
}

async function handleOperation(
	request: SubagentRpcRequestEnvelope,
	options: RegisterSubagentRpcBridgeOptions,
	ctx: ExtensionContext,
	activeOperations: Map<string, AbortController>,
) {
	const { operationId, digest, input } = operationInput(request.params, request.method);
	const operation = new DurableOperation(operationStorageRoot(options), ctx.cwd, operationId);
	const intent = operation.intent();
	if (intent && digest !== undefined && intent.digest !== digest) throw new SubagentRpcError("invalid_params", "Operation digest does not match its durable intent.");
	if (request.method === "lookup") {
		if (operation.cancelled()) await stopOperation(operation, options, ctx);
		return observeOperation(operation, options);
	}
	if (request.method === "cancel") {
		operation.cancel(digest!);
		activeOperations.get(operation.directory)?.abort(new Error("Operation cancelled."));
		await stopOperation(operation, options, ctx);
		return observeOperation(operation, options);
	}
	if (request.method === "diagnose") {
		if (!intent) throw new SubagentRpcError("not_found", "No durable operation exists for diagnostic guidance.");
		if (!diagnosticValidator.Check(input) || !input.diagnosticId.trim() || !input.message.trim()) throw new SubagentRpcError("invalid_params", "Diagnostic guidance requires diagnosticId, toolCallId and a nonempty message.");
		const diagnostic = { diagnosticId: input.diagnosticId, toolCallId: input.toolCallId, message: input.message };
		const claimed = operation.claimDiagnostic(diagnostic);
		const identity = { operationId: intent.operationId, digest: intent.digest, runId: intent.runId, diagnosticId: diagnostic.diagnosticId, toolCallId: diagnostic.toolCallId, guidanceOnly: true };
		if (!claimed) {
			const receipt = operation.diagnosticReceipt(diagnostic.diagnosticId);
			return diagnosticReceiptValidator.Check(receipt) ? { ...identity, ...receipt } : { ...identity, state: "pending" };
		}
		const observation = await observeOperation(operation, options);
		if (operation.cancelled()) {
			operation.completeDiagnostic(diagnostic.diagnosticId, { state: "cancelled" });
			return { ...identity, state: "cancelled" };
		}
		const status = "statusPayload" in observation ? observation.statusPayload : undefined;
		const targets = status?.steps?.flatMap((step, index) => step.lastToolFailure?.toolCallId === diagnostic.toolCallId ? [index] : []) ?? [];
		const targetIndex = targets.length === 1 ? targets[0]! : -1;
		if (!("asyncDir" in observation) || status?.state !== "running" || targetIndex < 0 || status.steps?.[targetIndex]?.status !== "running") {
			const receipt = { state: "rejected" as const, reason: "The referenced failed tool is not in a live child session." };
			operation.completeDiagnostic(diagnostic.diagnosticId, receipt);
			return { ...identity, ...receipt };
		}
		if (operation.cancelled()) {
			operation.completeDiagnostic(diagnostic.diagnosticId, { state: "cancelled" });
			return { ...identity, state: "cancelled" };
		}
		const controlId = `diagnostic-${createHash("sha256").update(diagnostic.diagnosticId).digest("hex")}`;
		requestAsyncSteer(observation.asyncDir, { id: controlId, message: diagnostic.message, mode: "follow_up", targetIndex, source: "confirmed-tool-failure-diagnosis" });
		operation.completeDiagnostic(diagnostic.diagnosticId, { state: "queued" });
		return { ...identity, state: "queued" };
	}
	const { operationId: _operationId, digest: _digest, ...launchInput } = input;
	const params = spawnParams({ ...launchInput, cwd: path.resolve(operation.scopeCwd ?? ctx.cwd, launchInput.cwd ?? ".") });
	assertSubagentParams(params, "RPC spawn params");
	const requestHash = operationRequestHash(params);
	if (intent?.requestHash !== undefined && intent.requestHash !== requestHash) throw new SubagentRpcError("invalid_params", "Operation replay launch parameters do not match the original request.");
	const claim: Parameters<DurableOperation["claim"]>[0] = { digest: digest!, requestHash, sessionId: resolveCurrentSessionId(ctx.sessionManager) };
	if (params.executionLifetime !== undefined) claim.effectiveExecutionLifetime = params.executionLifetime;
	if (params.executionOwnership?.mode === "kernel") {
		if (!intent) {
			const capability = await probeRuntimeOwnership(path.join(operationStorageRoot(options), "kernel-cache"));
			if (!capability.supported) throw new SubagentRpcError("invalid_state", `Kernel process ownership is unavailable: ${capability.reason ?? "platform preflight failed"}`);
		}
		claim.effectiveExecutionOwnership = params.executionOwnership;
		claim.executionRoute = params.ownedWorkflow !== undefined ? "parallel-data" : "single-async";
		if (params.ownedWorkflowKeys) claim.ownedWorkflowKeys = params.ownedWorkflowKeys;
	}
	const claimed = operation.claim(claim);
	if (!claimed) {
		const winner = operation.intent();
		if (!winner || winner.digest !== digest || (winner.requestHash !== undefined && winner.requestHash !== requestHash)) throw new SubagentRpcError("invalid_params", "Concurrent operation launch does not match its durable intent.");
		return observeOperation(operation, options);
	}
	if (operation.cancelled()) return observeOperation(operation, options);
	const controller = new AbortController();
	activeOperations.set(operation.directory, controller);
	try {
		const result = await options.execute(`rpc-spawn-${request.requestId}`, { ...params, rpcOperationRunId: operation.runId, rpcKernelOperationDirectory: path.join(operation.directory, "owned") }, controller.signal, undefined, ctx);
		operation.complete(dataFromToolResult(result));
		if (operation.cancelled()) await stopOperation(operation, options, ctx);
		return observeOperation(operation, options);
	} finally {
		activeOperations.delete(operation.directory);
	}
}

async function handleRequest(
	request: SubagentRpcRequestEnvelope,
	options: RegisterSubagentRpcBridgeOptions,
	fleetKeys: FleetKeyState,
	activeOperations: Map<string, AbortController>,
): Promise<unknown> {
	const ctx = options.getContext();
	if (request.method === "ping") return pingData(ctx, ctx ? await probeRuntimeOwnership(path.join(operationStorageRoot(options), "kernel-cache")) : undefined);
	if (!ctx) throw new SubagentRpcError("no_active_session", "No active extension context for subagent RPC.");
	if (request.method === "lookup" || request.method === "cancel" || request.method === "diagnose" || (request.method === "spawn" && isRecord(request.params) && (request.params.operationId !== undefined || request.params.digest !== undefined || request.params.executionOwnership !== undefined))) return handleOperation(request, options, ctx, activeOperations);

	if (request.method === "manage") {
		return executeChecked(options, ctx, request.requestId, request.method, manageParams(request.params));
	}
	if (request.method === "spawn") {
		return executeChecked(options, ctx, request.requestId, request.method, spawnParams(request.params));
	}
	if (request.method === "status") {
		const statusParams = normalizeStatusParams(request.params);
		let sessionId: string | undefined;
		if (!hasStatusTarget(statusParams)) {
			try {
				sessionId = resolveCurrentSessionId(ctx.sessionManager);
			} catch {
				// Let the executor produce the canonical error when session identity is unavailable.
			}
			if (canUseInMemoryStatus(options.state, sessionId)) {
				const fleet = buildFleetStatus(options.state, fleetKeys, sessionId);
				const asyncSnapshot = buildAsyncStatusSnapshotForState(options.state, sessionId);
				return {
					text: inMemoryStatusSummary(fleet),
					details: { mode: "management", results: [] },
					fleet,
					asyncSnapshot,
				};
			}
		}
		const status = await executeChecked(
			options,
			ctx,
			request.requestId,
			request.method,
			{ action: "status", ...statusParams },
		);
		sessionId ??= resolveCurrentSessionId(ctx.sessionManager);
		return {
			...status,
			fleet: buildFleetStatus(
				options.state,
				fleetKeys,
				sessionId,
			),
			asyncSnapshot: buildAsyncStatusSnapshotForState(options.state, sessionId),
		};
	}
	if (request.method === "steer") {
		return executeChecked(options, ctx, request.requestId, request.method, steerParams(request.params));
	}
	if (request.method === "interrupt") {
		return executeChecked(options, ctx, request.requestId, request.method, { action: "interrupt", ...normalizeTargetParams(request.params, "interrupt") });
	}
	if (request.method === "stop") {
		return stopAsyncRun(request.params, options, ctx);
	}
	if (request.method === "resume") {
		return executeChecked(options, ctx, request.requestId, request.method, resumeParams(request.params));
	}
	throw new SubagentRpcError("unsupported_method", `Unsupported subagent RPC method: ${String(request.method)}`);
}

function parseRequest(raw: unknown): SubagentRpcRequestEnvelope {
	if (!isRecord(raw)) throw new SubagentRpcError("invalid_request", "Subagent RPC request must be an object.");
	const requestId = assertRequestId(raw.requestId);
	if (raw.version !== SUBAGENT_RPC_PROTOCOL_VERSION) {
		throw new SubagentRpcError("unsupported_version", `Unsupported subagent RPC version: ${String(raw.version)}.`);
	}
	if (typeof raw.method !== "string" || !(SUBAGENT_RPC_METHODS as readonly string[]).includes(raw.method)) {
		throw new SubagentRpcError("unsupported_method", `Unsupported subagent RPC method: ${String(raw.method)}.`);
	}
	return {
		version: SUBAGENT_RPC_PROTOCOL_VERSION,
		requestId,
		method: raw.method as SubagentRpcMethod,
		...(raw.params !== undefined ? { params: raw.params } : {}),
		...(isRecord(raw.source) ? { source: raw.source as SubagentRpcRequestEnvelope["source"] } : {}),
	};
}

function safeReplyRequestId(raw: unknown): string {
	if (!isRecord(raw)) return "unknown";
	const requestId = raw.requestId;
	return typeof requestId === "string" && requestId.trim().length > 0 && !/[\r\n]/.test(requestId)
		? requestId
		: "unknown";
}

function errorReply(raw: unknown, error: unknown): SubagentRpcReplyEnvelope {
	const requestId = safeReplyRequestId(raw);
	const method = isRecord(raw) && typeof raw.method === "string" && (SUBAGENT_RPC_METHODS as readonly string[]).includes(raw.method)
		? raw.method as SubagentRpcMethod
		: undefined;
	const rpcError = error instanceof SubagentRpcError
		? error
		: new SubagentRpcError("execution_failed", error instanceof Error ? error.message : String(error));
	return {
		version: SUBAGENT_RPC_PROTOCOL_VERSION,
		requestId,
		...(method ? { method } : {}),
		success: false,
		error: {
			code: rpcError.code,
			message: rpcError.message,
		},
	};
}

export function registerSubagentRpcBridge(options: RegisterSubagentRpcBridgeOptions): {
	emitReady: (ctx?: ExtensionContext | null) => void;
	dispose: () => void;
} {
	const fleetKeys: FleetKeyState = { sessionId: null, next: 0, keys: new Map() };
	const activeOperations = new Map<string, AbortController>();
	const unsubscribe = options.events.on(SUBAGENT_RPC_REQUEST_EVENT, async (raw) => {
		let request: SubagentRpcRequestEnvelope | undefined;
		try {
			request = parseRequest(raw);
			const data = await handleRequest(request, options, fleetKeys, activeOperations);
			options.events.emit(subagentRpcReplyEvent(request.requestId), {
				version: SUBAGENT_RPC_PROTOCOL_VERSION,
				requestId: request.requestId,
				method: request.method,
				success: true,
				data,
			} satisfies SubagentRpcReplyEnvelope);
		} catch (error) {
			const reply = errorReply(request ?? raw, error);
			options.events.emit(subagentRpcReplyEvent(reply.requestId), reply);
		}
	});

	return {
		emitReady: (ctx) => {
			options.events.emit(SUBAGENT_RPC_READY_EVENT, pingData(ctx ?? options.getContext()));
		},
		dispose: () => {
			if (typeof unsubscribe === "function") unsubscribe();
		},
	};
}
