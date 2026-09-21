import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { it } from "node:test";
import { Type, type Static } from "typebox";
import { Compile } from "typebox/compile";
import { createEventBus, makeMinimalCtx } from "../support/helpers.ts";
import type { ExecutionLifetime } from "../../src/shared/types.ts";
import { DurableOperation, type OperationIdentity } from "../../src/runs/background/durable-operation.ts";
import { stopRequestsDir } from "../../src/runs/background/control-channel.ts";
import { registerSubagentRpcBridge, SUBAGENT_RPC_REQUEST_EVENT, subagentRpcReplyEvent, type SubagentRpcMethod } from "../../src/extension/rpc.ts";

type Options = Parameters<typeof registerSubagentRpcBridge>[0];
type Events = ReturnType<typeof createEventBus>;

function context(cwd: string, session = "session-1") {
	const ctx = makeMinimalCtx(cwd);
	ctx.sessionManager.getSessionId = () => session;
	ctx.sessionManager.getSessionFile = () => session;
	return ctx;
}

interface OperationRequest extends OperationIdentity {
	agent?: string;
	task?: string;
	executionLifetime?: ExecutionLifetime;
}

const observationSchema = Type.Object({
	runId: Type.Optional(Type.String()), state: Type.Optional(Type.String()), status: Type.Optional(Type.String()),
	neverStarted: Type.Optional(Type.Boolean()), cancellationRequested: Type.Optional(Type.Boolean()),
	effectiveExecutionLifetime: Type.Optional(Type.Union([
		Type.Object({ mode: Type.Literal("unbounded") }),
		Type.Object({ mode: Type.Literal("bounded"), timeoutMs: Type.Number() }),
	])),
	processTerminalProof: Type.Optional(Type.Object({ state: Type.String() })),
});
type OperationObservation = Static<typeof observationSchema>;
const replyValidator = Compile(Type.Union([
	Type.Object({ version: Type.Literal(1), requestId: Type.String(), success: Type.Literal(true), data: observationSchema }),
	Type.Object({ version: Type.Literal(1), requestId: Type.String(), success: Type.Literal(false), error: Type.Object({ message: Type.String() }) }),
]));

let sequence = 0;
function request(events: Events, method: SubagentRpcMethod, params: OperationRequest): Promise<OperationObservation> {
	const requestId = `operation-${++sequence}`;
	return new Promise((resolve, reject) => {
		const dispose = events.on(subagentRpcReplyEvent(requestId), (value) => {
			dispose();
			if (!replyValidator.Check(value)) { reject(new Error("Invalid operation reply")); return; }
			if (value.success) resolve(value.data);
			else reject(new Error(value.error.message));
		});
		events.emit(SUBAGENT_RPC_REQUEST_EVENT, { version: 1, requestId, method, params });
	});
}

const launch = { operationId: "operation-1", digest: "digest-1", agent: "worker", task: "Work", executionLifetime: { mode: "unbounded" } } satisfies OperationRequest;

it("reconciles a lost spawn reply across session restart without a second dispatch", async () => {
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "durable-rpc-"));
	const events = createEventBus();
	let dispatches = 0;
	const options: Options = {
		events, asyncDirRoot: root, getContext: () => context(root),
		execute: async (_id, params) => {
			dispatches++;
			return { content: [{ type: "text", text: "started" }], details: { mode: "single", results: [], runId: params.rpcOperationRunId, asyncDir: path.join(root, params.rpcOperationRunId!) } };
		},
	};
	const original = registerSubagentRpcBridge(options);
	try {
		const started = await request(events, "spawn", launch);
		original.dispose();
		const restarted = registerSubagentRpcBridge({ ...options, getContext: () => context(root, "session-2") });
		try {
			const lookup = await request(events, "lookup", { operationId: launch.operationId, digest: launch.digest });
			const replay = await request(events, "spawn", launch);
			assert.equal(lookup.runId, started.runId);
			assert.equal(replay.runId, started.runId);
			assert.deepEqual(lookup.effectiveExecutionLifetime, { mode: "unbounded" });
			assert.equal(dispatches, 1);
			await assert.rejects(request(events, "spawn", { ...launch, task: "Different" }), /parameters/);
			await assert.rejects(request(events, "spawn", { ...launch, digest: "different" }), /digest/);
		} finally { restarted.dispose(); }
	} finally { original.dispose(); fs.rmSync(root, { recursive: true, force: true }); }
});

it("fences a delayed spawn before dispatch and retains the fence after restart", async () => {
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "durable-fence-"));
	const events = createEventBus();
	const options: Options = { events, asyncDirRoot: root, getContext: () => context(root), execute: async () => assert.fail("cancelled operation dispatched") };
	let bridge = registerSubagentRpcBridge(options);
	try {
		const cancelled = await request(events, "cancel", { operationId: launch.operationId, digest: launch.digest });
		assert.equal(cancelled.neverStarted, true);
		assert.equal(cancelled.cancellationRequested, true);
		bridge.dispose();
		bridge = registerSubagentRpcBridge(options);
		const delayed = await request(events, "spawn", launch);
		assert.equal(delayed.state, "cancelled");
		assert.equal(delayed.neverStarted, true);
		assert.equal(delayed.processTerminalProof, undefined);
	} finally { bridge.dispose(); fs.rmSync(root, { recursive: true, force: true }); }
});

it("cancel racing a started dispatch aborts its signal without inventing exit proof", async () => {
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "durable-race-"));
	const events = createEventBus();
	let finish: (() => void) | undefined;
	let observedSignal: AbortSignal | undefined;
	let dispatches = 0;
	const bridge = registerSubagentRpcBridge({
		events, asyncDirRoot: root, getContext: () => context(root),
		execute: async (_id, params, signal) => {
			dispatches++;
			observedSignal = signal;
			await new Promise<void>((resolve) => { finish = resolve; });
			return { content: [], details: { mode: "single", results: [], runId: params.rpcOperationRunId } };
		},
	});
	try {
		const pending = request(events, "spawn", launch);
		const replay = await request(events, "spawn", launch);
		assert.equal(replay.state, "pending");
		const cancellation = await request(events, "cancel", { operationId: launch.operationId, digest: launch.digest });
		assert.equal(observedSignal?.aborted, true);
		assert.equal(cancellation.neverStarted, false);
		assert.equal(cancellation.processTerminalProof, undefined);
		finish!();
		const late = await pending;
		assert.equal(late.state, "cancelled");
		assert.equal(late.processTerminalProof, undefined);
		assert.equal(dispatches, 1);
	} finally { bridge.dispose(); fs.rmSync(root, { recursive: true, force: true }); }
});

it("recovers pre-reply runner identity from durable intent and disk status", async () => {
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "durable-crash-"));
	const events = createEventBus();
	const operation = new DurableOperation(root, root, launch.operationId);
	operation.claim({ digest: launch.digest, requestHash: "pre-crash", effectiveExecutionLifetime: launch.executionLifetime });
	fs.mkdirSync(path.join(root, operation.runId), { recursive: true });
	fs.writeFileSync(path.join(root, operation.runId, "status.json"), JSON.stringify({ runId: operation.runId, state: "running", pid: 321, activityState: "working", currentTool: "bash", currentToolStartedAt: 1 }));
	const bridge = registerSubagentRpcBridge({ events, asyncDirRoot: root, getContext: () => context(root, "new-session"), execute: async () => assert.fail("lookup dispatched") });
	try {
		const observation = await request(events, "lookup", { operationId: launch.operationId, digest: launch.digest });
		assert.equal(observation.state, "found");
		assert.equal(observation.runId, operation.runId);
		assert.equal(observation.status, "running");
		assert.equal(observation.processTerminalProof, undefined);
	} finally { bridge.dispose(); fs.rmSync(root, { recursive: true, force: true }); }
});

it("stops persisted workflow children after restart loses the in-process controller", async () => {
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "durable-restart-stop-"));
	const events = createEventBus();
	const operation = new DurableOperation(root, root, launch.operationId);
	operation.claim({ digest: launch.digest, sessionId: "session-1" });
	const childDir = path.join(root, "child");
	for (const id of [operation.runId, "child"]) fs.mkdirSync(path.join(root, id), { recursive: true });
	fs.writeFileSync(path.join(root, operation.runId, "status.json"), JSON.stringify({ runId: operation.runId, sessionId: "session-1", mode: "workflow", state: "running", startedAt: Date.now(), steps: [{ agent: "worker", runId: "child", async: true, status: "running" }] }));
	fs.writeFileSync(path.join(childDir, "status.json"), JSON.stringify({ runId: "child", sessionId: "session-1", mode: "single", state: "running", startedAt: Date.now(), lastUpdate: Date.now(), pid: 12345, steps: [{ agent: "worker", status: "running" }] }));
	const bridge = registerSubagentRpcBridge({ events, asyncDirRoot: root, getContext: () => context(root, "session-2"), kill: () => true, execute: async () => assert.fail("cancel must not dispatch") });
	try {
		const cancelled = await request(events, "cancel", { operationId: launch.operationId, digest: launch.digest });
		assert.equal(cancelled.cancellationRequested, true);
		assert.ok(fs.readdirSync(stopRequestsDir(childDir)).length > 0);
		assert.equal(cancelled.processTerminalProof, undefined);
	} finally { bridge.dispose(); fs.rmSync(root, { recursive: true, force: true }); }
});
