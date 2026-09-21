import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { it } from "node:test";
import { Type, type Static } from "typebox";
import { Compile } from "typebox/compile";
import { createEventBus, makeMinimalCtx } from "../support/helpers.ts";
import type { ExecutionLifetime } from "../../src/shared/types.ts";
import { DurableOperation, operationRequestHash, type OperationIdentity } from "../../src/runs/background/durable-operation.ts";
import { stopRequestsDir, consumeSteerRequests } from "../../src/runs/background/control-channel.ts";
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
	cwd?: string;
	agent?: string;
	task?: string;
	executionLifetime?: ExecutionLifetime;
	diagnosticId?: string;
	toolCallId?: string;
	message?: string;
}

const observationSchema = Type.Object({
	runId: Type.Optional(Type.String()), state: Type.Optional(Type.String()), status: Type.Optional(Type.String()),
	neverStarted: Type.Optional(Type.Boolean()), cancellationRequested: Type.Optional(Type.Boolean()),
	diagnosticId: Type.Optional(Type.String()), guidanceOnly: Type.Optional(Type.Boolean()),
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

it("keeps the guarded anchor owner immutable and rejects stale owner-death evidence", t => {
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "guarded-anchor-"));
	t.after(() => fs.rmSync(root, { recursive: true, force: true }));
	const original = new DurableOperation(root, root, launch.operationId);
	const owner = { pid: 123, uniqueId: "1234", pidVersion: 1, hostId: "host", bootId: "boot" };
	const claim = { digest: launch.digest, requestHash: "request", dispatchArbitration: 1 as const, launchOwner: owner };
	assert.equal(original.claim(claim), true);
	fs.rmSync(path.join(original.directory, "intent.json"));
	const alternate = new DurableOperation(root, root, launch.operationId);
	const otherOwner = { ...owner, pid: 456, uniqueId: "5678" };
	assert.equal(alternate.claim({ ...claim, launchOwner: otherOwner }), false);
	assert.deepEqual(alternate.intent()?.launchOwner, owner);
	assert.equal(alternate.rejectGuardedDispatch(otherOwner), false);
	assert.equal(alternate.dispatchPending(), true);
	assert.equal(alternate.rejectGuardedDispatch(owner), true);
	assert.equal(alternate.rejectedBeforeDispatch(), true);
});

for (const kind of ["proven", "unclassified", "foreign-run"] as const) it(`preserves pre-dispatch rejection evidence without replaying the launch (${kind})`, async t => {
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "durable-rejection-"));
	t.after(() => fs.rmSync(root, { recursive: true, force: true }));
	const events = createEventBus();
	let executions = 0;
	const options: Options = { events, asyncDirRoot: root, getContext: () => context(root), execute: async (_id, params) => {
		executions++;
		const details: import("../../src/shared/types.ts").Details = { mode: "single", results: [] };
		if (kind !== "unclassified") details.admission = { version: 1, state: "rejected-before-dispatch", runId: kind === "foreign-run" ? "other-run" : params.rpcOperationRunId!, reason: "agent-resolution-rejected" };
		return { isError: true, content: [{ type: "text", text: "rejected" }], details };
	} };
	const first = registerSubagentRpcBridge(options);
	await Promise.all([request(events, "spawn", launch), request(events, "spawn", launch)]);
	first.dispose();
	const restarted = registerSubagentRpcBridge({ ...options, getContext: () => context(path.join(root, "new-cwd"), "session-2") });
	t.after(() => restarted.dispose());
	assert.equal((await request(events, "lookup", launch)).neverStarted, kind === "proven");
	assert.equal((await request(events, "cancel", launch)).neverStarted, kind === "proven");
	assert.equal((await request(events, "spawn", launch)).neverStarted, kind === "proven");
	assert.equal(executions, 1);
});

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
		const restarted = registerSubagentRpcBridge({ ...options, getContext: () => context(path.join(root, "another-worktree"), "session-2") });
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

it("arbitrates one operation identity across concurrent working-directory scopes", () => {
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "durable-global-scope-"));
	try {
		const first = new DurableOperation(root, path.join(root, "first-worktree"), "same-operation");
		const second = new DurableOperation(root, path.join(root, "second-worktree"), "same-operation");
		assert.equal(first.claim({ digest: "same-digest", requestHash: "same-params" }), true);
		assert.equal(second.claim({ digest: "same-digest", requestHash: "same-params" }), false);
		assert.equal(second.runId, first.runId);
		assert.equal(second.directory, first.directory);
		assert.throws(() => second.claim({ digest: "same-digest", requestHash: "changed-params" }), /does not match/);
		const cancellation = new DurableOperation(root, path.join(root, "third-worktree"), "cancelled-operation");
		cancellation.cancel("cancelled-digest");
		const late = new DurableOperation(root, path.join(root, "fourth-worktree"), "cancelled-operation");
		assert.equal(late.claim({ digest: "cancelled-digest", requestHash: "late" }), false);
		assert.equal(late.cancelled(), true);
		assert.equal(late.runId, cancellation.runId);
	} finally { fs.rmSync(root, { recursive: true, force: true }); }
});

it("keeps an anchor-only reservation pending and resolves relative cwd against its original scope", async () => {
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "durable-anchor-only-"));
	const originalCwd = path.join(root, "original-worktree");
	const operation = new DurableOperation(root, originalCwd, launch.operationId);
	operation.claim({ digest: launch.digest, requestHash: operationRequestHash({ agent: launch.agent, task: launch.task, output: true, executionLifetime: launch.executionLifetime, async: true, cwd: path.join(originalCwd, "sub") }) });
	fs.rmSync(path.join(operation.directory, "intent.json"));
	const events = createEventBus();
	let dispatches = 0;
	const bridge = registerSubagentRpcBridge({ events, asyncDirRoot: root, getContext: () => context(path.join(root, "new-worktree")), execute: async (_id, params) => {
		dispatches++;
		assert.equal(params.cwd, path.join(originalCwd, "sub"));
		return { content: [], details: { mode: "single", results: [], runId: params.rpcOperationRunId } };
	} });
	try {
		assert.equal((await request(events, "lookup", { operationId: launch.operationId, digest: launch.digest })).state, "pending");
		await assert.rejects(request(events, "lookup", { operationId: launch.operationId, digest: "foreign" }), /digest/);
		assert.equal((await request(events, "spawn", { ...launch, cwd: "./sub" })).runId, operation.runId);
		assert.equal(dispatches, 1);
	} finally { bridge.dispose(); fs.rmSync(root, { recursive: true, force: true }); }
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

function diagnosticFixture(state: "running" | "complete" = "running") {
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "durable-diagnosis-"));
	const operation = new DurableOperation(root, root, launch.operationId);
	operation.claim({ digest: launch.digest, sessionId: "session-1" });
	const asyncDir = path.join(root, operation.runId);
	fs.mkdirSync(asyncDir);
	fs.writeFileSync(path.join(asyncDir, "status.json"), JSON.stringify({
		runId: operation.runId, sessionId: "session-1", mode: "single", state, startedAt: 1,
		runnerPhase: "model_stream", steps: [{ agent: "worker", status: state === "running" ? "running" : "complete", runnerPhase: "model_stream", lastToolFailure: { kind: "tool-execution-error", toolCallId: "failed-call", toolName: "bash", observedAt: 10, message: "command failed" } }],
	}));
	const events = createEventBus();
	const options: Options = { events, asyncDirRoot: root, getContext: () => context(root), execute: async () => assert.fail("diagnosis must not spawn or revive") };
	return { root, operation, asyncDir, events, options };
}

const diagnosis = { operationId: launch.operationId, digest: launch.digest, diagnosticId: "diagnostic-1", toolCallId: "failed-call", message: "Inspect the confirmed command error and use another approach." };

it("queues confirmed-failure guidance once across reply loss and restart without changing the current phase", async () => {
	const fixture = diagnosticFixture();
	let bridge = registerSubagentRpcBridge(fixture.options);
	try {
		const receipt = await request(fixture.events, "diagnose", diagnosis);
		assert.equal(receipt.state, "queued");
		assert.equal(receipt.guidanceOnly, true);
		const controls = consumeSteerRequests(fixture.asyncDir);
		assert.equal(controls.length, 1);
		assert.equal(controls[0]?.mode, "follow_up");
		assert.match(controls[0]?.id ?? "", /^diagnostic-[0-9a-f]{64}$/);
		bridge.dispose();
		bridge = registerSubagentRpcBridge({ ...fixture.options, getContext: () => context(fixture.root, "new-session") });
		assert.equal((await request(fixture.events, "diagnose", diagnosis)).state, "queued");
		assert.deepEqual(consumeSteerRequests(fixture.asyncDir), []);
		assert.equal(JSON.parse(fs.readFileSync(path.join(fixture.asyncDir, "status.json"), "utf8")).runnerPhase, "model_stream");
		await assert.rejects(request(fixture.events, "diagnose", { ...diagnosis, message: "Changed request" }), /do not match/);
	} finally { bridge.dispose(); fs.rmSync(fixture.root, { recursive: true, force: true }); }
});

it("does not replay an ambiguous diagnostic enqueue", async () => {
	const fixture = diagnosticFixture();
	fixture.operation.claimDiagnostic({ diagnosticId: diagnosis.diagnosticId, toolCallId: diagnosis.toolCallId, message: diagnosis.message });
	const bridge = registerSubagentRpcBridge(fixture.options);
	try {
		assert.equal((await request(fixture.events, "diagnose", diagnosis)).state, "pending");
		assert.equal((await request(fixture.events, "diagnose", diagnosis)).state, "pending");
		assert.deepEqual(consumeSteerRequests(fixture.asyncDir), []);
	} finally { bridge.dispose(); fs.rmSync(fixture.root, { recursive: true, force: true }); }
});

it("bounds transport filenames for long opaque diagnostic identifiers", async () => {
	const fixture = diagnosticFixture();
	const bridge = registerSubagentRpcBridge(fixture.options);
	try {
		const diagnosticId = "long diagnostic id ".repeat(13);
		assert.equal((await request(fixture.events, "diagnose", { ...diagnosis, diagnosticId })).state, "queued");
		const controls = consumeSteerRequests(fixture.asyncDir);
		assert.equal(controls.length, 1);
		assert.match(controls[0]?.id ?? "", /^diagnostic-[0-9a-f]{64}$/);
		assert.equal((await request(fixture.events, "diagnose", { ...diagnosis, diagnosticId })).state, "queued");
		assert.deepEqual(consumeSteerRequests(fixture.asyncDir), []);
	} finally { bridge.dispose(); fs.rmSync(fixture.root, { recursive: true, force: true }); }
});

it("rejects a tool call identifier that is ambiguous across live child sessions", async () => {
	const fixture = diagnosticFixture();
	const statusPath = path.join(fixture.asyncDir, "status.json");
	const status = JSON.parse(fs.readFileSync(statusPath, "utf8"));
	status.steps.push({ ...status.steps[0], agent: "another-worker" });
	fs.writeFileSync(statusPath, JSON.stringify(status));
	const bridge = registerSubagentRpcBridge(fixture.options);
	try {
		assert.equal((await request(fixture.events, "diagnose", diagnosis)).state, "rejected");
		assert.deepEqual(consumeSteerRequests(fixture.asyncDir), []);
	} finally { bridge.dispose(); fs.rmSync(fixture.root, { recursive: true, force: true }); }
});

for (const reason of ["cancelled", "terminal", "unconfirmed"] as const) it(`rejects diagnostic guidance for ${reason} work`, async () => {
	const fixture = diagnosticFixture(reason === "terminal" ? "complete" : "running");
	if (reason === "cancelled") fixture.operation.cancel(launch.digest);
	const bridge = registerSubagentRpcBridge(fixture.options);
	try {
		const receipt = await request(fixture.events, "diagnose", { ...diagnosis, toolCallId: reason === "unconfirmed" ? "healthy-call" : diagnosis.toolCallId });
		assert.equal(receipt.state, reason === "cancelled" ? "cancelled" : "rejected");
		assert.deepEqual(consumeSteerRequests(fixture.asyncDir), []);
	} finally { bridge.dispose(); fs.rmSync(fixture.root, { recursive: true, force: true }); }
});
