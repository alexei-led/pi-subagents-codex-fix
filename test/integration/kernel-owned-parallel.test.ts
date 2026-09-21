import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as path from "node:path";
import { describe, it } from "node:test";
import { Type } from "typebox";
import { Compile } from "typebox/compile";
import { cancelKernelOwnedProcess, observeKernelOwnedProcess } from "../../src/api/kernel-owned-process.mjs";
import { probeRuntimeOwnership } from "../../src/runs/background/runtime-ownership.ts";
import { createEventBus, makeAgent, makeMinimalCtx } from "../support/helpers.ts";
import { installAsyncExecutionHooks, makeAsyncExecutor, mockPi, tempDir, waitForAsyncResultFile, waitForAsyncState, waitForMockPiRuntime } from "../support/async-execution-fixture.ts";
import { registerSubagentRpcBridge, SUBAGENT_RPC_REQUEST_EVENT, subagentRpcReplyEvent, type SubagentRpcRequestEnvelope } from "../../src/extension/rpc.ts";
import { DurableOperation } from "../../src/runs/background/durable-operation.ts";

const launchReplyValidator = Compile(Type.Object({ success: Type.Literal(true), data: Type.Object({ isError: Type.Optional(Type.Boolean()), details: Type.Object({ asyncId: Type.Optional(Type.String()), ownedWorkflowKeys: Type.Optional(Type.Array(Type.String())) }) }) }));
function rpcLaunch(events: ReturnType<typeof createEventBus>, params: SubagentRpcRequestEnvelope["params"]) {
	return new Promise<{ isError?: boolean; details: { asyncId?: string; ownedWorkflowKeys?: string[] } }>((resolve, reject) => {
		const dispose = events.on(subagentRpcReplyEvent("kernel-spawn"), (reply) => {
			dispose();
			if (!launchReplyValidator.Check(reply)) reject(new Error(JSON.stringify(reply)));
			else resolve(reply.data);
		});
		events.emit(SUBAGENT_RPC_REQUEST_EVENT, { version: 1, requestId: "kernel-spawn", method: "spawn", params });
	});
}

async function waitForRetirement(operationDirectory: string): Promise<void> {
	for (let attempt = 0; attempt < 100; attempt++) {
		const observation = await observeKernelOwnedProcess(operationDirectory);
		if (observation.status === "retired") {
			assert.equal(observation.proof?.kind, "darwin-coalition-retired");
			return;
		}
		await new Promise((resolve) => setTimeout(resolve, 100));
	}
	assert.fail("Kernel-owned runner did not retire");
}

describe("kernel-owned parallel data through the native executor", () => {
	installAsyncExecutionHooks();
	for (const stop of [false, true]) {
		it(stop ? "stops both live children and observes coalition retirement" : "preserves ordered task keys and outputs under unbounded ownership", { timeout: 45_000 }, async (t) => {
			const operationRoot = path.join(tempDir, "operation-journal");
			const cacheDirectory = path.join(operationRoot, "kernel-cache");
			let capability = await probeRuntimeOwnership(cacheDirectory);
			const readinessDeadline = Date.now() + 15_000;
			while (!capability.supported && capability.reason === "Kernel ownership preflight is still pending." && Date.now() < readinessDeadline) {
				await new Promise((resolve) => setTimeout(resolve, 100));
				capability = await probeRuntimeOwnership(cacheDirectory);
			}
			assert.notEqual(capability.reason, "Kernel ownership preflight is still pending.");
			if (!capability.supported) { t.skip(capability.reason ?? "Kernel ownership unavailable"); return; }
			const operationDirectory = path.join(new DurableOperation(operationRoot, tempDir, "kernel-parallel").directory, "owned");
			const candidateDirectory = path.join(tempDir, "candidate");
			fs.mkdirSync(candidateDirectory);
			t.after(async () => { await cancelKernelOwnedProcess(operationDirectory, { deadlineMs: 5000 }); });
			const release = path.join(tempDir, "release");
			mockPi.onCall({ matchArgIncludes: "First task", waitForPath: release, output: "first owned result" });
			mockPi.onCall({ matchArgIncludes: "Second task", waitForPath: release, output: "second owned result" });
			const executor = makeAsyncExecutor([{ ...makeAgent("worker"), defaultTimeoutMs: 5 }], { timeoutMs: 5 });
			const ctx = makeMinimalCtx(tempDir);
			ctx.sessionManager.getSessionId = () => "kernel-parallel-session";
			const events = createEventBus();
			const rpc = registerSubagentRpcBridge({ events, operationDirRoot: operationRoot, getContext: () => ctx, execute: executor.executePublic });
			t.after(() => rpc.dispose());
			const launch = await rpcLaunch(events, {
				operationId: "kernel-parallel", digest: "kernel-parallel-digest",
				executionOwnership: { mode: "kernel" }, executionLifetime: { mode: "unbounded" },
				cwd: candidateDirectory,
				async: true, mission: false, context: "fresh",
				ownedWorkflow: { version: 1, kind: "parallel", concurrency: 2, tasks: [
					{ key: "first", agent: "worker", task: "First task", acceptance: false, output: false, executionLifetime: { mode: "unbounded" } },
					{ key: "second", agent: "worker", task: "Second task", acceptance: false, output: false, executionLifetime: { mode: "unbounded" } },
				] },
			});
			assert.notEqual(launch.isError, true, JSON.stringify(launch));
			assert.deepEqual(launch.details.ownedWorkflowKeys, ["first", "second"]);
			const runId = launch.details.asyncId;
			assert.ok(runId);
			await waitForMockPiRuntime(mockPi, 1, 15_000);
			const running = await waitForAsyncState(runId, (status) => status.steps?.filter((step) => step.status === "running").length === 2, 15_000);
			assert.equal(running.timeoutMs, undefined);
			assert.equal(running.deadlineAt, undefined);
			assert.deepEqual(running.effectiveExecutionLifetime, { mode: "unbounded" });
			if (stop) {
				const stopped = await executor.execute("stop-kernel-parallel", { action: "stop", id: runId }, new AbortController().signal, undefined, ctx);
				assert.notEqual(stopped.isError, true, JSON.stringify(stopped));
			} else fs.writeFileSync(release, "go");
			await waitForRetirement(operationDirectory);
			if (stop) {
				const stopped = await waitForAsyncState(runId, (status) => status.state === "stopped", 15_000);
				assert.equal(stopped.stopped, true);
				assert.ok(stopped.steps?.every((step) => step.status === "stopped"));
			} else {
				const result = JSON.parse(fs.readFileSync(await waitForAsyncResultFile(runId, 15_000), "utf8"));
				assert.equal(result.success, true, JSON.stringify(result));
				assert.deepEqual(result.ownedWorkflowKeys, ["first", "second"]);
				assert.deepEqual(result.results.map((child) => child.output), ["first owned result", "second owned result"]);
			}
			assert.deepEqual(fs.readdirSync(candidateDirectory), []);
		});
	}
});
