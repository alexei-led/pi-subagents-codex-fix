import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as path from "node:path";
import { describe, it } from "node:test";
import { cancelKernelOwnedProcess, observeKernelOwnedProcess, preflightKernelOwnedProcess } from "../../src/api/kernel-owned-process.mjs";
import { makeAgent, makeMinimalCtx } from "../support/helpers.ts";
import { installAsyncExecutionHooks, makeAsyncExecutor, mockPi, tempDir, waitForAsyncResultFile, waitForAsyncState, waitForMockPiRuntime } from "../support/async-execution-fixture.ts";

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
			const capability = await preflightKernelOwnedProcess({ artifactDirectory: path.join(tempDir, "kernel-artifacts") });
			if (!capability.supported) { t.skip(capability.reason ?? "Kernel ownership unavailable"); return; }
			const operationDirectory = path.join(tempDir, "owned-operation");
			t.after(async () => { await cancelKernelOwnedProcess(operationDirectory, { deadlineMs: 5000 }); });
			const release = path.join(tempDir, "release");
			mockPi.onCall({ matchArgIncludes: "First task", waitForPath: release, output: "first owned result" });
			mockPi.onCall({ matchArgIncludes: "Second task", waitForPath: release, output: "second owned result" });
			const executor = makeAsyncExecutor([{ ...makeAgent("worker"), defaultTimeoutMs: 5 }], { timeoutMs: 5 });
			const ctx = makeMinimalCtx(tempDir);
			ctx.sessionManager.getSessionId = () => "kernel-parallel-session";
			const launch = await executor.execute("kernel-parallel", {
				executionOwnership: { mode: "kernel" }, executionLifetime: { mode: "unbounded" },
				rpcKernelOperationDirectory: operationDirectory, async: true, mission: false, context: "fresh",
				ownedWorkflow: { version: 1, kind: "parallel", concurrency: 2, tasks: [
					{ key: "first", agent: "worker", task: "First task", acceptance: false, output: false, executionLifetime: { mode: "unbounded" } },
					{ key: "second", agent: "worker", task: "Second task", acceptance: false, output: false, executionLifetime: { mode: "unbounded" } },
				] },
			}, new AbortController().signal, undefined, ctx);
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
			const result = JSON.parse(fs.readFileSync(await waitForAsyncResultFile(runId, 15_000), "utf8"));
			assert.equal(result.success, !stop, JSON.stringify(result));
			assert.deepEqual(result.ownedWorkflowKeys, ["first", "second"]);
			if (stop) assert.equal(result.stopped, true);
			else assert.deepEqual(result.results.map((child) => child.output), ["first owned result", "second owned result"]);
			await waitForRetirement(operationDirectory);
		});
	}
});
