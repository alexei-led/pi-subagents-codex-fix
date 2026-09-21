import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as path from "node:path";
import { describe, it } from "node:test";
import { makeAgent, makeMinimalCtx } from "../support/helpers.ts";
import { ASYNC_DIR, installAsyncExecutionHooks, makeAsyncExecutor, mockPi, readAsyncPayload, tempDir, waitForAsyncState, waitForMockPiRuntime } from "../support/async-execution-fixture.ts";

describe("background workflow ownership of explicit async children", () => {
	installAsyncExecutionHooks();
	for (const stop of [false, true]) {
		it(stop ? "stops its live child through the workflow abort signal" : "waits for the child terminal output before publishing completion", async () => {
			const release = path.join(tempDir, "child-release");
			mockPi.onCall({ waitForPath: release, output: "owned background child completed" });
			const executor = makeAsyncExecutor([makeAgent("worker")]);
			const ctx = makeMinimalCtx(tempDir);
			ctx.sessionManager.getSessionId = () => "workflow-owned-child";
			const launch = await executor.execute("owned-child", {
				workflowScript: 'return await runs.run("owned", { agent: "worker", task: "Wait for release", async: true, acceptance: false });',
				async: true, executionLifetime: { mode: "unbounded" }, mission: false, context: "fresh",
			}, new AbortController().signal, undefined, ctx);
			assert.notEqual(launch.isError, true, JSON.stringify(launch));
			const workflowId = launch.details.asyncId;
			assert.ok(workflowId);
			const runtime = await waitForMockPiRuntime(mockPi, 0, 15_000);
			assert.ok(runtime.runId);
			const workflowStatus = JSON.parse(fs.readFileSync(path.join(ASYNC_DIR, workflowId, "status.json"), "utf8"));
			assert.equal(workflowStatus.state, "running");
			if (stop) {
				const stopped = await executor.execute("stop-owned-child", { action: "stop", id: workflowId }, new AbortController().signal, undefined, ctx);
				assert.notEqual(stopped.isError, true, JSON.stringify(stopped));
				const status = await waitForAsyncState(runtime.runId, (candidate) => candidate.state === "stopped", 15_000);
				assert.equal(status.stopped, true);
			} else fs.writeFileSync(release, "go");
			const result = await readAsyncPayload(workflowId);
			assert.equal(result.success, !stop, JSON.stringify(result));
			if (!stop) assert.match(JSON.stringify(result), /owned background child completed/);
		});
	}
});
