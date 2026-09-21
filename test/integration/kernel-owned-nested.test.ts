import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as path from "node:path";
import { describe, it } from "node:test";
import { fileURLToPath } from "node:url";
import { preflightKernelOwnedProcess, cancelKernelOwnedProcess, observeKernelOwnedProcess } from "../../src/api/kernel-owned-process.mjs";
import { setChildSessionFactoryModule } from "../../src/runs/shared/child-session.ts";
import { installAsyncExecutionHooks, makeAsyncExecutor, mockPi, tempDir, waitForAsyncResultFile } from "../support/async-execution-fixture.ts";
import { makeAgent, makeMinimalCtx } from "../support/helpers.ts";

describe("kernel marker producer to native nested consumers", () => {
  installAsyncExecutionHooks();
  it("runs foreground and async descendants with the real gate descriptor and rejects foreign bindings", { timeout: 45_000 }, async t => {
    const capability = await preflightKernelOwnedProcess({ artifactDirectory: path.join(tempDir, "kernel-cache") });
    if (!capability.supported) { t.skip(capability.reason ?? "Kernel ownership unavailable"); return; }
    const operationDirectory = path.join(tempDir, "owned-root");
    const output = path.join(tempDir, "nested-observation.json");
    const previous = process.env.PI_KERNEL_NESTED_TEST_RESULT;
    process.env.PI_KERNEL_NESTED_TEST_RESULT = output;
    t.after(async () => {
      await cancelKernelOwnedProcess(operationDirectory, { deadlineMs: 5000 });
      if (previous === undefined) delete process.env.PI_KERNEL_NESTED_TEST_RESULT;
      else process.env.PI_KERNEL_NESTED_TEST_RESULT = previous;
    });
    setChildSessionFactoryModule(fileURLToPath(new URL("../fixtures/kernel-owned-nested-factory.mjs", import.meta.url)));
    mockPi.onCall({ matchArgIncludes: "Nested foreground", output: "foreground completed" });
    mockPi.onCall({ matchArgIncludes: "Nested background", output: "background completed" });
    mockPi.onCall({ matchArgIncludes: "Root task", output: "root completed" });
    const executor = makeAsyncExecutor([makeAgent("worker")]);
    const launched = await executor.execute("owned-root", { agent: "worker", task: "Root task", async: true, output: false, acceptance: false, mission: false, context: "fresh", executionLifetime: { mode: "unbounded" }, executionOwnership: { mode: "kernel" }, rpcKernelOperationDirectory: operationDirectory }, new AbortController().signal, undefined, makeMinimalCtx(tempDir));
    assert.notEqual(launched.isError, true, JSON.stringify(launched));
    assert.ok(launched.details.asyncId);
    const result = JSON.parse(fs.readFileSync(await waitForAsyncResultFile(launched.details.asyncId, 25_000), "utf8"));
    assert.equal(result.success, true, JSON.stringify(result));
    const nested = JSON.parse(fs.readFileSync(output, "utf8"));
    assert.equal(nested.descriptor.operationDirectory, operationDirectory);
    assert.equal(nested.membership.identity.operationId, nested.descriptor.operationId);
    assert.equal(nested.status.state, "complete");
    assert.deepEqual(nested.status.effectiveExecutionLifetime, { mode: "unbounded" });
    for (let attempt = 0; attempt < 100 && (await observeKernelOwnedProcess(operationDirectory)).status !== "retired"; attempt++) await new Promise(resolve => setTimeout(resolve, 50));
    assert.equal((await observeKernelOwnedProcess(operationDirectory)).status, "retired");
  });
});
