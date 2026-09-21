import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as path from "node:path";
import { describe, it } from "node:test";
import { preflightKernelOwnedProcess, cancelKernelOwnedProcess, observeKernelOwnedProcess } from "../../src/api/kernel-owned-process.mjs";
import { observeNativeKernelRun } from "../../src/runs/background/runtime-ownership.ts";
import { readAsyncRecoveryDescriptor } from "../../src/runs/background/async-resume.ts";
import { setChildSessionFactoryModule } from "../../src/runs/shared/child-session.ts";
import { installAsyncExecutionHooks, makeAsyncExecutor, mockPi, tempDir, waitForAsyncResultFile, ASYNC_DIR } from "../support/async-execution-fixture.ts";
import { makeAgent, makeMinimalCtx } from "../support/helpers.ts";

describe("owned recovery through the public resume boundary", () => {
  installAsyncExecutionHooks();
  it("preserves owned metadata and refuses ordinary revival without downgrade", { timeout: 60_000 }, async t => {
    let capability = await preflightKernelOwnedProcess({ artifactDirectory: path.join(tempDir, "kernel-cache") });
    const readinessDeadline = Date.now() + 15_000;
    while (!capability.supported && capability.reason === "kernel-runtime-initializing" && Date.now() < readinessDeadline) capability = await preflightKernelOwnedProcess({ artifactDirectory: path.join(tempDir, "kernel-cache") });
    assert.notEqual(capability.reason, "kernel-runtime-initializing");
    if (!capability.supported) { t.skip(capability.reason ?? "Kernel ownership unavailable"); return; }
    const owned = path.join(tempDir, "owned");
    const release = path.join(tempDir, "release-descendant");
    const factory = path.join(tempDir, "resume-factory.mjs");
    const descendant = `const fs=require('node:fs');const timer=setInterval(()=>{if(fs.existsSync(${JSON.stringify(release)}))clearInterval(timer)},20);`;
    fs.writeFileSync(factory, `import {spawn} from 'node:child_process';import {createFakeChildSessions} from ${JSON.stringify(new URL("../support/fake-child-session.ts", import.meta.url).href)};export default function(){const fake=createFakeChildSessions(()=>process.env.MOCK_PI_QUEUE_DIR).factory;return{create:launch=>{if(process.env.PI_KERNEL_OWNED_OPERATION){const child=spawn(process.execPath,['-e',${JSON.stringify(descendant)}],{detached:true,stdio:'ignore',env:process.env});child.unref()}return fake.create(launch)},dispose:()=>fake.dispose()}}`);
    setChildSessionFactoryModule(factory);
    try {
    mockPi.onCall({ output: "owned initial output" });
    const executor = makeAsyncExecutor([makeAgent("worker")]);
    const ctx = makeMinimalCtx(tempDir);
    const launch = await executor.execute("owned-resume-source", { agent: "worker", task: "Owned original", async: true, output: false, acceptance: false, mission: false, context: "fresh", executionLifetime: { mode: "unbounded" }, executionOwnership: { mode: "kernel" }, rpcKernelOperationDirectory: owned }, new AbortController().signal, undefined, ctx);
    assert.notEqual(launch.isError, true, JSON.stringify(launch));
    assert.ok(launch.details.asyncId);
    const id = launch.details.asyncId;
    const asyncDir = path.join(ASYNC_DIR, id);
    const resultPath = await waitForAsyncResultFile(id, 15_000);
    const descriptor = readAsyncRecoveryDescriptor(asyncDir);
    assert.deepEqual(descriptor?.executionOwnership, { mode: "kernel" });
    assert.equal(descriptor?.kernelOperationDirectory, owned);
    const before = fs.readdirSync(ASYNC_DIR).sort();
    const resume = async (override: "omitted" | "undefined" | "legacy" | "kernel") => {
      const params = { action: "resume", id, message: "Continue without a new operation" };
      if (override === "undefined") Object.assign(params, { executionOwnership: undefined });
      if (override === "legacy") Object.assign(params, { executionOwnership: { mode: "legacy" } });
      if (override === "kernel") Object.assign(params, { executionOwnership: { mode: "kernel" } });
      const resumed = await executor.executePublic(`resume-${override}`, params, new AbortController().signal, undefined, ctx);
      if (!resumed.isError && resumed.details.asyncId) await executor.execute("cleanup-unexpected-revival", { action: "stop", id: resumed.details.asyncId }, new AbortController().signal, undefined, ctx);
      assert.equal(resumed.isError, true, JSON.stringify(resumed));
      assert.deepEqual(fs.readdirSync(ASYNC_DIR).sort(), before);
      assert.equal(mockPi.callCount(), 1);
    };
    assert.equal((await observeKernelOwnedProcess(owned)).status, "active");
    await resume("omitted");
    fs.writeFileSync(release, "finish");
    for (let attempt = 0; attempt < 150 && (await observeNativeKernelRun(owned, id)).processTerminalProof.state !== "observed"; attempt++) await new Promise(resolve => setTimeout(resolve, 50));
    assert.equal((await observeNativeKernelRun(owned, id)).processTerminalProof.state, "observed");
    const statusPath = path.join(asyncDir, "status.json");
    const descriptorPath = path.join(asyncDir, "recovery-descriptor.json");
    const originalStatus = JSON.parse(fs.readFileSync(statusPath, "utf8"));
    const originalResult = JSON.parse(fs.readFileSync(resultPath, "utf8"));
    const proofPath = path.join(asyncDir, "process-terminal.json");
    const originalProof = JSON.parse(fs.readFileSync(proofPath, "utf8"));
    for (const state of ["complete", "paused", "failed"]) {
      fs.writeFileSync(statusPath, JSON.stringify({ ...originalStatus, state }));
      for (const override of ["omitted", "undefined", "legacy", "kernel"] as const) await resume(override);
    }
    const legacyDescriptor = { ...descriptor };
    delete legacyDescriptor.executionOwnership;
    delete legacyDescriptor.kernelOperationDirectory;
    fs.writeFileSync(descriptorPath, JSON.stringify(legacyDescriptor));
    for (const evidence of ["status", "result", "proof", "embedded-proof"]) {
      const status = { ...originalStatus };
      const result = { ...originalResult };
      if (evidence !== "status") { delete status.effectiveExecutionOwnership; delete status.kernelOperationDirectory; }
      if (evidence !== "result") { delete result.effectiveExecutionOwnership; delete result.kernelOperationDirectory; }
      delete status.processTerminal;
      fs.rmSync(proofPath, { force: true });
      if (evidence === "proof") fs.writeFileSync(proofPath, JSON.stringify(originalProof));
      if (evidence === "embedded-proof") status.processTerminal = originalProof;
      fs.writeFileSync(statusPath, JSON.stringify(status));
      fs.writeFileSync(resultPath, JSON.stringify(result));
      await resume("omitted");
    }
    for (const malformed of [{}, { kernelProof: {} }, { ...originalProof, runId: "foreign-run" }]) {
      const status = { ...originalStatus };
      delete status.effectiveExecutionOwnership;
      delete status.kernelOperationDirectory;
      delete status.processTerminal;
      fs.writeFileSync(statusPath, JSON.stringify(status));
      fs.writeFileSync(proofPath, JSON.stringify(malformed));
      await resume("omitted");
      fs.rmSync(proofPath);
      fs.writeFileSync(statusPath, JSON.stringify({ ...status, processTerminal: malformed }));
      await resume("omitted");
    }
    } finally { fs.writeFileSync(release, "finish"); await cancelKernelOwnedProcess(owned, { deadlineMs: 5000 }); }
  });
});
