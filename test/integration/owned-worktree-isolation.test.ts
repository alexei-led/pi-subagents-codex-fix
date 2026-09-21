import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import * as fs from "node:fs";
import * as path from "node:path";
import { fileURLToPath } from "node:url";
import { describe, it } from "node:test";
import { Type } from "typebox";
import { Compile } from "typebox/compile";
import { preflightKernelOwnedProcess, cancelKernelOwnedProcess, observeKernelOwnedProcess } from "../../src/api/kernel-owned-process.mjs";
import { ownedGitEnvironment } from "../../src/runs/shared/owned-git-environment.ts";
import { setChildSessionFactoryModule } from "../../src/runs/shared/child-session.ts";
import { registerSubagentRpcBridge, SUBAGENT_RPC_REQUEST_EVENT, subagentRpcReplyEvent } from "../../src/extension/rpc.ts";
import { DurableOperation } from "../../src/runs/background/durable-operation.ts";
import { createEventBus, makeAgent, makeMinimalCtx } from "../support/helpers.ts";
import { installAsyncExecutionHooks, makeAsyncExecutor, mockPi, tempDir, waitForAsyncResultFile, waitForAsyncState } from "../support/async-execution-fixture.ts";

const receiptValidator = Compile(Type.Object({ success: Type.Literal(true), data: Type.Object({ isError: Type.Optional(Type.Boolean()), details: Type.Object({ asyncId: Type.String() }) }) }));

describe("owned parallel worktree isolation", () => {
  installAsyncExecutionHooks();
  for (const variant of ["request", "configuration", "isolation-alias", "explicit-false", "isolation-none", "nested", "single-explicit-false"] as const) it(`preserves ${variant} worktree policy at the real child boundary`, { timeout: 60_000 }, async t => {
    const operationRoot = path.join(tempDir, "journal");
    let capability = await preflightKernelOwnedProcess({ artifactDirectory: path.join(operationRoot, "kernel-cache") });
    const readinessDeadline = Date.now() + 15_000;
    while (!capability.supported && capability.reason === "kernel-runtime-initializing" && Date.now() < readinessDeadline) capability = await preflightKernelOwnedProcess({ artifactDirectory: path.join(operationRoot, "kernel-cache") });
    assert.notEqual(capability.reason, "kernel-runtime-initializing");
    if (!capability.supported) { t.skip(capability.reason ?? "Kernel ownership unavailable"); return; }
    const source = path.join(tempDir, "source");
    fs.mkdirSync(source);
    const env = ownedGitEnvironment(process.env);
    const git = (...args: string[]) => execFileSync("git", ["-c", "core.hooksPath=/dev/null", "-c", "user.name=Test", "-c", "user.email=test@example.invalid", ...args], { cwd: source, env, encoding: "utf8" }).trim();
    git("init", "--quiet", "-b", "main");
    fs.writeFileSync(path.join(source, "shared.txt"), "main\n");
    git("add", "shared.txt"); git("commit", "--quiet", "-m", "main");
    const sourceHead = git("rev-parse", "HEAD");
    git("checkout", "--quiet", "-b", "owned-base");
    fs.writeFileSync(path.join(source, "shared.txt"), "base\n");
    git("commit", "--quiet", "-am", "base");
    const alternateHead = git("rev-parse", "HEAD");
    git("checkout", "--quiet", "main");
    const indexBefore = fs.readFileSync(path.join(source, ".git", "index"));
    const hook = path.join(tempDir, "setup-hook.mjs");
    fs.writeFileSync(hook, `#!${process.execPath}\nimport fs from 'node:fs';import {inspectInheritedKernelOwnedProcessMembership} from ${JSON.stringify(new URL("../../src/api/kernel-owned-process.mjs", import.meta.url).href)};const membership=await inspectInheritedKernelOwnedProcessMembership(process.env.PI_KERNEL_OWNED_OPERATION);if(!membership.owned)throw new Error('Unowned setup hook');fs.appendFileSync(${JSON.stringify(path.join(tempDir, "hooks.jsonl"))},JSON.stringify({cwd:process.cwd(),membership})+'\\n');fs.writeFileSync('hook-marker','owned');process.stdout.write(JSON.stringify({syntheticPaths:['hook-marker']}));`, { mode: 0o755 });
    const isolated = variant !== "explicit-false" && variant !== "isolation-none" && variant !== "single-explicit-false";
    const config = { worktree: variant === "configuration" || variant === "explicit-false" || variant === "isolation-none" || variant === "nested" || variant === "single-explicit-false", worktreeProvider: "native", worktreeBaseDir: path.join(tempDir, "isolated-trees"), worktreeBranchPrefix: "owned-review", worktreeSetupHook: hook, worktreeSetupHookTimeoutMs: variant === "configuration" ? 5000 : undefined, timeoutMs: 5 };
    const parallelParams = { async: true, cwd: source, mission: false, artifacts: false, context: "fresh", executionLifetime: { mode: "unbounded" }, executionOwnership: { mode: "kernel" }, ownedWorkflow: { version: 1, kind: "parallel", concurrency: 2, tasks: [{ key: "first", agent: "first", task: "Write first", output: false, acceptance: false, skill: false }, { key: "second", agent: "second", task: "Write second", output: false, acceptance: false, skill: false }] } };
    if (variant === "request") Object.assign(parallelParams, { worktree: true, baseRef: "owned-base" });
    if (variant === "explicit-false") Object.assign(parallelParams, { worktree: false });
    if (variant === "isolation-alias") Object.assign(parallelParams, { isolation: "worktree" });
    if (variant === "isolation-none") Object.assign(parallelParams, { isolation: "none" });
    const specification = path.join(tempDir, "worktree-spec.json");
    fs.writeFileSync(specification, JSON.stringify({ directory: tempDir, config, parallelParams }));
    const previous = process.env.PI_OWNED_WORKTREE_SPEC;
    process.env.PI_OWNED_WORKTREE_SPEC = specification;
    setChildSessionFactoryModule(fileURLToPath(new URL("../fixtures/owned-worktree-factory.mjs", import.meta.url)));
    mockPi.onCall({ matchArgIncludes: "Write first", delayMs: 250, output: "first" });
    mockPi.onCall({ matchArgIncludes: "Write second", delayMs: 250, output: "second" });
    if (variant === "nested") mockPi.onCall({ matchArgIncludes: "Coordinate", output: "coordinated" });
    if (variant === "single-explicit-false") mockPi.onCall({ matchArgIncludes: "Judge", output: "judged" });
    const executor = makeAsyncExecutor([makeAgent("first"), makeAgent("second"), makeAgent("coordinator"), makeAgent("judge")], variant === "nested" ? { ...config, worktree: false } : config);
    const events = createEventBus();
    const rpc = registerSubagentRpcBridge({ events, operationDirRoot: operationRoot, getContext: () => makeMinimalCtx(source), execute: executor.executePublic });
    const operationId = "worktree-contract";
    const owned = path.join(new DurableOperation(operationRoot, source, operationId).directory, "owned");
    try {
      const receipt = new Promise<string>((resolve, reject) => {
        const dispose = events.on(subagentRpcReplyEvent("owned-worktree"), value => { dispose(); if (!receiptValidator.Check(value) || value.data.isError) reject(new Error(JSON.stringify(value))); else resolve(value.data.details.asyncId); });
      });
      const params = variant === "nested" || variant === "single-explicit-false" ? { agent: variant === "nested" ? "coordinator" : "judge", task: variant === "nested" ? "Coordinate" : "Judge", async: true, cwd: source, mission: false, artifacts: false, output: false, acceptance: false, context: "fresh", worktree: false, executionLifetime: { mode: "unbounded" }, executionOwnership: { mode: "kernel" } } : parallelParams;
      events.emit(SUBAGENT_RPC_REQUEST_EVENT, { version: 1, requestId: "owned-worktree", method: "spawn", params: { ...params, operationId, digest: "worktree-digest" } });
      const id = await receipt;
      const rootResult = JSON.parse(fs.readFileSync(await waitForAsyncResultFile(id, 35_000), "utf8"));
      assert.equal(rootResult.success, true, JSON.stringify(rootResult));
      const status = variant === "nested" ? JSON.parse(fs.readFileSync(path.join(tempDir, "nested-status.json"), "utf8")) : await waitForAsyncState(id, state => state.state === "complete");
      const result = variant === "nested" ? JSON.parse(fs.readFileSync(path.join(tempDir, "nested-result.json"), "utf8")) : rootResult;
      assert.equal(result.success, true, JSON.stringify(result));
      assert.equal(result.cwd, source);
      assert.equal(status.cwd, source);
      const children = fs.readFileSync(path.join(tempDir, "children.jsonl"), "utf8").trim().split("\n").map(line => JSON.parse(line));
      assert.equal(children.length, variant === "single-explicit-false" ? 1 : 2);
      assert.equal(new Set(children.map(child => child.cwd)).size, isolated ? 2 : 1, JSON.stringify(children));
      for (const child of children) {
        assert.equal(child.head, variant === "request" ? alternateHead : sourceHead);
        assert.deepEqual(child.lifetime, { mode: "unbounded" });
        assert.equal(child.hook, isolated);
        assert.equal(child.membership.owned, true);
        if (isolated) { assert.notEqual(child.cwd, source); assert.equal(child.cwd.startsWith(config.worktreeBaseDir + path.sep), true); assert.equal(child.branch.startsWith("owned-review/"), true); assert.equal(child.written, `${child.agent}\n`); assert.equal(status.steps.find(step => step.agent === child.agent).worktreePath, child.cwd); }
        else assert.equal(child.cwd, source);
      }
      assert.equal(git("rev-parse", "HEAD"), sourceHead);
      if (isolated) {
        assert.equal(fs.readFileSync(path.join(source, "shared.txt"), "utf8"), "main\n"); assert.equal(git("status", "--porcelain"), ""); assert.deepEqual(fs.readFileSync(path.join(source, ".git", "index")), indexBefore); assert.equal(fs.readFileSync(path.join(tempDir, "hooks.jsonl"), "utf8").trim().split("\n").length, 2);
        const handoff = JSON.parse(fs.readFileSync(status.parallelHandoff.path, "utf8"));
        assert.equal(handoff.groups[0].baseCommit, variant === "request" ? alternateHead : sourceHead);
        for (const child of handoff.groups[0].children) assert.match(fs.readFileSync(child.patch.path, "utf8"), new RegExp(`\\+${child.agent}\\n`));
      }
      else assert.equal(fs.existsSync(path.join(tempDir, "hooks.jsonl")), false);
      for (let attempt = 0; attempt < 100 && (await observeKernelOwnedProcess(owned)).status !== "retired"; attempt++) await new Promise(resolve => setTimeout(resolve, 50));
      assert.equal((await observeKernelOwnedProcess(owned)).status, "retired");
    } finally { rpc.dispose(); await cancelKernelOwnedProcess(owned, { deadlineMs: 5000 }); if (previous === undefined) delete process.env.PI_OWNED_WORKTREE_SPEC; else process.env.PI_OWNED_WORKTREE_SPEC = previous; }
  });
});
