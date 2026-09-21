import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import * as fs from "node:fs";
import * as path from "node:path";
import { fileURLToPath } from "node:url";
import { describe, it } from "node:test";
import { Type } from "typebox";
import { Compile } from "typebox/compile";
import { cancelKernelOwnedProcess } from "../../src/api/kernel-owned-process.mjs";
import { DurableOperation, claimNativeOperationDispatch } from "../../src/runs/background/durable-operation.ts";
import { probeRuntimeOwnership } from "../../src/runs/background/runtime-ownership.ts";
import { registerSubagentRpcBridge, SUBAGENT_RPC_REQUEST_EVENT, subagentRpcReplyEvent, type SubagentRpcMethod } from "../../src/extension/rpc.ts";
import { createEventBus, makeAgent, makeMinimalCtx } from "../support/helpers.ts";
import { installAsyncExecutionHooks, makeAsyncExecutor, mockPi, tempDir, ASYNC_DIR, RESULTS_DIR } from "../support/async-execution-fixture.ts";

const replyValidator = Compile(Type.Object({ success: Type.Literal(true), data: Type.Object({ neverStarted: Type.Boolean(), state: Type.String(), runId: Type.String(), processTerminalProof: Type.Optional(Type.Object({ state: Type.String() })) }) }));

describe("native dispatch crash recovery", () => {
  installAsyncExecutionHooks();
  for (const scenario of ["anchor", "claim", "prepared", "dispatch", "prepared-cancel", "live-owner"] as const) it(`recovers the ${scenario} boundary without a replacement launch`, { timeout: 45_000 }, async t => {
    const root = path.join(tempDir, "journal");
    let capability = await probeRuntimeOwnership(path.join(root, "kernel-cache"));
    const deadline = Date.now() + 15_000;
    while (!capability.supported && capability.reason === "Kernel ownership preflight is still pending." && Date.now() < deadline) capability = await probeRuntimeOwnership(path.join(root, "kernel-cache"));
    assert.notEqual(capability.reason, "Kernel ownership preflight is still pending.");
    if (!capability.supported) { t.skip(capability.reason ?? "Kernel ownership unavailable"); return; }
    const params = { operationId: "crashed-launch", digest: "crashed-digest", agent: "worker", task: "Crash recovery work", async: true, mission: false, output: false, acceptance: false, context: "fresh", executionOwnership: { mode: "kernel" }, executionLifetime: { mode: "unbounded" } };
    const operation = new DurableOperation(root, tempDir, params.operationId);
    const owned = path.join(operation.directory, "owned");
    t.after(async () => { await cancelKernelOwnedProcess(owned, { deadlineMs: 5000 }); });
    mockPi.onCall({ output: "recovered same prepared worker" });
    const specification = path.join(tempDir, "crash-spec.json");
    fs.writeFileSync(specification, JSON.stringify({ cwd: tempDir, root, params, cut: scenario === "prepared-cancel" ? "prepared" : scenario }));
    const actor = spawn(process.execPath, ["--experimental-strip-types", "--import", fileURLToPath(new URL("../support/register-loader.mjs", import.meta.url)), fileURLToPath(new URL("../fixtures/native-dispatch-crash.mjs", import.meta.url)), specification], { env: process.env, stdio: ["ignore", "ignore", "pipe"] });
    let stderr = "";
    actor.stderr.on("data", chunk => { stderr += String(chunk); });
    const closed = once(actor, "close");
    t.after(() => { if (actor.exitCode === null) actor.kill("SIGKILL"); });
    const events = createEventBus();
    const rpc = registerSubagentRpcBridge({ events, operationDirRoot: root, asyncDirRoot: ASYNC_DIR, resultsDir: RESULTS_DIR, getContext: () => makeMinimalCtx(path.join(tempDir, "restarted-session")), execute: async () => { assert.fail("Recovery must never call the original executor again"); } });
    t.after(() => rpc.dispose());
    let sequence = 0;
    function request(method: SubagentRpcMethod) {
      const requestId = `recover-${++sequence}`;
      return new Promise<{ neverStarted: boolean; state: string; runId: string; processTerminalProof?: { state: string } }>((resolve, reject) => {
        const dispose = events.on(subagentRpcReplyEvent(requestId), value => {
          dispose();
          if (!replyValidator.Check(value)) reject(new Error(JSON.stringify(value)));
          else resolve(value.data);
        });
        events.emit(SUBAGENT_RPC_REQUEST_EVENT, { version: 1, requestId, method, params });
      });
    }
    if (scenario === "live-owner") {
      const ownerDeadline = Date.now() + 15_000;
      while (!fs.existsSync(path.join(root, "owner-ready")) && actor.exitCode === null && Date.now() < ownerDeadline) await new Promise(resolve => setTimeout(resolve, 25));
      assert.equal(fs.existsSync(path.join(root, "owner-ready")), true, stderr);
      assert.equal((await request("lookup")).neverStarted, false);
      assert.equal((await request("spawn")).neverStarted, false);
      assert.equal(fs.existsSync(path.join(operation.directory, "dispatch-decision.json")), false);
      fs.writeFileSync(path.join(root, "release-owner"), "continue");
    }
    const [exitCode] = await closed;
    assert.equal(exitCode, scenario === "live-owner" ? 0 : 73, stderr);
    assert.equal(fs.existsSync(path.join(owned, "request.json")), scenario !== "claim" && scenario !== "anchor");
    assert.equal(fs.existsSync(path.join(operation.directory, "dispatch-decision.json")), scenario === "dispatch" || scenario === "live-owner");
    if (scenario === "prepared-cancel") await request("cancel");
    let observation = await request("lookup");
    if (scenario === "anchor" || scenario === "claim" || scenario === "prepared-cancel") {
      assert.equal(observation.neverStarted, true);
      assert.equal(claimNativeOperationDispatch(owned, operation.runId), false);
      assert.equal((await request("spawn")).neverStarted, true);
      if (scenario === "claim" || scenario === "anchor") {
        const executor = makeAsyncExecutor([makeAgent("worker")]);
        const late = await executor.execute("late-origin", { ...params, executionOwnership: { mode: "kernel" }, executionLifetime: { mode: "unbounded" }, context: "fresh", rpcOperationRunId: operation.runId, rpcKernelOperationDirectory: owned }, new AbortController().signal, undefined, makeMinimalCtx(tempDir));
        assert.equal(late.isError, true);
      }
      assert.equal(mockPi.callCount(), 0);
    } else {
      const recoveryDeadline = Date.now() + 20_000;
      while (observation.processTerminalProof?.state !== "observed" && Date.now() < recoveryDeadline) { await new Promise(resolve => setTimeout(resolve, 50)); observation = await request("lookup"); }
      assert.equal(observation.neverStarted, false);
      assert.equal(observation.processTerminalProof?.state, "observed");
      assert.equal(mockPi.callCount(), 1);
      assert.equal((await request("spawn")).runId, observation.runId);
    }
  });
});
