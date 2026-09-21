import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as path from "node:path";
import { describe, it } from "node:test";
import { Type } from "typebox";
import { Compile } from "typebox/compile";
import { probeRuntimeOwnership } from "../../src/runs/background/runtime-ownership.ts";
import { DurableOperation } from "../../src/runs/background/durable-operation.ts";
import { registerSubagentRpcBridge, SUBAGENT_RPC_REQUEST_EVENT, subagentRpcReplyEvent, type SubagentRpcMethod } from "../../src/extension/rpc.ts";
import { createEventBus, makeAgent, makeMinimalCtx } from "../support/helpers.ts";
import { installAsyncExecutionHooks, makeAsyncExecutor, mockPi, tempDir } from "../support/async-execution-fixture.ts";

const replyValidator = Compile(Type.Object({ success: Type.Literal(true), data: Type.Object({ neverStarted: Type.Optional(Type.Boolean()), state: Type.String(), runId: Type.String(), operationId: Type.String(), digest: Type.String() }) }));

describe("native durable rejected admission", () => {
  installAsyncExecutionHooks();
  for (const rejection of ["unknown-agent", "cwd-file", "missing-cwd"] as const) it(`proves ${rejection} rejection before dispatch across cancellation, restart, and concurrent replay`, { timeout: 30_000 }, async t => {
    const root = path.join(tempDir, "native-journal");
    let capability = await probeRuntimeOwnership(path.join(root, "kernel-cache"));
    const deadline = Date.now() + 15_000;
    while (!capability.supported && capability.reason === "Kernel ownership preflight is still pending." && Date.now() < deadline) capability = await probeRuntimeOwnership(path.join(root, "kernel-cache"));
    assert.notEqual(capability.reason, "Kernel ownership preflight is still pending.");
    if (!capability.supported) { t.skip(capability.reason ?? "Kernel ownership unavailable"); return; }
    const events = createEventBus();
    const executor = makeAsyncExecutor([makeAgent("worker")]);
    let executions = 0;
    let ctx = makeMinimalCtx(tempDir);
    const options = { events, operationDirRoot: root, getContext: () => ctx, execute: (...args: Parameters<typeof executor.executePublic>) => { executions++; return executor.executePublic(...args); } };
    let rpc = registerSubagentRpcBridge(options);
    t.after(() => rpc.dispose());
    let sequence = 0;
    const cwd = rejection === "unknown-agent" ? tempDir : path.join(tempDir, rejection);
    if (rejection === "cwd-file") fs.writeFileSync(cwd, "not a directory");
    const params = { operationId: "rejected-operation", digest: "rejected-digest", cwd, agent: rejection === "unknown-agent" ? "not-installed" : "worker", task: "Do not dispatch", async: true, executionOwnership: { mode: "kernel" }, executionLifetime: { mode: "unbounded" } };
    async function request(method: SubagentRpcMethod) {
      const requestId = `admission-${++sequence}`;
      return new Promise<{ neverStarted?: boolean; state: string; runId: string; operationId: string; digest: string }>((resolve, reject) => {
        const dispose = events.on(subagentRpcReplyEvent(requestId), value => {
          dispose();
          if (!replyValidator.Check(value)) reject(new Error(JSON.stringify(value)));
          else resolve(value.data);
        });
        events.emit(SUBAGENT_RPC_REQUEST_EVENT, { version: 1, requestId, method, params });
      });
    }
    await Promise.all([request("spawn"), request("spawn")]);
    const rejected = await request("lookup");
    assert.equal(rejected.neverStarted, true);
    assert.equal(rejected.operationId, params.operationId);
    assert.equal(rejected.digest, params.digest);
    const operation = new DurableOperation(root, tempDir, params.operationId);
    assert.equal(fs.existsSync(path.join(operation.directory, "owned", "request.json")), false);
    rpc.dispose();
    ctx = makeMinimalCtx(path.join(tempDir, "different-session-cwd"));
    rpc = registerSubagentRpcBridge(options);
    const cancelled = await request("cancel");
    assert.equal(cancelled.neverStarted, true);
    assert.equal(cancelled.state, "cancelled");
    assert.equal((await request("spawn")).runId, rejected.runId);
    assert.equal((await request("lookup")).neverStarted, true);
    assert.equal(executions, 1);
    assert.equal(mockPi.callCount(), 0);
  });
});
