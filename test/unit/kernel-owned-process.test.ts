import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { once } from "node:events";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test, { type TestContext } from "node:test";
import { fileURLToPath } from "node:url";
import { Type } from "typebox";
import { Compile } from "typebox/compile";
import { observeNativeKernelRun, writeNativeKernelMapping } from "../../src/runs/background/runtime-ownership.ts";
import { DurableOperation } from "../../src/runs/background/durable-operation.ts";
import { readStatus } from "../../src/shared/utils.ts";
import { registerSubagentRpcBridge, SUBAGENT_RPC_REQUEST_EVENT, subagentRpcReplyEvent } from "../../src/extension/rpc.ts";
import { createEventBus, makeMinimalCtx } from "../support/helpers.ts";
import {
  cancelKernelOwnedProcess,
  observeKernelOwnedProcess,
  prepareKernelOwnedProcess,
  requestKernelOwnedProcessCancellation,
} from "../../src/runs/background/kernel-owned-process.mjs";
import {
  BACKEND,
  binding,
  canonical,
  digest,
  loadDecision,
  loadRequest,
  publishJson,
  publishRecord,
  readJson,
} from "../../src/runs/background/kernel-owned-process-store.mjs";

const fixturePath = fileURLToPath(
  new URL("../fixtures/kernel-owned-process-boundary.mjs", import.meta.url),
);
const storeUrl = new URL(
  "../../src/runs/background/kernel-owned-process-store.mjs",
  import.meta.url,
).href;
const callValidator = Compile(
  Type.Object({ command: Type.String(), args: Type.Array(Type.String()) }),
);
type Lifetime = { kind: "unbounded" } | { kind: "bounded"; timeoutMs: number };
interface AdmissionDeadline {
  deadlineMonotonicNs?: string;
}
interface BoundaryState {
  hostId: string;
  bootId: string;
  monotonicNs: string;
  coalition:
    | { ok: true; started: string; exited: string }
    | { ok: false; errno: number; error: string };
  members: { pid: number; uniqueId: string; pidVersion: number }[];
  delayCommand?: string;
  delayMs?: number;
  retirementRecords?: ("exit" | "timeout")[];
}

function fixture(t: TestContext, lifetime: Lifetime = { kind: "unbounded" }, preparedDirectory?: string) {
  const directory = preparedDirectory ?? fs.mkdtempSync(path.join(os.tmpdir(), "kernel-owned-boundary-"));
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  fs.chmodSync(directory, 0o700);
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const nativeExecutable = path.join(directory, "native.mjs");
  const native = fs
    .readFileSync(fixturePath, "utf8")
    .replace("#!/usr/bin/env node", `#!${process.execPath}`);
  fs.writeFileSync(nativeExecutable, native, { mode: 0o700 });
  const gate = "boundary gate asset\n";
  const store = "boundary store asset\n";
  fs.writeFileSync(path.join(directory, "kernel-owned-process-helper.mjs"), gate, { mode: 0o600 });
  fs.writeFileSync(path.join(directory, "kernel-owned-process-store.mjs"), store, { mode: 0o600 });
  const operationId = randomUUID();
  const command = { argv: ["/bin/true"], cwd: directory, env: {}, lifetime };
  const request = {
    operationId,
    operationDirectory: directory,
    command,
    hostId: "test-host",
    bootId: "test-boot",
    originCoalitionId: "4000",
    nativeExecutable,
    nativeDigest: digest(native),
    gateDigest: digest(gate),
    storeDigest: digest(store),
    nodeExecutable: process.execPath,
    label: `com.pi-subagents.owned.${operationId}`,
    domain: `gui/${process.getuid?.() ?? 0}`,
  };
  const envelope = { version: 1, digest: digest(canonical(request)), request };
  publishJson(path.join(directory, "request.json"), envelope);
  const state: BoundaryState = {
    hostId: request.hostId,
    bootId: request.bootId,
    monotonicNs: "500",
    coalition: { ok: true, started: "2", exited: "1" },
    members: [{ pid: 987654, uniqueId: "8000", pidVersion: 3 }],
  };
  function save() {
    fs.writeFileSync(path.join(directory, "boundary-state.json"), JSON.stringify(state), {
      mode: 0o600,
    });
  }
  save();
  const deadline: AdmissionDeadline = {};
  if (lifetime.kind === "bounded") deadline.deadlineMonotonicNs = "1000";
  const admission = {
    kind: "admitted",
    ...binding(envelope),
    observedAt: new Date().toISOString(),
    identity: {
      version: 1,
      backend: BACKEND,
      ...binding(envelope),
      coalitionId: "4242",
      leader: { pid: 876543, uniqueId: "7000", pidVersion: 1 },
    },
    registration: { started: "2", exited: "0" },
    ...deadline,
  };
  function admit() {
    assert.equal(publishRecord(path.join(directory, "decision.json"), admission), true);
  }
  function calls() {
    const file = path.join(directory, "boundary-calls.jsonl");
    if (!fs.existsSync(file)) return [];
    return fs
      .readFileSync(file, "utf8")
      .trim()
      .split("\n")
      .map((line) => {
        const value: unknown = JSON.parse(line);
        assert.ok(callValidator.Check(value));
        return value;
      });
  }
  return { directory, command, envelope, state, admission, save, admit, calls };
}

const terminalReplyValidator = Compile(Type.Object({ success: Type.Literal(true), data: Type.Object({ status: Type.String(), terminationReason: Type.Optional(Type.String()), processTerminalProof: Type.Object({ state: Type.String() }), statusPayload: Type.Optional(Type.Object({ state: Type.String(), terminationReason: Type.Optional(Type.String()) })) }) }));

for (const outcome of ["bounded", "unfinished", "native-stop", "outer-stop", "complete", "unknown", "missing-status", "missing-status-stop"] as const) {
  test(`native reconciliation settles verified retirement without an exit receipt (${outcome})`, async t => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "native-retired-receipt-"));
    t.after(() => fs.rmSync(root, { recursive: true, force: true }));
    const durable = new DurableOperation(root, root, "native-operation");
    const bounded = outcome === "bounded" || outcome === "native-stop" || outcome === "outer-stop" || outcome === "missing-status-stop";
    const missingStatus = outcome === "missing-status" || outcome === "missing-status-stop";
    durable.claim({ digest: "native-digest", requestHash: "request-hash", effectiveExecutionOwnership: { mode: "kernel" }, effectiveExecutionLifetime: bounded ? { mode: "bounded", timeoutMs: 1 } : { mode: "unbounded" } });
    const operation = fixture(t, bounded ? { kind: "bounded", timeoutMs: 1 } : { kind: "unbounded" }, path.join(durable.directory, "owned"));
    operation.admit();
    operation.state.coalition = { ok: false, errno: outcome === "unknown" ? 1 : 3, error: "retired" };
    operation.save();
    if (bounded) publishRecord(path.join(operation.directory, "timeout.json"), { ...binding(operation.envelope), observedAt: new Date().toISOString() });
    const asyncDir = path.join(root, durable.runId);
    fs.mkdirSync(asyncDir);
    const completed = { agent: "completed", status: "complete", endedAt: 1, output: "preserved completed output" };
    if (!missingStatus) fs.writeFileSync(path.join(asyncDir, "status.json"), JSON.stringify({ runId: durable.runId, mode: "single", state: outcome === "complete" ? "complete" : outcome === "unfinished" ? "queued" : "running", startedAt: 1, steps: [completed, { agent: "unfinished", status: outcome === "complete" ? "complete" : "running" }] }));
    writeNativeKernelMapping(operation.directory, { version: 1, runId: durable.runId, runnerProcessInstanceId: "runner-instance", asyncDir, kernelBinding: binding(operation.envelope) });
    if (outcome === "native-stop" || outcome === "missing-status-stop") fs.writeFileSync(path.join(operation.directory, "native-stop.json"), JSON.stringify({ runId: durable.runId, requestedAt: Date.now() }));
    if (outcome === "outer-stop") durable.cancel("native-digest");
    assert.equal(fs.existsSync(path.join(operation.directory, "exit.json")), false);
    const result = await observeNativeKernelRun(operation.directory, durable.runId);
    assert.equal(result.processTerminalProof.state, outcome === "unknown" ? "unknown" : "observed");
    assert.equal(result.runnerFailed, outcome !== "complete" && outcome !== "unknown");
    const stopped = outcome === "native-stop" || outcome === "outer-stop" || outcome === "missing-status-stop";
    const expectedState = stopped ? "stopped" : outcome === "complete" ? "complete" : outcome === "unknown" ? "running" : "failed";
    const status = readStatus(asyncDir);
    if (!missingStatus) {
      assert.equal(status?.state, expectedState);
      assert.deepEqual(status?.steps?.[0], completed);
      assert.equal(status?.steps?.[1]?.status, expectedState);
      assert.equal(status?.terminationReason, outcome === "bounded" ? "execution_lifetime_expired" : undefined);
      if (bounded) assert.equal(status?.timedOut, true);
    }
    const events = createEventBus();
    const rpc = registerSubagentRpcBridge({ events, operationDirRoot: root, asyncDirRoot: root, resultsDir: path.join(root, "results"), getContext: () => makeMinimalCtx(root), execute: async () => assert.fail("Observation cannot dispatch a replacement") });
    t.after(() => rpc.dispose());
    const reply = new Promise<ReturnType<typeof terminalReplyValidator.Parse>>((resolve, reject) => {
      events.on(subagentRpcReplyEvent("terminal-lookup"), value => {
        if (!terminalReplyValidator.Check(value)) reject(new Error(JSON.stringify(value)));
        else resolve(value);
      });
    });
    events.emit(SUBAGENT_RPC_REQUEST_EVENT, { version: 1, requestId: "terminal-lookup", method: "lookup", params: { operationId: "native-operation", digest: "native-digest" } });
    const data = (await reply).data;
    assert.equal(data.status, expectedState);
    assert.equal(data.terminationReason, outcome === "bounded" ? "execution_lifetime_expired" : undefined);
    if (!missingStatus) assert.equal(data.statusPayload?.state, expectedState);
  });
}

test("cancellation creates a durable fence before an operation directory exists", async (t) => {
  const operation = fixture(t);
  const missing = path.join(operation.directory, "not-yet-prepared");
  await requestKernelOwnedProcessCancellation(missing);
  const first = readJson(path.join(missing, "cancel-request.json"));
  await requestKernelOwnedProcessCancellation(missing);
  assert.deepEqual(readJson(path.join(missing, "cancel-request.json")), first);
  assert.equal(first.version, 1);
  assert.equal(fs.statSync(missing).mode & 0o777, 0o700);
  assert.equal(fs.statSync(path.join(missing, "cancel-request.json")).mode & 0o777, 0o600);
});

test("copied request cannot open another admission slot in a different operation directory", async (t) => {
  const operation = fixture(t);
  const relocated = path.join(operation.directory, "relocated");
  fs.mkdirSync(relocated, { mode: 0o700 });
  for (const file of [
    "request.json",
    "kernel-owned-process-helper.mjs",
    "kernel-owned-process-store.mjs",
    "native.mjs",
    "boundary-state.json",
  ]) {
    fs.copyFileSync(path.join(operation.directory, file), path.join(relocated, file));
  }
  assert.equal(fs.existsSync(path.join(operation.directory, "decision.json")), false);
  assert.equal(fs.existsSync(path.join(relocated, "decision.json")), false);
  assert.throws(() => loadRequest(relocated), /request-directory-mismatch/);
  const observation = await observeKernelOwnedProcess(relocated);
  assert.equal(observation.status, "unknown");
  assert.equal(observation.proof, undefined);
  await assert.rejects(
    prepareKernelOwnedProcess({ operationDirectory: relocated, ...operation.command }),
    /request-directory-mismatch/,
  );
  assert.equal(fs.existsSync(path.join(relocated, "decision.json")), false);
  assert.deepEqual(operation.calls(), []);
});

test("never-started admission winner excludes a paused competing admission", async (t) => {
  const operation = fixture(t);
  publishJson(path.join(operation.directory, "candidate.json"), operation.admission);
  const worker = spawn(
    process.execPath,
    [fixturePath, "admission-race", operation.directory, storeUrl],
    { stdio: ["ignore", "pipe", "pipe"] },
  );
  const closed = once(worker, "close");
  let output = "";
  worker.stdout.on("data", (chunk) => {
    output += String(chunk);
  });
  await once(worker.stdout, "data");
  try {
    const result = await cancelKernelOwnedProcess(operation.directory, { deadlineMs: 1000 });
    assert.equal(result.status, "never-started");
    fs.writeFileSync(path.join(operation.directory, "release"), "", { mode: 0o600 });
    const [code] = await closed;
    assert.equal(code, 0);
    assert.match(output, /"won":false/);
    const decision = loadDecision(operation.directory, loadRequest(operation.directory));
    assert.equal(decision.kind, "never-started");
    await prepareKernelOwnedProcess({
      operationDirectory: operation.directory,
      ...operation.command,
    });
    assert.equal((await observeKernelOwnedProcess(operation.directory)).status, "never-started");
    assert.equal(
      operation.calls().some((call) => call.command === "signal"),
      false,
    );
  } finally {
    fs.writeFileSync(path.join(operation.directory, "release"), "", { mode: 0o600 });
    await closed;
  }
});

test("admitted operation cannot be replaced by a later never-started decision", async (t) => {
  const operation = fixture(t);
  operation.admit();
  await requestKernelOwnedProcessCancellation(operation.directory);
  await prepareKernelOwnedProcess({
    operationDirectory: operation.directory,
    ...operation.command,
  });
  assert.equal(loadDecision(operation.directory, operation.envelope).kind, "admitted");
  const result = await observeKernelOwnedProcess(operation.directory);
  assert.equal(result.status, "active");
  assert.ok(operation.calls().some((call) => call.command === "signal"));
});

for (const corruption of ["request", "binding", "decision-checksum"] as const) {
  test(`invalid ${corruption} cannot yield terminal proof`, async (t) => {
    const operation = fixture(t);
    if (corruption === "binding") {
      publishRecord(path.join(operation.directory, "decision.json"), {
        ...operation.admission,
        bootId: "different-boot",
      });
    } else {
      operation.admit();
      const file = path.join(
        operation.directory,
        corruption === "request" ? "request.json" : "decision.json",
      );
      const value = readJson(file);
      if (corruption === "request") value.request.hostId = "changed-host";
      else value.identity.coalitionId = "4243";
      fs.writeFileSync(file, JSON.stringify(value));
    }
    operation.state.coalition = { ok: false, errno: 3, error: "retired" };
    operation.save();
    const result = await observeKernelOwnedProcess(operation.directory);
    assert.equal(result.status, "unknown");
    assert.equal(result.proof, undefined);
    assert.equal(
      operation.calls().some((call) => call.command === "coalition"),
      false,
    );
  });
}

test("zero coalition counters remain active", async (t) => {
  const operation = fixture(t);
  operation.admit();
  operation.state.coalition = { ok: true, started: "0", exited: "0" };
  operation.save();
  const result = await observeKernelOwnedProcess(operation.directory);
  assert.equal(result.status, "active");
  assert.equal(result.proof, undefined);
});

for (const observation of [
  "retired",
  "host-mismatch",
  "boot-mismatch",
  "permission-denied",
] as const) {
  test(`coalition ${observation} preserves the terminal evidence boundary`, async (t) => {
    const operation = fixture(t);
    operation.admit();
    operation.state.coalition = {
      ok: false,
      errno: observation === "permission-denied" ? 1 : 3,
      error: observation,
    };
    if (observation === "host-mismatch") operation.state.hostId = "other-host";
    if (observation === "boot-mismatch") operation.state.bootId = "other-boot";
    operation.save();
    const result = await observeKernelOwnedProcess(operation.directory);
    assert.equal(result.status, observation === "retired" ? "retired" : "unknown");
    if (observation === "retired") assert.equal(result.proof.kind, "darwin-coalition-retired");
    else assert.equal(result.proof, undefined);
  });
}

test("cancellation control deadline bounds a delayed native service", async (t) => {
  const operation = fixture(t);
  operation.admit();
  operation.state.delayCommand = "host";
  operation.state.delayMs = 2500;
  operation.save();
  const started = performance.now();
  const result = await cancelKernelOwnedProcess(operation.directory, { deadlineMs: 100 });
  assert.ok(performance.now() - started < 900);
  assert.equal(result.status, "unknown");
  assert.equal(result.proof, undefined);
  assert.equal(readJson(path.join(operation.directory, "cancel-request.json")).version, 1);
});

test("observer recovers expired bounded execution after helper death and sweeps members", async (t) => {
  const operation = fixture(t, { kind: "bounded", timeoutMs: 1 });
  operation.admit();
  assert.equal((await observeKernelOwnedProcess(operation.directory)).timedOut, undefined);
  assert.equal(fs.existsSync(path.join(operation.directory, "timeout.json")), false);
  operation.state.monotonicNs = "1000";
  operation.save();
  const result = await observeKernelOwnedProcess(operation.directory);
  assert.equal(result.status, "active");
  assert.equal(result.timedOut, true);
  assert.ok(fs.existsSync(path.join(operation.directory, "timeout.json")));
  assert.deepEqual(
    operation
      .calls()
      .filter((call) => call.command === "signal")
      .map((call) => call.args),
    [["987654", "3", "8000", "4242", "9"]],
  );
});

test("retirement before lifetime expiry does not invent timeout on later observation", async (t) => {
  const operation = fixture(t, { kind: "bounded", timeoutMs: 1 });
  operation.admit();
  operation.state.coalition = { ok: false, errno: 3, error: "retired" };
  operation.save();
  const first = await observeKernelOwnedProcess(operation.directory);
  assert.equal(first.status, "retired");
  assert.equal(first.timedOut, undefined);
  operation.state.monotonicNs = "2000";
  operation.save();
  const later = await observeKernelOwnedProcess(operation.directory);
  assert.equal(later.status, "retired");
  assert.equal(later.timedOut, undefined);
  assert.equal(fs.existsSync(path.join(operation.directory, "timeout.json")), false);
  assert.equal(
    operation.calls().some((call) => call.command === "signal"),
    false,
  );
});

for (const outcome of ["success", "failure", "timeout", "corrupt-exit", "corrupt-timeout", "missing"] as const) {
  test(`retirement refreshes final workload metadata published during the kernel query (${outcome})`, async (t) => {
    const operation = fixture(t);
    operation.admit();
    operation.state.coalition = { ok: false, errno: 3, error: "retired" };
    operation.state.retirementRecords = [];
    if (outcome !== "missing") {
      publishRecord(path.join(operation.directory, "pending-exit.json"), {
        ...binding(operation.envelope), exitCode: outcome === "failure" ? 7 : 0,
        signal: null, timedOut: false, observedAt: new Date().toISOString(),
      });
      operation.state.retirementRecords.push("exit");
    }
    if (outcome === "timeout" || outcome === "corrupt-timeout") {
      publishRecord(path.join(operation.directory, "pending-timeout.json"), {
        ...binding(operation.envelope), observedAt: new Date().toISOString(),
      });
      operation.state.retirementRecords.push("timeout");
    }
    if (outcome === "corrupt-exit" || outcome === "corrupt-timeout") {
      fs.writeFileSync(path.join(operation.directory, `pending-${outcome === "corrupt-exit" ? "exit" : "timeout"}.json`), "{}");
    }
    operation.save();
    const observation = await observeKernelOwnedProcess(operation.directory);
    if (outcome === "corrupt-exit" || outcome === "corrupt-timeout") {
      assert.equal(observation.status, "unknown");
      assert.equal(observation.proof, undefined);
    } else {
      assert.equal(observation.status, "retired");
      assert.equal(observation.proof?.kind, "darwin-coalition-retired");
      assert.equal(observation.exitCode, outcome === "missing" ? undefined : outcome === "failure" ? 7 : 0);
      assert.equal(observation.timedOut, outcome === "missing" ? undefined : outcome === "timeout");
    }
  });
}
