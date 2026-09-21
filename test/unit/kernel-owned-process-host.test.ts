import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import {
  cancelKernelOwnedProcess,
  launchKernelOwnedProcess,
  observeKernelOwnedProcess,
  reconcileKernelOwnedProcess,
  prepareKernelOwnedProcess,
  requestKernelOwnedProcessCancellation,
  type KernelOwnedProcessRequest,
} from "../../src/api/kernel-owned-process.mjs";
import {
  binding,
  loadRequest,
  publishJson,
} from "../../src/runs/background/kernel-owned-process-store.mjs";

const hostRoot = process.env.PI_KERNEL_HOST_TEST_DIR;
const execute = promisify(execFile);
const fixture = fileURLToPath(
  new URL("../fixtures/kernel-owned-process-worker.mjs", import.meta.url),
);
const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

test(
  "kernel operation survives escaped descendants, replay, and admission cancellation",
  {
    skip: process.platform !== "darwin" || !hostRoot,
    timeout: 60_000,
  },
  async (context) => {
    assert.ok(hostRoot);
    fs.mkdirSync(hostRoot, { recursive: true, mode: 0o700 });
    const suite = fs.mkdtempSync(path.join(hostRoot, "kernel-host-"));
    const operations: string[] = [];
    const environment = Object.fromEntries(
      Object.entries(process.env).filter(
        (entry): entry is [string, string] =>
          entry[1] !== undefined && entry[0] !== "PI_KERNEL_OWNED_OPERATION",
      ),
    );
    const request = (name: string, mode: string): KernelOwnedProcessRequest => {
      const operationDirectory = path.join(suite, name);
      operations.push(operationDirectory);
      return {
        operationDirectory,
        artifactDirectory: path.join(hostRoot, "native"),
        argv: [process.execPath, fixture, mode, path.join(suite, `${name}.result`)],
        cwd: process.cwd(),
        env: environment,
        lifetime: { kind: "unbounded" },
      };
    };
    async function retire(directory: string) {
      const deadline = Date.now() + 10_000;
      while (Date.now() < deadline) {
        const observation = await observeKernelOwnedProcess(directory);
        if (observation.status === "retired") return observation;
        assert.notEqual(observation.status, "unknown", observation.reason);
        await delay(25);
      }
      assert.fail("kernel retirement not observed");
    }
    async function awaitEscapedChild(name: string, directory: string) {
      const deadline = Date.now() + 3000;
      while (!fs.existsSync(path.join(suite, `${name}.result`)) && Date.now() < deadline) {
        await delay(10);
      }
      assert.equal(fs.existsSync(path.join(suite, `${name}.result`)), true);
      assert.equal((await observeKernelOwnedProcess(directory)).status, "active");
    }
    try {
      await context.test("natural double-fork work finishes before retirement", async () => {
        const input = request("natural", "natural");
        const handle = await launchKernelOwnedProcess(input);
        assert.ok(handle.identity);
        const retired = await retire(input.operationDirectory);
        assert.equal(retired.exitCode, 0);
        assert.equal(retired.timedOut, false);
        assert.equal(
          fs.readFileSync(path.join(suite, "natural.result"), "utf8"),
          "detached-completed",
        );
        assert.equal(retired.proof?.kind, "darwin-coalition-retired");
        assert.equal(retired.proof?.requestDigest, handle.requestDigest);
      });
      await context.test(
        "same-label replay after interrupted bootstrap intent executes once",
        async () => {
          const input = request("replay", "counter");
          await prepareKernelOwnedProcess(input);
          const envelope = loadRequest(input.operationDirectory);
          publishJson(path.join(input.operationDirectory, "bootstrap.json"), {
            ...binding(envelope),
            requestedAt: new Date().toISOString(),
          });
          await Promise.all([launchKernelOwnedProcess(input), launchKernelOwnedProcess(input)]);
          await retire(input.operationDirectory);
          await launchKernelOwnedProcess(input);
          assert.equal(fs.readFileSync(path.join(suite, "replay.result"), "utf8"), "executed\n");
        },
      );
      await context.test("pre-admission cancellation prevents late launch", async () => {
        const input = request("cancel-first", "counter");
        await requestKernelOwnedProcessCancellation(input.operationDirectory);
        const prepared = await prepareKernelOwnedProcess(input);
        const observation = await cancelKernelOwnedProcess(input.operationDirectory);
        const replay = await launchKernelOwnedProcess(input);
        assert.equal(observation.status, "never-started");
        assert.equal(observation.proof?.requestDigest, prepared.requestDigest);
        assert.equal(replay.observation.status, "never-started");
        assert.equal((await reconcileKernelOwnedProcess(input.operationDirectory)).status, "never-started");
        assert.equal(fs.existsSync(path.join(suite, "cancel-first.result")), false);
      });
      await context.test("a recovered controller reconciles one prepared launch without a new identity", async () => {
        const input = request("prepared-reconcile", "counter");
        const prepared = await prepareKernelOwnedProcess(input);
        assert.equal((await observeKernelOwnedProcess(input.operationDirectory)).status, "pending");
        await Promise.all([reconcileKernelOwnedProcess(input.operationDirectory), reconcileKernelOwnedProcess(input.operationDirectory)]);
        const retired = await retire(input.operationDirectory);
        assert.equal(retired.proof?.requestDigest, prepared.requestDigest);
        assert.equal((await reconcileKernelOwnedProcess(input.operationDirectory)).status, "retired");
        assert.equal(fs.readFileSync(path.join(suite, "prepared-reconcile.result"), "utf8"), "executed\n");
      });
      await context.test("cancellation races admission without duplicate execution", async () => {
        const input = request("cancel-race", "counter");
        await prepareKernelOwnedProcess(input);
        await Promise.all([
          launchKernelOwnedProcess(input),
          cancelKernelOwnedProcess(input.operationDirectory),
        ]);
        const final = await cancelKernelOwnedProcess(input.operationDirectory);
        assert.ok(final.status === "never-started" || final.status === "retired");
        await launchKernelOwnedProcess(input);
        const output = path.join(suite, "cancel-race.result");
        if (fs.existsSync(output)) assert.equal(fs.readFileSync(output, "utf8"), "executed\n");
      });
      await context.test("durable cancellation continues after gate death", async () => {
        const input = request("gate-death", "linger-root");
        const handle = await launchKernelOwnedProcess(input);
        assert.ok(handle.identity);
        await awaitEscapedChild("gate-death", input.operationDirectory);
        const envelope = loadRequest(input.operationDirectory);
        const leader = handle.identity.leader;
        await execute(
          envelope.request.nativeExecutable,
          [
            "signal",
            String(leader.pid),
            String(leader.pidVersion),
            leader.uniqueId,
            handle.identity.coalitionId,
            "9",
          ],
          { timeout: 2000 },
        );
        await requestKernelOwnedProcessCancellation(input.operationDirectory);
        const retired = await retire(input.operationDirectory);
        assert.equal(retired.proof?.kind, "darwin-coalition-retired");
      });
      await context.test("bounded expiry is recovered after gate death", async () => {
        const input = request("bounded", "linger-root");
        input.lifetime = { kind: "bounded", timeoutMs: 2000 };
        const handle = await launchKernelOwnedProcess(input);
        assert.ok(handle.identity);
        await awaitEscapedChild("bounded", input.operationDirectory);
        const envelope = loadRequest(input.operationDirectory);
        const leader = handle.identity.leader;
        await execute(
          envelope.request.nativeExecutable,
          [
            "signal",
            String(leader.pid),
            String(leader.pidVersion),
            leader.uniqueId,
            handle.identity.coalitionId,
            "9",
          ],
          { timeout: 2000 },
        );
        const retired = await retire(input.operationDirectory);
        assert.equal(retired.timedOut, true);
      });
      await context.test("inherited membership is checked and nested launch rejected", async () => {
        const input = request("membership", "membership");
        await launchKernelOwnedProcess(input);
        const retired = await retire(input.operationDirectory);
        assert.equal(retired.exitCode, 0);
        const output = fs.readFileSync(path.join(suite, "membership.result"), "utf8");
        assert.match(output, /"owned":true/);
        assert.match(output, /"nestedRejected":true/);
      });
    } finally {
      for (const directory of operations) {
        const result = await cancelKernelOwnedProcess(directory);
        assert.ok(
          result.status === "never-started" || result.status === "retired",
          `${directory}: ${result.status}`,
        );
      }
    }
  },
);
