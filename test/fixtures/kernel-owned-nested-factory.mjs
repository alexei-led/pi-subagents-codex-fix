import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { createSubagentExecutor } from "../../src/runs/foreground/subagent-executor.ts";
import { inspectInheritedKernelOwnedProcessMembership } from "../../src/api/kernel-owned-process.mjs";
import { setChildSessionFactory, setChildSessionFactoryModule } from "../../src/runs/shared/child-session.ts";
import { fileURLToPath } from "node:url";
import { createFakeChildSessions } from "../support/fake-child-session.ts";
import { createEventBus, makeAgent, makeMinimalCtx } from "../support/helpers.ts";

export default function () {
  const fake = createFakeChildSessions(() => process.env.MOCK_PI_QUEUE_DIR).factory;
  return {
    async create(launch) {
      if (!process.env.PI_KERNEL_NESTED_TEST_STARTED) {
        process.env.PI_KERNEL_NESTED_TEST_STARTED = "1";
        setChildSessionFactory(fake);
        setChildSessionFactoryModule(fileURLToPath(import.meta.url));
        const marker = process.env.PI_KERNEL_OWNED_OPERATION;
        const descriptor = JSON.parse(marker);
        const membership = await inspectInheritedKernelOwnedProcessMembership(marker);
        assert.equal(membership.owned, true);
        for (const field of ["operationId", "requestDigest", "hostId", "bootId"])
          assert.equal((await inspectInheritedKernelOwnedProcessMembership(JSON.stringify({ ...descriptor, [field]: "foreign" }))).owned, false);
        const executor = createSubagentExecutor({
          pi: { events: createEventBus(), getSessionName: () => undefined },
          state: { baseCwd: launch.cwd, currentSessionId: null, asyncJobs: new Map(), foregroundControls: new Map(), lastForegroundControlId: null },
          config: {}, asyncByDefault: false, tempArtifactsDir: launch.cwd,
          getSubagentSessionRoot: () => launch.cwd, expandTilde: value => value,
          discoverAgents: () => ({ agents: [makeAgent("nested")] }),
        });
        const ctx = makeMinimalCtx(launch.cwd);
        const params = { agent: "nested", task: "Nested foreground", async: false, context: "fresh", output: false, acceptance: false, mission: false, skill: false, executionLifetime: { mode: "unbounded" } };
        process.env.PI_KERNEL_OWNED_OPERATION = JSON.stringify({ ...descriptor, bootId: "foreign" });
        const rejected = await executor.execute("foreign-marker", params, new AbortController().signal, undefined, ctx);
        assert.equal(rejected.isError, true);
        process.env.PI_KERNEL_OWNED_OPERATION = marker;
        const foreground = await executor.execute("nested-foreground", params, new AbortController().signal, undefined, ctx);
        assert.notEqual(foreground.isError, true, JSON.stringify(foreground));
        const foregroundRevival = await executor.executePublic("revive-owned-foreground", { action: "resume", id: foreground.details.runId, message: "Continue" }, new AbortController().signal, undefined, ctx);
        assert.equal(foregroundRevival.isError, true);
        const background = await executor.execute("nested-background", { ...params, task: "Nested background", async: true }, new AbortController().signal, undefined, ctx);
        assert.notEqual(background.isError, true, JSON.stringify(background));
        const statusPath = path.join(background.details.asyncDir, "status.json");
        const deadline = Date.now() + 15_000;
        let status;
        do {
          status = JSON.parse(fs.readFileSync(statusPath, "utf8"));
          if (status.state === "complete" || status.state === "failed" || status.state === "stopped") break;
          await new Promise(resolve => setTimeout(resolve, 25));
        } while (Date.now() < deadline);
        assert.equal(status.state, "complete", JSON.stringify(status));
        fs.writeFileSync(process.env.PI_KERNEL_NESTED_TEST_RESULT, JSON.stringify({ descriptor, membership, foreground, background, status }));
      }
      return fake.create(launch);
    },
    dispose: () => fake.dispose(),
  };
}
