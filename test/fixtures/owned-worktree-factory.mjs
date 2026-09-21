import fs from "node:fs";
import path from "node:path";
import { execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { createFakeChildSessions } from "../support/fake-child-session.ts";
import { inspectInheritedKernelOwnedProcessMembership } from "../../src/api/kernel-owned-process.mjs";
import { createSubagentExecutor } from "../../src/runs/foreground/subagent-executor.ts";
import { setChildSessionFactoryModule } from "../../src/runs/shared/child-session.ts";
import { RESULTS_DIR } from "../../src/shared/types.ts";
import { createEventBus, makeAgent, makeMinimalCtx } from "../support/helpers.ts";

export default function () {
  const fake = createFakeChildSessions(() => process.env.MOCK_PI_QUEUE_DIR).factory;
  return {
    async create(launch) {
      const spec = JSON.parse(fs.readFileSync(process.env.PI_OWNED_WORKTREE_SPEC, "utf8"));
      if (launch.runtime.agent === "coordinator") {
        setChildSessionFactoryModule(fileURLToPath(import.meta.url));
        const executor = createSubagentExecutor({ pi: { events: createEventBus(), getSessionName: () => undefined }, state: { baseCwd: launch.cwd, currentSessionId: null, asyncJobs: new Map(), foregroundControls: new Map(), lastForegroundControlId: null }, config: spec.config, asyncByDefault: false, tempArtifactsDir: spec.directory, getSubagentSessionRoot: () => spec.directory, expandTilde: value => value, discoverAgents: () => ({ agents: [makeAgent("first"), makeAgent("second")] }) });
        const nested = await executor.executePublic("nested-owned-worktrees", spec.parallelParams, new AbortController().signal, undefined, makeMinimalCtx(launch.cwd));
        if (nested.isError || !nested.details.asyncId) throw new Error(JSON.stringify(nested));
        const resultPath = path.join(RESULTS_DIR, `${nested.details.asyncId}.json`);
        const deadline = Date.now() + 20_000;
        while (!fs.existsSync(resultPath) && Date.now() < deadline) await new Promise(resolve => setTimeout(resolve, 25));
        fs.writeFileSync(path.join(spec.directory, "nested-result.json"), fs.readFileSync(resultPath));
        fs.writeFileSync(path.join(spec.directory, "nested-status.json"), fs.readFileSync(path.join(nested.details.asyncDir, "status.json")));
      } else {
        const membership = await inspectInheritedKernelOwnedProcessMembership(process.env.PI_KERNEL_OWNED_OPERATION);
        if (!membership.owned) throw new Error("Worktree child escaped its owned root");
        const git = args => execFileSync("git", args, { cwd: launch.cwd, encoding: "utf8" }).trim();
        const observation = { agent: launch.runtime.agent, cwd: launch.cwd, root: git(["rev-parse", "--show-toplevel"]), head: git(["rev-parse", "HEAD"]), branch: git(["branch", "--show-current"]), before: fs.readFileSync(path.join(launch.cwd, "shared.txt"), "utf8"), hook: fs.existsSync(path.join(launch.cwd, "hook-marker")), membership, lifetime: launch.runtime.executionLifetime };
        fs.writeFileSync(path.join(launch.cwd, "shared.txt"), `${launch.runtime.agent}\n`);
        fs.appendFileSync(path.join(spec.directory, "children.jsonl"), `${JSON.stringify({ ...observation, written: fs.readFileSync(path.join(launch.cwd, "shared.txt"), "utf8") })}\n`);
      }
      return fake.create(launch);
    },
    dispose: () => fake.dispose(),
  };
}
