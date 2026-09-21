import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import * as fs from "node:fs";
import * as path from "node:path";
import { describe, it } from "node:test";
import { preflightKernelOwnedProcess, cancelKernelOwnedProcess, observeKernelOwnedProcess } from "../../src/api/kernel-owned-process.mjs";
import { ownedGitEnvironment } from "../../src/runs/shared/owned-git-environment.ts";
import { setChildSessionFactoryModule } from "../../src/runs/shared/child-session.ts";
import { installAsyncExecutionHooks, makeAsyncExecutor, mockPi, tempDir, waitForAsyncResultFile } from "../support/async-execution-fixture.ts";
import { makeAgent, makeMinimalCtx } from "../support/helpers.ts";

describe("owned worker Git environment isolation", () => {
	installAsyncExecutionHooks();
	it("executes Git in the candidate despite parent selectors targeting a canary checkout", { timeout: 45_000 }, async (t) => {
		const capability = await preflightKernelOwnedProcess({ artifactDirectory: path.join(tempDir, "kernel-cache") });
		if (!capability.supported) { t.skip(capability.reason ?? "Kernel ownership unavailable"); return; }
		const candidate = path.join(tempDir, "candidate");
		const canary = path.join(tempDir, "canary");
		const cleanEnv = ownedGitEnvironment(process.env);
		for (const directory of [candidate, canary]) {
			fs.mkdirSync(directory);
			execFileSync("git", ["init", "--quiet"], { cwd: directory, env: cleanEnv });
			execFileSync("git", ["-c", "core.hooksPath=/dev/null", "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "--quiet", "--allow-empty", "-m", "fixture"], { cwd: directory, env: cleanEnv });
		}
		const canaryHead = execFileSync("git", ["rev-parse", "HEAD"], { cwd: canary, env: cleanEnv, encoding: "utf8" });
		const canaryIndex = fs.readFileSync(path.join(canary, ".git", "index"));
		const marker = path.join(tempDir, "git-observation.json");
		const factory = path.join(tempDir, "git-factory.mjs");
		fs.writeFileSync(factory, `import fs from "node:fs"; import {execFileSync} from "node:child_process";
import {createFakeChildSessions} from ${JSON.stringify(new URL("../support/fake-child-session.ts", import.meta.url).href)};
export default function(){const fake=createFakeChildSessions(()=>process.env.MOCK_PI_QUEUE_DIR).factory;return {create:async launch=>{fs.writeFileSync(${JSON.stringify(marker)},JSON.stringify({root:execFileSync("git",["rev-parse","--show-toplevel"],{cwd:launch.cwd,encoding:"utf8"}).trim(),cwd:process.cwd(),author:process.env.GIT_AUTHOR_NAME,auth:process.env.SSH_AUTH_SOCK,gitDir:process.env.GIT_DIR,configCount:process.env.GIT_CONFIG_COUNT}));return fake.create(launch)},dispose:()=>fake.dispose()}}`);
		setChildSessionFactoryModule(factory);
		const poisoned = { GIT_DIR: path.join(canary, ".git"), GIT_WORK_TREE: canary, GIT_COMMON_DIR: path.join(canary, ".git"), GIT_INDEX_FILE: path.join(canary, ".git", "index"), GIT_NAMESPACE: "canary", GIT_CONFIG_COUNT: "1", GIT_CONFIG_KEY_0: "core.worktree", GIT_CONFIG_VALUE_0: canary, GIT_AUTHOR_NAME: "preserved-author", SSH_AUTH_SOCK: "/tmp/test-auth-socket" };
		const previous = new Map(Object.keys(poisoned).map((key) => [key, process.env[key]]));
		Object.assign(process.env, poisoned);
		const operationDirectory = path.join(tempDir, "owned-operation");
		t.after(async () => { await cancelKernelOwnedProcess(operationDirectory, { deadlineMs: 5000 }); });
		try {
			mockPi.onCall({ output: "Git environment verified" });
			const executor = makeAsyncExecutor([makeAgent("worker")]);
			const launched = await executor.execute("owned-git", { agent: "worker", task: "Check Git routing", cwd: candidate, async: true, output: false, acceptance: false, mission: false, context: "fresh", executionLifetime: { mode: "unbounded" }, executionOwnership: { mode: "kernel" }, rpcKernelOperationDirectory: operationDirectory }, new AbortController().signal, undefined, makeMinimalCtx(tempDir));
			assert.notEqual(launched.isError, true, JSON.stringify(launched));
			assert.ok(launched.details.asyncId);
			await waitForAsyncResultFile(launched.details.asyncId, 15_000);
			for (let attempt = 0; attempt < 100; attempt++) {
				if ((await observeKernelOwnedProcess(operationDirectory)).status === "retired") break;
				await new Promise((resolve) => setTimeout(resolve, 100));
			}
			assert.equal((await observeKernelOwnedProcess(operationDirectory)).status, "retired");
			const observed = JSON.parse(fs.readFileSync(marker, "utf8"));
			assert.equal(observed.root, candidate);
			assert.equal(observed.cwd, candidate);
			assert.equal(observed.author, "preserved-author");
			assert.equal(observed.auth, "/tmp/test-auth-socket");
			assert.equal(observed.gitDir, undefined);
			assert.equal(observed.configCount, undefined);
			assert.equal(process.env.GIT_DIR, poisoned.GIT_DIR);
			assert.equal(execFileSync("git", ["rev-parse", "HEAD"], { cwd: canary, env: cleanEnv, encoding: "utf8" }), canaryHead);
			assert.deepEqual(fs.readFileSync(path.join(canary, ".git", "index")), canaryIndex);
			assert.equal(execFileSync("git", ["status", "--porcelain"], { cwd: candidate, env: cleanEnv, encoding: "utf8" }), "");
		} finally {
			for (const [key, value] of previous) { if (value === undefined) delete process.env[key]; else process.env[key] = value; }
		}
	});
});
