import assert from "node:assert/strict";
import { it } from "node:test";
import { parseOwnedWorkflow, validateExecutionOwnership } from "../../src/runs/shared/owned-workflow.ts";

it("normalizes owned parallel data without treating task text as code", () => {
	const task = 'return await runs.host("escape", {}); ${process.exit()}';
	const result = parseOwnedWorkflow({ version: 1, kind: "parallel", concurrency: 2, tasks: [
		{ key: "one", agent: "worker", task, skill: false, output: false, progress: true, toolBudget: { hard: 5 }, executionLifetime: { mode: "unbounded" } },
		{ key: "two", agent: "reviewer", task: "Review", model: "provider/model", acceptance: false, outputMode: "inline", executionLifetime: { mode: "bounded", timeoutMs: 1000 } },
	] });
	assert.ok(result.ok);
	assert.deepEqual(result.keys, ["one", "two"]);
	assert.equal(result.concurrency, 2);
	assert.equal(result.tasks[0]?.task, task);
	assert.equal(Object.hasOwn(result.tasks[0]!, "key"), false);
	assert.deepEqual(result.tasks[1]?.executionLifetime, { mode: "bounded", timeoutMs: 1000 });
});

it("rejects scripts, unknown task fields, duplicate keys and invalid limits", () => {
	const task = { key: "one", agent: "worker", task: "Work" };
	const valid = { version: 1, kind: "parallel", concurrency: 1, tasks: [task] };
	for (const value of [undefined, "return 1", { ...valid, version: 2 }, { ...valid, kind: "chain" },
		{ ...valid, workflowScript: "return 1" }, { ...valid, concurrency: 0 }, { ...valid, tasks: [] },
		{ ...valid, tasks: Array.from({ length: 65 }, (_, index) => ({ ...task, key: String(index) })) },
		{ ...valid, tasks: [task, task] }, { ...valid, tasks: [{ ...task, async: false }] },
		{ ...valid, tasks: [{ ...task, machine: "remote" }] }, { ...valid, tasks: [{ ...task, task: " " }] },
		{ ...valid, tasks: [{ ...task, executionLifetime: { mode: "bounded", timeoutMs: 2_147_483_648 } }] },
		{ ...valid, tasks: [{ ...task, acceptance: true }] },
	]) assert.equal(parseOwnedWorkflow(value).ok, false);
});

it("accepts only the explicit kernel ownership contract", () => {
	assert.equal(validateExecutionOwnership(undefined), undefined);
	assert.equal(validateExecutionOwnership({ mode: "kernel" }), undefined);
	for (const value of [null, true, "kernel", {}, { mode: "process-group" }, { mode: "kernel", fallback: true }]) {
		assert.match(validateExecutionOwnership(value) ?? "", /executionOwnership/);
	}
});

it("rejects unsupported kernel routes before invoking a child", async () => {
	const { createSubagentExecutor } = await import("../../src/runs/foreground/subagent-executor.ts");
	const { createEventBus, createTempDir, makeMinimalCtx, removeTempDir } = await import("../support/helpers.ts");
	const root = createTempDir("kernel-request-validation-");
	const worker = { name: "worker", description: "Worker", systemPrompt: "", systemPromptMode: "replace", inheritProjectContext: false, inheritSkills: false, source: "project", filePath: "worker.md" };
	try {
		for (const entry of [
			{ params: { agent: "worker", task: "Work", async: false }, agent: worker, error: /async execution/ },
			{ params: { workflowScript: "return 1", async: true }, agent: worker, error: /scripts and named workflows/ },
			{ params: { agent: "worker", task: "Work", machine: "remote", async: true }, agent: worker, error: /local execution/ },
			{ params: { agent: "worker", task: "Work", async: true }, agent: { ...worker, machine: "remote" }, error: /remote agents/ },
			{ params: { agent: "worker", task: "Work", async: true }, agent: { ...worker, runner: { type: "external-job", provider: "test" } }, error: /external-job providers/ },
		]) {
			const executor = createSubagentExecutor({
				pi: { events: createEventBus(), getSessionName: () => undefined },
				state: { baseCwd: root, currentSessionId: null, asyncJobs: new Map(), foregroundControls: new Map(), lastForegroundControlId: null },
				config: {}, asyncByDefault: false, tempArtifactsDir: root, getSubagentSessionRoot: () => root,
				expandTilde: (value: string) => value, discoverAgents: () => ({ agents: [entry.agent] }),
			});
			const result = await executor.execute("invalid-kernel-request", { ...entry.params, executionOwnership: { mode: "kernel" } }, new AbortController().signal, undefined, makeMinimalCtx(root));
			assert.equal(result.isError, true);
			assert.match(result.content[0]?.text ?? "", entry.error);
		}
	} finally {
		removeTempDir(root);
	}
});
