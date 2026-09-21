import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as path from "node:path";
import { setImmediate } from "node:timers/promises";
import { it } from "node:test";
import { waitForStartupControl } from "../../src/runs/background/subagent-runner.ts";
import { requestAsyncStop } from "../../src/runs/background/control-channel.ts";
import { createTempDir, removeTempDir } from "../support/helpers.ts";

it("keeps unbounded startup permission pending beyond the legacy cutoff and accepts a recovered grant", async (t) => {
	const directory = createTempDir("startup-permission-");
	t.after(() => removeTempDir(directory));
	t.mock.timers.enable({ apis: ["Date", "setTimeout"], now: Date.now() });
	const controlPath = path.join(directory, "runner-startup-proceed.json");
	let settled = false;
	const waiting = waitForStartupControl(controlPath, "same-durable-token", "proceed", { mode: "unbounded" }).finally(() => { settled = true; });
	t.mock.timers.tick(31 * 60 * 1000);
	await setImmediate();
	assert.equal(settled, false);
	fs.writeFileSync(controlPath, JSON.stringify({ action: "proceed", token: "same-durable-token" }));
	t.mock.timers.tick(20);
	await waiting;
	assert.equal(settled, true);
});

it("honors cancellation before a delayed startup grant", async (t) => {
	const directory = createTempDir("startup-cancel-");
	t.after(() => removeTempDir(directory));
	t.mock.timers.enable({ apis: ["Date", "setTimeout"], now: Date.now() });
	const controlPath = path.join(directory, "runner-startup-proceed.json");
	const waiting = waitForStartupControl(controlPath, "token", "proceed", { mode: "unbounded" });
	const rejected = assert.rejects(waiting, /cancelled before permission/);
	t.mock.timers.tick(60_000);
	await setImmediate();
	requestAsyncStop(directory, { source: "test-cancel" });
	fs.writeFileSync(controlPath, JSON.stringify({ action: "proceed", token: "token" }));
	t.mock.timers.tick(20);
	await rejected;
});

for (const lifetime of [undefined, { mode: "bounded" as const, timeoutMs: 1000 }]) it(`retains ${lifetime ? "an explicit bounded" : "the legacy"} startup permission deadline`, async (t) => {
	const directory = createTempDir("startup-bounded-");
	t.after(() => removeTempDir(directory));
	t.mock.timers.enable({ apis: ["Date", "setTimeout"], now: Date.now() });
	const timeoutMs = lifetime?.timeoutMs ?? 30_000;
	const waiting = waitForStartupControl(path.join(directory, "runner-startup-proceed.json"), "token", "proceed", lifetime);
	const rejected = assert.rejects(waiting, new RegExp(`Timed out after ${timeoutMs}ms`));
	t.mock.timers.tick(timeoutMs + 1);
	await rejected;
});
