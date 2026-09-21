import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import { once } from "node:events";
import * as fs from "node:fs";
import * as path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { Type, type Static } from "typebox";
import { Compile } from "typebox/compile";

const artifactRoot = process.env.PI_KERNEL_NATIVE_TEST_DIR;
const identitySchema = Type.Object({
  pid: Type.Integer({ minimum: 1 }),
  uniqueId: Type.String({ pattern: "^[0-9]+$" }),
  pidVersion: Type.Integer({ minimum: 0 }),
});
const responseSchema = Type.Object({
  ok: Type.Boolean(),
  errno: Type.Optional(Type.Integer()),
  error: Type.Optional(Type.String()),
  identity: Type.Optional(identitySchema),
  coalitionId: Type.Optional(Type.String({ pattern: "^[0-9]+$" })),
  started: Type.Optional(Type.String({ pattern: "^[0-9]+$" })),
  exited: Type.Optional(Type.String({ pattern: "^[0-9]+$" })),
  members: Type.Optional(Type.Array(identitySchema)),
  incomplete: Type.Optional(Type.Boolean()),
  hostId: Type.Optional(Type.String()),
  bootId: Type.Optional(Type.String()),
  monotonicNs: Type.Optional(Type.String({ pattern: "^[0-9]+$" })),
  platform: Type.Optional(Type.Literal("darwin")),
  abi: Type.Optional(Type.Literal(1)),
});
const responseValidator = Compile(responseSchema);
type NativeResponse = Static<typeof responseSchema>;

test(
  "native coalition CLI binds signals to identity and coalition",
  {
    skip: process.platform !== "darwin" || !artifactRoot,
    timeout: 30_000,
  },
  async () => {
    assert.ok(artifactRoot);
    fs.mkdirSync(artifactRoot, { recursive: true });
    const directory = fs.mkdtempSync(path.join(artifactRoot, "native-cli-"));
    const binary = path.join(directory, "kernel-owned-process-native");
    const source = fileURLToPath(
      new URL("../../src/runs/background/kernel-owned-process-native.c", import.meta.url),
    );
    const build = spawnSync(
      "/usr/bin/clang",
      ["-std=c11", "-O2", "-Wall", "-Wextra", "-Werror", source, "-o", binary],
      {
        encoding: "utf-8",
        timeout: 20_000,
      },
    );
    assert.equal(build.status, 0, build.stderr);
    function invoke(...args: string[]): NativeResponse {
      const result = spawnSync(binary, args, { encoding: "utf-8", timeout: 3_000 });
      assert.equal(result.error, undefined);
      assert.equal(result.signal, null);
      const value: unknown = JSON.parse(result.stdout);
      assert.ok(responseValidator.Check(value), result.stdout);
      assert.equal(result.status, value.ok === true ? 0 : 1);
      return value;
    }
    try {
      const host = invoke("host");
      assert.equal(host.ok, true);
      assert.equal(host.platform, "darwin");
      assert.equal(host.abi, 1);
      assert.match(String(host.hostId), /^[0-9a-f-]{36}$/);
      assert.match(String(host.bootId), /^[0-9a-f-]{36}$/);
      assert.ok(host.monotonicNs);
      let previousNs = BigInt(host.monotonicNs);
      for (let attempt = 0; attempt < 3; attempt++) {
        const next = invoke("host");
        assert.ok(next.monotonicNs);
        assert.deepEqual({ ...next, monotonicNs: host.monotonicNs }, host);
        const nextNs = BigInt(next.monotonicNs);
        assert.ok(nextNs >= previousNs);
        previousNs = nextNs;
      }
      assert.equal(invoke("coalition", "18446744073709551615").errno, 3);
      for (const invalid of ["-1", "0", "1junk", " 1", "18446744073709551616"]) {
        assert.equal(invoke("inspect", invalid).errno, 22);
      }
      const child = spawn("/bin/sleep", ["8"], { stdio: "ignore" });
      const closed = once(child, "close");
      await once(child, "spawn");
      assert.ok(child.pid);
      const inspected = invoke("inspect", String(child.pid));
      assert.equal(inspected.ok, true);
      assert.ok(inspected.identity);
      const identity = inspected.identity;
      assert.equal(identity.pid, child.pid);
      assert.ok(inspected.coalitionId);
      const pid = String(child.pid);
      const version = String(identity.pidVersion);
      const unique = String(identity.uniqueId);
      const coalition = String(inspected.coalitionId);
      const signalArgs = [pid, version, unique, coalition, "15"];
      try {
        const usage = invoke("coalition", coalition);
        assert.equal(usage.ok, true);
        assert.ok(BigInt(String(usage.started)) > BigInt(String(usage.exited)));
        const listed = invoke("members", coalition);
        assert.equal(listed.ok, true);
        assert.notEqual(listed.incomplete, undefined);
        assert.ok(listed.members);
        assert.ok(listed.members.some((entry) => entry.pid === child.pid));
        assert.equal(
          invoke("signal", pid, String(Number(version) + 1), unique, coalition, "15").errno,
          3,
        );
        assert.equal(
          invoke("signal", pid, version, String(BigInt(unique) + 1n), coalition, "15").errno,
          3,
        );
        assert.equal(invoke("signal", pid, version, unique, "18446744073709551615", "15").errno, 1);
        assert.deepEqual(invoke("inspect", pid), inspected);
        const supervisor = invoke("inspect", String(process.pid));
        assert.ok(supervisor.identity);
        const own = supervisor.identity;
        assert.equal(
          invoke(
            "signal",
            String(process.pid),
            String(own.pidVersion),
            String(own.uniqueId),
            String(supervisor.coalitionId),
            "15",
          ).errno,
          1,
        );
        assert.equal(invoke("signal", ...signalArgs).ok, true);
        const [, signal] = await closed;
        assert.equal(signal, "SIGTERM");
        assert.equal(invoke("inspect", pid).errno, 3);
      } finally {
        if (child.exitCode === null && child.signalCode === null) invoke("signal", ...signalArgs);
        await closed;
      }
    } finally {
      fs.rmSync(directory, { recursive: true, force: true });
    }
  },
);
