import { execFileSync, spawn } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import {
  BACKEND,
  MARKER,
  binding,
  bound,
  cancelled,
  isProcessIdentity,
  isString,
  isRecord,
  loadDecision,
  loadRequest,
  publishRecord,
  sealNeverStarted,
  verifyAssets,
} from "./kernel-owned-process-store.mjs";

const directory = process.argv[2];
let envelope;

function native(...args) {
  let output;
  try {
    output = execFileSync(envelope.request.nativeExecutable, args, {
      encoding: "utf8",
      timeout: 2000,
      maxBuffer: 4 * 1024 * 1024,
      stdio: ["ignore", "pipe", "ignore"],
    });
  } catch (error) {
    if (!isString(error.stdout)) throw new Error("native-observation-failed");
    output = error.stdout;
  }
  const value = JSON.parse(output);
  if (!isRecord(value)) throw new Error("native-response-invalid");
  return value;
}

function recordExit(exitCode, signal, timedOut) {
  publishRecord(path.join(directory, "exit.json"), {
    ...binding(envelope),
    exitCode,
    signal,
    timedOut,
    observedAt: new Date().toISOString(),
  });
}

function start() {
  envelope = loadRequest(directory);
  if (envelope.digest !== process.argv[3]) throw new Error("launch-request-binding-invalid");
  verifyAssets(directory, envelope);
  if (loadDecision(directory, envelope)) return;
  if (cancelled(directory)) {
    sealNeverStarted(directory, envelope);
    return;
  }
  const host = native("host");
  if (
    !host.ok ||
    host.abi !== 1 ||
    host.platform !== "darwin" ||
    host.hostId !== envelope.request.hostId ||
    host.bootId !== envelope.request.bootId ||
    !isString(host.monotonicNs) ||
    !/^[0-9]+$/.test(host.monotonicNs)
  )
    throw new Error("launch-host-binding-invalid");
  const inspected = native("inspect", String(process.pid));
  if (
    !inspected.ok ||
    !isProcessIdentity(inspected.identity) ||
    inspected.identity.pid !== process.pid ||
    inspected.coalitionId === envelope.request.originCoalitionId
  )
    throw new Error("launch-coalition-not-isolated");
  const registration = native("coalition", inspected.coalitionId);
  if (!registration.ok || BigInt(registration.started) - BigInt(registration.exited) !== 2n)
    throw new Error("launch-coalition-not-exclusive");
  const identity = {
    version: 1,
    backend: BACKEND,
    ...binding(envelope),
    coalitionId: inspected.coalitionId,
    leader: inspected.identity,
  };
  const admission = {
    kind: "admitted",
    ...binding(envelope),
    identity,
    registration: { started: registration.started, exited: registration.exited },
    observedAt: new Date().toISOString(),
  };
  if (envelope.request.command.lifetime.kind === "bounded")
    admission.deadlineMonotonicNs = String(
      BigInt(host.monotonicNs) + BigInt(envelope.request.command.lifetime.timeoutMs) * 1_000_000n,
    );
  const won = publishRecord(path.join(directory, "decision.json"), admission);
  if (!won) return;
  if (!bound(loadDecision(directory, envelope), envelope))
    throw new Error("durable-admission-invalid");

  let child;
  let childClosed = false;
  let finishing = cancelled(directory);
  let timedOut = false;
  let finishStarted = finishing ? Date.now() : undefined;
  let timer;
  let deadline;
  const stdout = fs.openSync(path.join(directory, "workload.stdout.log"), "a", 0o600);
  const stderr = fs.openSync(path.join(directory, "workload.stderr.log"), "a", 0o600);

  function finish() {
    if (!finishing) {
      finishing = true;
      finishStarted = Date.now();
    }
  }

  function cleanup() {
    try {
      if (cancelled(directory)) finish();
      if (!finishing && !childClosed) return;
      const members = native("members", identity.coalitionId);
      if (!members.ok || !Array.isArray(members.members)) return;
      const others = members.members.filter((member) => member.pid !== process.pid);
      const signum = Date.now() - finishStarted < 300 ? "15" : "9";
      if (finishing) {
        for (const member of others) {
          if (!isProcessIdentity(member)) throw new Error("member-identity-invalid");
          native(
            "signal",
            String(member.pid),
            String(member.pidVersion),
            member.uniqueId,
            identity.coalitionId,
            signum,
          );
        }
      }
      if (others.length === 0 && (!child || childClosed)) {
        clearInterval(timer);
        clearTimeout(deadline);
        if (!child) recordExit(null, null, timedOut);
        process.exit(0);
      }
    } catch {
      // Preserve the live coalition when cleanup cannot be established; an external observer can retry.
    }
  }

  if (!finishing) {
    const command = envelope.request.command;
    child = spawn(command.argv[0], command.argv.slice(1), {
      cwd: command.cwd,
      env: {
        ...command.env,
        [MARKER]: JSON.stringify({ operationDirectory: directory, ...binding(envelope) }),
      },
      stdio: ["ignore", stdout, stderr],
      detached: false,
    });
    child.on("spawn", () => {
      try {
        const workload = native("inspect", String(child.pid));
        if (
          workload.ok &&
          isProcessIdentity(workload.identity) &&
          workload.coalitionId === identity.coalitionId
        ) {
          publishRecord(path.join(directory, "workload.json"), {
            ...binding(envelope),
            identity: workload.identity,
            observedAt: new Date().toISOString(),
          });
        }
      } catch {
        /* A fast exit is resolved by the bound exit record and coalition retirement. */
      }
    });
    child.on("error", () => {
      childClosed = true;
      recordExit(null, "spawn-error", timedOut);
      finish();
    });
    child.on("close", (code, signal) => {
      childClosed = true;
      recordExit(code, signal, timedOut);
    });
    if (command.lifetime.kind === "bounded") {
      deadline = setTimeout(() => {
        timedOut = true;
        publishRecord(path.join(directory, "timeout.json"), {
          ...binding(envelope),
          observedAt: new Date().toISOString(),
        });
        finish();
      }, command.lifetime.timeoutMs);
    }
  }
  fs.closeSync(stdout);
  fs.closeSync(stderr);
  timer = setInterval(cleanup, 100);
  cleanup();
}

try {
  start();
} catch {
  process.stderr.write("owned-process-gate-failed\n");
  process.exitCode = 1;
}
