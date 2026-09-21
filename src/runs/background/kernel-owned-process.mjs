import { execFile } from "node:child_process";
import { AsyncLocalStorage } from "node:async_hooks";
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import {
  MARKER,
  binding,
  bound,
  cancelled,
  canonical,
  digest,
  ensureDirectory,
  isProcessIdentity,
  isRecord,
  isString,
  isBoolean,
  isTimestamp,
  loadDecision,
  loadRequest,
  normalizeCommand,
  optionalJson,
  publish,
  publishJson,
  publishRecord,
  validRecord,
  sealNeverStarted,
  verifyAssets,
} from "./kernel-owned-process-store.mjs";

const execute = promisify(execFile);
const moduleDirectory = path.dirname(fileURLToPath(import.meta.url));
const compilation = new Map();
const serviceDeadline = new AsyncLocalStorage();
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function serviceTimeout(maximum = 2000) {
  const deadline = serviceDeadline.getStore();
  const remaining = deadline === undefined ? maximum : Math.min(maximum, deadline - Date.now());
  if (remaining <= 0) throw new Error("control-deadline-exceeded");
  return remaining;
}

async function native(executable, args) {
  let output;
  try {
    ({ stdout: output } = await execute(executable, args, {
      encoding: "utf8",
      timeout: serviceTimeout(),
      maxBuffer: 4 * 1024 * 1024,
    }));
  } catch (error) {
    if (!isString(error.stdout) || !error.stdout) throw new Error("native-service-unavailable");
    output = error.stdout;
  }
  const result = JSON.parse(output);
  if (!isRecord(result) || !isBoolean(result.ok)) throw new Error("native-response-invalid");
  return result;
}

async function currentHost(executable) {
  const host = await native(executable, ["host"]);
  if (
    !host.ok ||
    host.platform !== "darwin" ||
    host.abi !== 1 ||
    !isString(host.hostId) ||
    !host.hostId ||
    !isString(host.bootId) ||
    !host.bootId ||
    !isString(host.monotonicNs) ||
    !/^[0-9]+$/.test(host.monotonicNs)
  )
    throw new Error("kernel-host-abi-unsupported");
  return host;
}

async function checkHost(envelope) {
  const host = await currentHost(envelope.request.nativeExecutable);
  if (host.hostId !== envelope.request.hostId || host.bootId !== envelope.request.bootId)
    throw new Error("kernel-host-or-boot-mismatch");
  return host;
}

async function compileNative(artifactDirectory) {
  ensureDirectory(artifactDirectory);
  const source = path.join(moduleDirectory, "kernel-owned-process-native.c");
  const sourceDigest = digest(fs.readFileSync(source));
  const destination = path.join(
    artifactDirectory,
    `darwin-coalition-${process.arch}-${sourceDigest}`,
  );
  if (fs.existsSync(destination)) return destination;
  const temporary = `${destination}.${randomUUID()}.tmp`;
  try {
    await execute(
      "/usr/bin/clang",
      ["-std=c11", "-O2", "-Wall", "-Wextra", "-Werror", source, "-o", temporary],
      { timeout: 10000, maxBuffer: 1024 * 1024 },
    );
    fs.chmodSync(temporary, 0o500);
    publish(destination, fs.readFileSync(temporary));
    fs.chmodSync(destination, 0o500);
  } catch {
    throw new Error("native-compiler-unavailable");
  } finally {
    fs.rmSync(temporary, { force: true });
  }
  return destination;
}

export async function preflightKernelOwnedProcess({ artifactDirectory }) {
  if (process.platform !== "darwin" || !["arm64", "x64"].includes(process.arch))
    return { supported: false, reason: "platform-unsupported" };
  try {
    const directory = path.resolve(artifactDirectory);
    const key = `${directory}:${digest(fs.readFileSync(path.join(moduleDirectory, "kernel-owned-process-native.c")))}`;
    let pending = compilation.get(key);
    if (!pending) {
      pending = compileNative(directory);
      compilation.set(key, pending);
      pending.catch(() => compilation.delete(key));
    }
    const nativeExecutable = await Promise.race([pending, sleep(900).then(() => undefined)]);
    if (!nativeExecutable) return { supported: false, reason: "kernel-runtime-initializing" };
    const host = await currentHost(nativeExecutable);
    const domain = `gui/${process.getuid()}`;
    await execute("/bin/launchctl", ["print", domain], {
      timeout: 1500,
      maxBuffer: 4 * 1024 * 1024,
    });
    return { supported: true, hostId: host.hostId, bootId: host.bootId, nativeExecutable };
  } catch (error) {
    return {
      supported: false,
      reason:
        error.message === "native-compiler-unavailable"
          ? error.message
          : "kernel-runtime-or-gui-domain-unavailable",
    };
  }
}

function receipt(directory, envelope) {
  return {
    operationDirectory: directory,
    ...binding(envelope),
    stdoutPath: path.join(directory, "workload.stdout.log"),
    stderrPath: path.join(directory, "workload.stderr.log"),
  };
}

export async function prepareKernelOwnedProcess(input) {
  if (process.env[MARKER]) throw new Error("nested-owned-launch-unsupported");
  const directory = path.resolve(input.operationDirectory);
  if (directory !== input.operationDirectory)
    throw new Error("operation-directory-must-be-canonical-absolute");
  ensureDirectory(directory);
  const command = normalizeCommand(input);
  const existing = optionalJson(path.join(directory, "request.json"));
  if (existing !== undefined) {
    const envelope = loadRequest(directory);
    if (canonical(command) !== canonical(envelope.request.command))
      throw new Error("immutable-launch-request-mismatch");
    verifyAssets(directory, envelope);
    await checkHost(envelope);
    if (cancelled(directory)) sealNeverStarted(directory, envelope);
    return receipt(directory, envelope);
  }
  const capability = await preflightKernelOwnedProcess({
    artifactDirectory:
      input.artifactDirectory ?? path.join(path.dirname(directory), ".kernel-owned-runtime"),
  });
  if (!capability.supported) throw new Error(capability.reason);
  const origin = await native(capability.nativeExecutable, ["inspect", String(process.pid)]);
  if (!origin.ok || !isProcessIdentity(origin.identity) || !isString(origin.coalitionId))
    throw new Error("origin-coalition-unavailable");
  const gate = fs.readFileSync(path.join(moduleDirectory, "kernel-owned-process-helper.mjs"));
  const store = fs
    .readFileSync(path.join(moduleDirectory, "kernel-owned-process-store.mjs"), "utf8")
    .replace('from "typebox"', `from ${JSON.stringify(import.meta.resolve("typebox"))}`)
    .replace(
      'from "typebox/compile"',
      `from ${JSON.stringify(import.meta.resolve("typebox/compile"))}`,
    );
  publish(path.join(directory, "kernel-owned-process-helper.mjs"), gate);
  publish(path.join(directory, "kernel-owned-process-store.mjs"), store);
  for (const name of [
    "workload.stdout.log",
    "workload.stderr.log",
    "gate.stdout.log",
    "gate.stderr.log",
  ])
    publish(path.join(directory, name), "");
  const operationId = randomUUID();
  const request = {
    operationDirectory: directory,
    operationId,
    command,
    hostId: capability.hostId,
    bootId: capability.bootId,
    originCoalitionId: origin.coalitionId,
    nativeExecutable: capability.nativeExecutable,
    nativeDigest: digest(fs.readFileSync(capability.nativeExecutable)),
    gateDigest: digest(gate),
    storeDigest: digest(store),
    nodeExecutable: process.execPath,
    label: `com.pi-subagents.owned.${operationId}`,
    domain: `gui/${process.getuid()}`,
  };
  publishJson(path.join(directory, "request.json"), {
    version: 1,
    digest: digest(canonical(request)),
    request,
  });
  const envelope = loadRequest(directory);
  if (canonical(command) !== canonical(envelope.request.command))
    throw new Error("immutable-launch-request-mismatch");
  verifyAssets(directory, envelope);
  if (cancelled(directory)) sealNeverStarted(directory, envelope);
  return receipt(directory, envelope);
}

function xml(value) {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&apos;");
}

function plist(directory, envelope) {
  const request = envelope.request;
  const args = [
    request.nodeExecutable,
    path.join(directory, "kernel-owned-process-helper.mjs"),
    directory,
    envelope.digest,
  ];
  return `<?xml version="1.0" encoding="UTF-8"?><!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd"><plist version="1.0"><dict><key>Label</key><string>${xml(request.label)}</string><key>ProgramArguments</key><array>${args.map((arg) => `<string>${xml(arg)}</string>`).join("")}</array><key>RunAtLoad</key><true/><key>KeepAlive</key><false/><key>LaunchOnlyOnce</key><true/><key>ExitTimeOut</key><integer>1</integer><key>Umask</key><integer>63</integer><key>StandardOutPath</key><string>${xml(path.join(directory, "gate.stdout.log"))}</string><key>StandardErrorPath</key><string>${xml(path.join(directory, "gate.stderr.log"))}</string></dict></plist>`;
}

async function bootout(directory, envelope) {
  const started = optionalJson(path.join(directory, "bootstrap.json"));
  if (started === undefined || !bound(started, envelope)) return;
  try {
    await execute(
      "/bin/launchctl",
      ["bootout", `${envelope.request.domain}/${envelope.request.label}`],
      { timeout: serviceTimeout(), maxBuffer: 65536 },
    );
  } catch {
    /* Job lifecycle is not terminal evidence. */
  }
}

async function ensurePreparedBootstrap(directory, envelope) {
  if (cancelled(directory)) { sealNeverStarted(directory, envelope); return; }
  if (loadDecision(directory, envelope)) return;
  publishJson(path.join(directory, "bootstrap.json"), { ...binding(envelope), requestedAt: new Date().toISOString() });
  const intent = optionalJson(path.join(directory, "bootstrap.json"));
  if (!bound(intent, envelope) || !isTimestamp(intent.requestedAt)) throw new Error("bootstrap-intent-invalid");
  const target = `${envelope.request.domain}/${envelope.request.label}`;
  let absent = false;
  try {
    await execute("/bin/launchctl", ["print", target], { timeout: serviceTimeout(1500), maxBuffer: 65536 });
  } catch (error) {
    absent = isString(error.stderr) && error.stderr.includes("Could not find service");
  }
  if (!absent || loadDecision(directory, envelope)) return;
  if (cancelled(directory)) { sealNeverStarted(directory, envelope); return; }
  const job = path.join(directory, "job.plist");
  const content = plist(directory, envelope);
  publish(job, content);
  if (fs.readFileSync(job, "utf8") !== content) throw new Error("launch-plist-digest-mismatch");
  try {
    await execute("/bin/launchctl", ["bootstrap", envelope.request.domain, job], { timeout: serviceTimeout(), maxBuffer: 65536 });
  } catch {
    /* A bounded service timeout leaves the same prepared admission pending. */
  }
}

/** Reconcile an already-authorized immutable request; never manufacture a new launch identity. */
export async function reconcileKernelOwnedProcess(operationDirectory) {
  const directory = path.resolve(operationDirectory);
  const observation = await observeKernelOwnedProcess(directory);
  if (observation.status !== "pending") return observation;
  if (process.env[MARKER]) return { ...observation, status: "unknown", reason: "nested-owned-launch-unsupported" };
  try {
    await serviceDeadline.run(Date.now() + 1200, async () => {
      const envelope = loadRequest(directory);
      verifyAssets(directory, envelope);
      await checkHost(envelope);
      await ensurePreparedBootstrap(directory, envelope);
    });
  } catch {
    return { ...observation, reason: "prepared-launch-reconciliation-pending" };
  }
  return observeKernelOwnedProcess(directory);
}

export async function launchKernelOwnedProcess(input) {
  const prepared = await prepareKernelOwnedProcess(input);
  const directory = prepared.operationDirectory;
  const envelope = loadRequest(directory);
  let observation = await observeKernelOwnedProcess(directory);
  if (["never-started", "retired"].includes(observation.status))
    return {
      ...prepared,
      observation,
      identity: observation.identity,
      workloadIdentity: observation.workloadIdentity,
    };
  if (observation.status === "unknown") throw new Error("owned-launch-journal-unknown");
  if (observation.status === "pending") await ensurePreparedBootstrap(directory, envelope);
  const deadline = Date.now() + 5000;
  do {
    observation = await observeKernelOwnedProcess(directory);
    if (
      observation.workloadIdentity ||
      ["never-started", "retired", "unknown"].includes(observation.status)
    )
      break;
    await sleep(25);
  } while (Date.now() < deadline);
  return {
    ...prepared,
    observation,
    identity: observation.identity,
    workloadIdentity: observation.workloadIdentity,
  };
}

export async function observeKernelOwnedProcess(operationDirectory) {
  if (serviceDeadline.getStore() !== undefined) return observeOperation(operationDirectory);
  return serviceDeadline.run(Date.now() + 1200, () => observeOperation(operationDirectory));
}

async function observeOperation(operationDirectory) {
  const directory = path.resolve(operationDirectory);
  try {
    const envelope = loadRequest(directory);
    verifyAssets(directory, envelope);
    const host = await checkHost(envelope);
    const base = { operationDirectory: directory, binding: binding(envelope) };
    const cancellationRequested = cancelled(directory);
    const decision = loadDecision(directory, envelope);
    if (!decision) return { ...base, status: "pending" };
    if (decision.kind === "never-started")
      return {
        ...base,
        status: "never-started",
        proof: { kind: "never-started", ...binding(envelope), observedAt: decision.observedAt },
      };
    const workload = optionalJson(path.join(directory, "workload.json"));
    const exit = optionalJson(path.join(directory, "exit.json"));
    let timeout = optionalJson(path.join(directory, "timeout.json"));
    if (
      workload !== undefined &&
      (!validRecord(workload) ||
        !bound(workload, envelope) ||
        !isProcessIdentity(workload.identity) ||
        !isTimestamp(workload.observedAt))
    )
      throw new Error("workload-identity-invalid");
    if (
      exit !== undefined &&
      (!validRecord(exit) ||
        !bound(exit, envelope) ||
        (exit.exitCode !== null && !Number.isSafeInteger(exit.exitCode)) ||
        (exit.signal !== null && !isString(exit.signal)) ||
        !isBoolean(exit.timedOut) ||
        !isTimestamp(exit.observedAt))
    )
      throw new Error("workload-exit-invalid");
    if (
      timeout !== undefined &&
      (!validRecord(timeout) || !bound(timeout, envelope) || !isTimestamp(timeout.observedAt))
    )
      throw new Error("workload-timeout-invalid");
    const details = { ...base, identity: decision.identity };
    if (workload) details.workloadIdentity = workload.identity;
    if (exit) {
      details.exitCode = exit.exitCode;
      details.signal = exit.signal;
      details.timedOut = exit.timedOut || timeout !== undefined;
    } else if (timeout) details.timedOut = true;
    const observed = await native(envelope.request.nativeExecutable, [
      "coalition",
      decision.identity.coalitionId,
    ]);
    if (observed.ok) {
      if (
        !isString(observed.started) ||
        !isString(observed.exited) ||
        !/^[0-9]+$/.test(observed.started) ||
        !/^[0-9]+$/.test(observed.exited)
      )
        throw new Error("coalition-counters-invalid");
      if (
        envelope.request.command.lifetime.kind === "bounded" &&
        BigInt(host.monotonicNs) >= BigInt(decision.deadlineMonotonicNs)
      ) {
        publishRecord(path.join(directory, "timeout.json"), {
          ...binding(envelope),
          observedAt: new Date().toISOString(),
        });
        timeout = optionalJson(path.join(directory, "timeout.json"));
        if (!validRecord(timeout) || !bound(timeout, envelope))
          throw new Error("workload-timeout-invalid");
        details.timedOut = true;
      }
      if (cancellationRequested || timeout !== undefined) {
        const members = await native(envelope.request.nativeExecutable, [
          "members",
          decision.identity.coalitionId,
        ]);
        if (!members.ok || !Array.isArray(members.members))
          throw new Error("cancellation-members-unavailable");
        const sweepDeadline = Date.now() + 500;
        for (const member of members.members.slice(0, 32)) {
          if (Date.now() >= sweepDeadline) break;
          if (!isProcessIdentity(member)) throw new Error("member-identity-invalid");
          if (member.pid === process.pid) continue;
          await native(envelope.request.nativeExecutable, [
            "signal",
            String(member.pid),
            String(member.pidVersion),
            member.uniqueId,
            decision.identity.coalitionId,
            "9",
          ]);
        }
      }
      return { ...details, status: "active" };
    }
    if (observed.errno !== 3)
      return { ...details, status: "unknown", reason: "coalition-observation-failed" };
    return {
      ...details,
      status: "retired",
      proof: {
        kind: "darwin-coalition-retired",
        ...binding(envelope),
        identity: decision.identity,
        observedAt: new Date().toISOString(),
      },
    };
  } catch (error) {
    const reasons = new Set([
      "kernel-host-or-boot-mismatch",
      "request-digest-invalid",
      "admission-binding-invalid",
      "admission-identity-invalid",
      "workload-identity-invalid",
      "workload-exit-invalid",
      "runtime-asset-digest-invalid",
      "journal-file-invalid",
      "request-schema-invalid",
      "request-directory-mismatch",
    ]);
    return {
      operationDirectory: directory,
      status: "unknown",
      reason: reasons.has(error.message) ? error.message : "owned-operation-unavailable",
    };
  }
}

export async function inspectKernelOwnedProcessMembership(operationDirectory, pid = process.pid) {
  try {
    if (!Number.isSafeInteger(pid) || pid < 1) throw new Error("invalid-pid");
    const directory = path.resolve(operationDirectory);
    const envelope = loadRequest(directory);
    verifyAssets(directory, envelope);
    await checkHost(envelope);
    const decision = loadDecision(directory, envelope);
    if (!decision || decision.kind !== "admitted")
      return { owned: false, reason: "operation-not-admitted" };
    const inspected = await native(envelope.request.nativeExecutable, ["inspect", String(pid)]);
    if (
      !inspected.ok ||
      !isProcessIdentity(inspected.identity) ||
      inspected.coalitionId !== decision.identity.coalitionId
    )
      return { owned: false, reason: "process-outside-owned-coalition" };
    return { owned: true, identity: decision.identity, processIdentity: inspected.identity };
  } catch {
    return { owned: false, reason: "membership-unavailable" };
  }
}

export async function requestKernelOwnedProcessCancellation(operationDirectory) {
  const directory = path.resolve(operationDirectory);
  ensureDirectory(directory);
  publishJson(path.join(directory, "cancel-request.json"), {
    version: 1,
    requestedAt: new Date().toISOString(),
  });
  cancelled(directory);
}

export async function cancelKernelOwnedProcess(operationDirectory, options = {}) {
  const duration = options.deadlineMs ?? 5000;
  if (!Number.isSafeInteger(duration) || duration < 0 || duration > 60000)
    throw new Error("invalid-cancellation-deadline");
  return serviceDeadline.run(Date.now() + duration, () =>
    cancelOperation(operationDirectory, duration),
  );
}

async function cancelOperation(operationDirectory, duration) {
  const directory = path.resolve(operationDirectory);
  await requestKernelOwnedProcessCancellation(directory);
  let envelope;
  try {
    envelope = loadRequest(directory);
    verifyAssets(directory, envelope);
    await checkHost(envelope);
    sealNeverStarted(directory, envelope);
  } catch {
    return observeKernelOwnedProcess(directory);
  }
  let observation = await observeKernelOwnedProcess(directory);
  if (["never-started", "retired"].includes(observation.status)) {
    await bootout(directory, envelope);
    return observation;
  }
  if (observation.status === "unknown") return observation;
  let caller;
  try {
    caller = await native(envelope.request.nativeExecutable, ["inspect", String(process.pid)]);
  } catch {
    return { ...observation, status: "unknown", reason: "cancellation-control-deadline" };
  }
  if (caller.ok && caller.coalitionId === observation.identity?.coalitionId)
    return { ...observation, reason: "cancellation-requested-inside-owner" };
  const started = Date.now();
  do {
    observation = await observeKernelOwnedProcess(directory);
    if (["never-started", "retired"].includes(observation.status)) {
      await bootout(directory, envelope);
      return observation;
    }
    if (observation.status === "unknown" || !observation.identity) return observation;
    try {
      const members = await native(envelope.request.nativeExecutable, [
        "members",
        observation.identity.coalitionId,
      ]);
      if (members.ok && Array.isArray(members.members)) {
        for (const member of members.members) {
          serviceTimeout();
          if (!isProcessIdentity(member))
            return { ...observation, status: "unknown", reason: "member-identity-invalid" };
          await native(envelope.request.nativeExecutable, [
            "signal",
            String(member.pid),
            String(member.pidVersion),
            member.uniqueId,
            observation.identity.coalitionId,
            Date.now() - started < 300 ? "15" : "9",
          ]);
        }
      }
    } catch {
      return { ...observation, status: "unknown", reason: "cancellation-observation-failed" };
    }
    try {
      await sleep(serviceTimeout(25));
    } catch {
      break;
    }
  } while (Date.now() - started < duration);
  return { ...observation, status: "unknown", reason: "coalition-retirement-not-observed" };
}
