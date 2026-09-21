import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { Type } from "typebox";
import { Compile } from "typebox/compile";

export const BACKEND = "darwin-resource-coalition-v1";
export const MARKER = "PI_KERNEL_OWNED_OPERATION";
const recordValidator = Compile(Type.Record(Type.String(), Type.Unknown()));
const stringValidator = Compile(Type.String());
const booleanValidator = Compile(Type.Boolean());
export const isString = (value) => stringValidator.Check(value);
export const isBoolean = (value) => booleanValidator.Check(value);

export function canonical(value) {
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
  if (isRecord(value)) {
    return `{${Object.keys(value)
      .sort()
      .map((key) => `${JSON.stringify(key)}:${canonical(value[key])}`)
      .join(",")}}`;
  }
  return JSON.stringify(value);
}

export function digest(value) {
  return createHash("sha256").update(value).digest("hex");
}

export function ensureDirectory(directory) {
  if (!path.isAbsolute(directory)) throw new Error("operation-directory-must-be-absolute");
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  const stat = fs.lstatSync(directory);
  if (!stat.isDirectory() || stat.isSymbolicLink() || stat.uid !== process.getuid()) {
    throw new Error("operation-directory-not-owned");
  }
  fs.chmodSync(directory, 0o700);
}

export function syncDirectory(directory) {
  const fd = fs.openSync(directory, "r");
  try {
    fs.fsyncSync(fd);
  } finally {
    fs.closeSync(fd);
  }
}

export function publish(file, content) {
  const temporary = `${file}.${randomUUID()}.tmp`;
  const fd = fs.openSync(temporary, "wx", 0o600);
  try {
    fs.writeFileSync(fd, content);
    fs.fsyncSync(fd);
  } finally {
    fs.closeSync(fd);
  }
  try {
    fs.linkSync(temporary, file);
    syncDirectory(path.dirname(file));
    return true;
  } catch (error) {
    if (error.code === "EEXIST") return false;
    throw error;
  } finally {
    fs.unlinkSync(temporary);
  }
}

export function publishJson(file, value) {
  return publish(file, `${canonical(value)}\n`);
}

export function publishRecord(file, value) {
  return publishJson(file, { ...value, recordDigest: digest(canonical(value)) });
}

export function validRecord(value) {
  if (!isRecord(value)) return false;
  const { recordDigest, ...payload } = value;
  return isString(recordDigest) && recordDigest === digest(canonical(payload));
}

export function readJson(file) {
  const stat = fs.lstatSync(file);
  if (
    !stat.isFile() ||
    stat.isSymbolicLink() ||
    stat.size > 4 * 1024 * 1024 ||
    stat.uid !== process.getuid() ||
    stat.mode & 0o077
  ) {
    throw new Error("journal-file-invalid");
  }
  return JSON.parse(fs.readFileSync(file, "utf8"));
}

export function optionalJson(file) {
  try {
    return readJson(file);
  } catch (error) {
    if (error.code === "ENOENT") return undefined;
    throw error;
  }
}

export function isRecord(value) {
  return recordValidator.Check(value);
}

export function isProcessIdentity(value) {
  return (
    isRecord(value) &&
    Number.isSafeInteger(value.pid) &&
    value.pid > 0 &&
    isString(value.uniqueId) &&
    /^[1-9][0-9]*$/.test(value.uniqueId) &&
    Number.isSafeInteger(value.pidVersion) &&
    value.pidVersion >= 0 &&
    value.pidVersion <= 0xffffffff
  );
}

export function isTimestamp(value) {
  return (
    isString(value) && Number.isFinite(Date.parse(value)) && new Date(value).toISOString() === value
  );
}

export function normalizeCommand(request) {
  const argv = request.argv;
  if (
    !Array.isArray(argv) ||
    argv.length === 0 ||
    !argv.every((arg) => isString(arg) && !arg.includes("\0")) ||
    !path.isAbsolute(argv[0])
  ) {
    throw new Error("invalid-command-argv");
  }
  if (!isString(request.cwd) || !path.isAbsolute(request.cwd) || request.cwd.includes("\0"))
    throw new Error("invalid-command-cwd");
  if (
    !isRecord(request.env) ||
    Object.entries(request.env).some(
      ([key, value]) =>
        !key || key.includes("=") || key.includes("\0") || !isString(value) || value.includes("\0"),
    )
  ) {
    throw new Error("invalid-command-environment");
  }
  if (Object.hasOwn(request.env, MARKER)) throw new Error("nested-owned-launch-unsupported");
  const lifetime = request.lifetime;
  if (
    !isRecord(lifetime) ||
    (lifetime.kind !== "unbounded" && lifetime.kind !== "bounded") ||
    (lifetime.kind === "bounded" &&
      (!Number.isSafeInteger(lifetime.timeoutMs) ||
        lifetime.timeoutMs < 1 ||
        lifetime.timeoutMs > 2_147_483_647))
  ) {
    throw new Error("explicit-execution-lifetime-required");
  }
  return {
    argv: [...argv],
    cwd: request.cwd,
    env: { ...request.env },
    lifetime:
      lifetime.kind === "unbounded"
        ? { kind: "unbounded" }
        : { kind: "bounded", timeoutMs: lifetime.timeoutMs },
  };
}

export function loadRequest(directory) {
  const envelope = readJson(path.join(directory, "request.json"));
  if (
    !isRecord(envelope) ||
    envelope.version !== 1 ||
    !isString(envelope.digest) ||
    !isRecord(envelope.request) ||
    digest(canonical(envelope.request)) !== envelope.digest
  ) {
    throw new Error("request-digest-invalid");
  }
  const request = envelope.request;
  if (request.operationDirectory !== path.resolve(directory)) {
    throw new Error("request-directory-mismatch");
  }
  for (const key of [
    "operationId",
    "hostId",
    "bootId",
    "originCoalitionId",
    "nativeExecutable",
    "nativeDigest",
    "gateDigest",
    "storeDigest",
    "nodeExecutable",
    "label",
    "domain",
  ]) {
    if (!isString(request[key]) || !request[key] || request[key].includes("\0"))
      throw new Error("request-schema-invalid");
  }
  if (
    !/^[a-f0-9-]{36}$/.test(request.operationId) ||
    !/^gui\/[0-9]+$/.test(request.domain) ||
    request.label !== `com.pi-subagents.owned.${request.operationId}` ||
    !path.isAbsolute(request.nativeExecutable) ||
    !path.isAbsolute(request.nodeExecutable) ||
    canonical(normalizeCommand(request.command)) !== canonical(request.command)
  ) {
    throw new Error("request-schema-invalid");
  }
  return envelope;
}

export function binding(envelope) {
  const { operationId, hostId, bootId } = envelope.request;
  return { operationId, requestDigest: envelope.digest, hostId, bootId };
}

export function bound(value, envelope) {
  return (
    isRecord(value) &&
    Object.entries(binding(envelope)).every(([key, expected]) => value[key] === expected)
  );
}

export function loadDecision(directory, envelope) {
  const decision = optionalJson(path.join(directory, "decision.json"));
  if (decision === undefined) return undefined;
  if (!validRecord(decision) || !bound(decision, envelope) || !isTimestamp(decision.observedAt))
    throw new Error("admission-binding-invalid");
  if (decision.kind === "never-started") return decision;
  if (
    decision.kind !== "admitted" ||
    !isRecord(decision.identity) ||
    !bound(decision.identity, envelope) ||
    decision.identity.version !== 1 ||
    decision.identity.backend !== BACKEND ||
    !isProcessIdentity(decision.identity.leader) ||
    !isString(decision.identity.coalitionId) ||
    !/^[1-9][0-9]*$/.test(decision.identity.coalitionId) ||
    !isRecord(decision.registration) ||
    !/^[0-9]+$/.test(decision.registration.started) ||
    !/^[0-9]+$/.test(decision.registration.exited) ||
    BigInt(decision.registration.started) <= BigInt(decision.registration.exited)
  ) {
    throw new Error("admission-identity-invalid");
  }
  if (
    envelope.request.command.lifetime.kind === "bounded" &&
    (!isString(decision.deadlineMonotonicNs) || !/^[1-9][0-9]*$/.test(decision.deadlineMonotonicNs))
  )
    throw new Error("admission-deadline-invalid");
  return decision;
}

export function cancelled(directory) {
  const value = optionalJson(path.join(directory, "cancel-request.json"));
  if (value === undefined) return false;
  if (!isRecord(value) || value.version !== 1 || !isTimestamp(value.requestedAt))
    throw new Error("cancellation-record-invalid");
  return true;
}

export function sealNeverStarted(directory, envelope) {
  publishRecord(path.join(directory, "decision.json"), {
    kind: "never-started",
    ...binding(envelope),
    observedAt: new Date().toISOString(),
  });
  return loadDecision(directory, envelope);
}

export function verifyAssets(directory, envelope) {
  for (const [file, expected] of [
    [envelope.request.nativeExecutable, envelope.request.nativeDigest],
    [path.join(directory, "kernel-owned-process-helper.mjs"), envelope.request.gateDigest],
    [path.join(directory, "kernel-owned-process-store.mjs"), envelope.request.storeDigest],
  ]) {
    const stat = fs.lstatSync(file);
    if (
      !stat.isFile() ||
      stat.isSymbolicLink() ||
      stat.uid !== process.getuid() ||
      digest(fs.readFileSync(file)) !== expected
    )
      throw new Error("runtime-asset-digest-invalid");
  }
}
