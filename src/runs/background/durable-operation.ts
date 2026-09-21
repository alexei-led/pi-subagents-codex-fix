import { createHash, randomUUID } from "node:crypto";
import * as fs from "node:fs";
import * as path from "node:path";
import { Type } from "typebox";
import { Compile } from "typebox/compile";
import type { Details, ExecutionLifetime, ExecutionOwnership } from "../../shared/types.ts";

type JsonValue = string | number | boolean | null | JsonValue[] | { [key: string]: JsonValue };
interface OperationResult { text: string; details?: Details; isError?: boolean }

export interface OperationIdentity {
	operationId: string;
	digest: string;
}

export interface OperationIntent extends OperationIdentity {
	version: 1;
	kind: "launch" | "cancel";
	runId: string;
	sessionId?: string;
	requestHash?: string;
	effectiveExecutionLifetime?: ExecutionLifetime;
	effectiveExecutionOwnership?: ExecutionOwnership;
	executionRoute?: "single-async" | "parallel-data";
	ownedWorkflowKeys?: string[];
}

const intentValidator = Compile(Type.Object({
	version: Type.Literal(1), operationId: Type.String(), digest: Type.String(),
	kind: Type.Union([Type.Literal("launch"), Type.Literal("cancel")]), runId: Type.String(),
	sessionId: Type.Optional(Type.String()), requestHash: Type.Optional(Type.String()),
	effectiveExecutionOwnership: Type.Optional(Type.Object({ mode: Type.Literal("kernel") })),
	executionRoute: Type.Optional(Type.Union([Type.Literal("single-async"), Type.Literal("parallel-data")])),
	ownedWorkflowKeys: Type.Optional(Type.Array(Type.String())),
	effectiveExecutionLifetime: Type.Optional(Type.Union([
		Type.Object({ mode: Type.Literal("unbounded") }),
		Type.Object({ mode: Type.Literal("bounded"), timeoutMs: Type.Integer({ minimum: 1, maximum: 2_147_483_647 }) }),
	])),
}));

function hasErrorCode(cause: unknown, code: string): boolean {
	return cause instanceof Error && "code" in cause && cause.code === code;
}

function hash(value: string): string {
	return createHash("sha256").update(value).digest("hex");
}

function canonical(value: JsonValue): JsonValue {
	if (Array.isArray(value)) return value.map(canonical);
	if (value !== null && Object(value) === value) return Object.fromEntries(Object.entries(value).sort(([a], [b]) => a.localeCompare(b)).map(([key, entry]) => [key, canonical(entry)]));
	return value;
}

export function operationRequestHash(params: import("../foreground/subagent-executor.ts").SubagentParamsLike): string {
	const value: JsonValue = JSON.parse(JSON.stringify(params));
	return hash(JSON.stringify(canonical(value)));
}

/** An immutable, fsynced file published without replacing a concurrent winner. */
function publishOnce(file: string, value: OperationIntent | OperationIdentity | OperationResult): boolean {
	fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
	const temporary = `${file}.${randomUUID()}.tmp`;
	const fd = fs.openSync(temporary, "wx", 0o600);
	try {
		fs.writeFileSync(fd, JSON.stringify(value));
		fs.fsyncSync(fd);
	} finally {
		fs.closeSync(fd);
	}
	try {
		fs.linkSync(temporary, file);
		if (process.platform !== "win32") {
			const directory = fs.openSync(path.dirname(file), "r");
			try { fs.fsyncSync(directory); } finally { fs.closeSync(directory); }
		}
		return true;
	} catch (error) {
		if (hasErrorCode(error, "EEXIST")) return false;
		throw error;
	} finally {
		fs.unlinkSync(temporary);
	}
}

function read(file: string): JsonValue | undefined {
	try { return JSON.parse(fs.readFileSync(file, "utf8")); }
	catch (error) {
		if (hasErrorCode(error, "ENOENT")) return undefined;
		throw error;
	}
}

/** Durable launch arbitration; an unresolved launch is never implicitly retried. */
export class DurableOperation {
	readonly directory: string;
	readonly runId: string;
	readonly operationId: string;

	constructor(root: string, cwd: string, operationId: string) {
		this.operationId = operationId;
		const scope = hash(path.resolve(cwd));
		this.directory = path.join(root, ".operations", scope, hash(operationId));
		this.runId = hash(`${scope}:${operationId}`).slice(0, 32);
	}

	intent(): OperationIntent | undefined {
		const value = read(path.join(this.directory, "intent.json"));
		if (value === undefined) return undefined;
		if (!intentValidator.Check(value) || value.operationId !== this.operationId || value.runId !== this.runId) throw new Error("Invalid durable operation intent; ownership is unknown.");
		return value;
	}

	claim(intent: Omit<OperationIntent, "version" | "kind" | "runId" | "operationId">): boolean {
		return publishOnce(path.join(this.directory, "intent.json"), { ...intent, version: 1, kind: "launch", runId: this.runId, operationId: this.operationId });
	}

	cancel(digest: string): void {
		const existing = this.intent();
		if (existing && existing.digest !== digest) throw new Error("Operation digest does not match its durable intent.");
		publishOnce(path.join(this.directory, "intent.json"), { version: 1, kind: "cancel", operationId: this.operationId, digest, runId: this.runId });
		if (this.intent()?.digest !== digest) throw new Error("Operation digest does not match its durable intent.");
		publishOnce(path.join(this.directory, "cancel.json"), { operationId: this.operationId, digest });
	}

	cancelled(): boolean {
		return this.intent()?.kind === "cancel" || fs.existsSync(path.join(this.directory, "cancel.json"));
	}

	result(): JsonValue | undefined { return read(path.join(this.directory, "result.json")); }
	complete(result: OperationResult): void { publishOnce(path.join(this.directory, "result.json"), result); }
}
