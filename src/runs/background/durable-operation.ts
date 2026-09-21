import { createHash, randomUUID } from "node:crypto";
import * as fs from "node:fs";
import * as path from "node:path";
import { Type } from "typebox";
import { Compile } from "typebox/compile";
import type { Details, ExecutionLifetime, ExecutionOwnership } from "../../shared/types.ts";

type JsonValue = string | number | boolean | null | JsonValue[] | { [key: string]: JsonValue };
interface OperationResult { text: string; details?: Details; isError?: boolean }
export interface DiagnosticIntent { diagnosticId: string; toolCallId: string; message: string }
interface DiagnosticReceipt { state: "queued" | "cancelled" | "rejected"; reason?: string }
interface OperationAnchor extends OperationIdentity { version: 1; scope: string; scopeCwd?: string; kind: "launch" | "cancel"; requestHash?: string }
const anchorValidator = Compile(Type.Object({ version: Type.Literal(1), operationId: Type.String(), digest: Type.String(), scope: Type.String({ pattern: "^[a-f0-9]{64}$" }), scopeCwd: Type.Optional(Type.String()), kind: Type.Union([Type.Literal("launch"), Type.Literal("cancel")]), requestHash: Type.Optional(Type.String()) }));

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
function publishOnce(file: string, value: OperationIntent | OperationIdentity | OperationResult | DiagnosticIntent | DiagnosticReceipt | OperationAnchor): boolean {
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
	directory: string;
	runId: string;
	readonly operationId: string;
	scopeCwd: string | undefined;
	private root: string;
	private scope: string;
	private anchorPath: string;

	constructor(root: string, cwd: string, operationId: string) {
		this.operationId = operationId;
		this.root = root;
		this.scopeCwd = path.resolve(cwd);
		this.scope = hash(this.scopeCwd);
		this.directory = path.join(root, ".operations", this.scope, hash(operationId));
		this.runId = hash(`${this.scope}:${operationId}`).slice(0, 32);
		this.anchorPath = path.join(root, ".operation-index", `${hash(operationId)}.json`);
		this.resolveScope();
	}

	private resolveScope(): OperationAnchor | undefined {
		const anchor = read(this.anchorPath);
		if (anchor !== undefined) {
			if (!anchorValidator.Check(anchor) || anchor.operationId !== this.operationId || (anchor.scopeCwd !== undefined && hash(anchor.scopeCwd) !== anchor.scope)) throw new Error("Invalid operation scope anchor; ownership is unknown.");
			this.scope = anchor.scope;
			this.scopeCwd = anchor.scopeCwd;
		} else {
			let scopes: string[] = [];
			try { scopes = fs.readdirSync(path.join(this.root, ".operations")); } catch (cause) { if (!hasErrorCode(cause, "ENOENT")) throw cause; }
			const matches = scopes.filter((scope) => /^[a-f0-9]{64}$/.test(scope) && fs.existsSync(path.join(this.root, ".operations", scope, hash(this.operationId), "intent.json")));
			if (matches.length > 1) throw new Error("Operation exists in multiple scopes; ownership is ambiguous.");
			if (matches[0] && matches[0] !== this.scope) { this.scope = matches[0]; this.scopeCwd = undefined; }
		}
		this.directory = path.join(this.root, ".operations", this.scope, hash(this.operationId));
		this.runId = hash(`${this.scope}:${this.operationId}`).slice(0, 32);
		return anchorValidator.Check(anchor) ? anchor : undefined;
	}

	private reserve(kind: "launch" | "cancel", digest: string, requestHash?: string): OperationAnchor {
		const existing = this.intent();
		const record: OperationAnchor = { version: 1, operationId: this.operationId, digest: existing?.digest ?? digest, scope: this.scope, kind: existing?.kind ?? kind };
		if (this.scopeCwd) record.scopeCwd = this.scopeCwd;
		const effectiveHash = existing?.requestHash ?? requestHash;
		if (effectiveHash !== undefined) record.requestHash = effectiveHash;
		publishOnce(this.anchorPath, record);
		const anchor = this.resolveScope();
		if (!anchor || anchor.digest !== digest || (kind === "launch" && anchor.requestHash !== undefined && anchor.requestHash !== requestHash)) throw new Error("Operation replay does not match the original scope and launch parameters.");
		return anchor;
	}

	intent(): OperationIntent | undefined {
		const anchor = this.resolveScope();
		const value = read(path.join(this.directory, "intent.json"));
		if (value === undefined) {
			if (!anchor) return undefined;
			const reservation: OperationIntent = { version: 1, kind: anchor.kind, operationId: this.operationId, digest: anchor.digest, runId: this.runId };
			if (anchor.requestHash !== undefined) reservation.requestHash = anchor.requestHash;
			return reservation;
		}
		if (!intentValidator.Check(value) || value.operationId !== this.operationId || value.runId !== this.runId) throw new Error("Invalid durable operation intent; ownership is unknown.");
		return value;
	}

	claim(intent: Omit<OperationIntent, "version" | "kind" | "runId" | "operationId">): boolean {
		const anchor = this.reserve("launch", intent.digest, intent.requestHash);
		if (anchor.kind === "cancel") return false;
		return publishOnce(path.join(this.directory, "intent.json"), { ...intent, version: 1, kind: "launch", runId: this.runId, operationId: this.operationId });
	}

	cancel(digest: string): void {
		this.reserve("cancel", digest);
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

	claimDiagnostic(intent: DiagnosticIntent): boolean {
		const file = path.join(this.directory, "diagnostics", hash(intent.diagnosticId), "intent.json");
		const claimed = publishOnce(file, intent);
		if (!claimed && JSON.stringify(canonical(read(file) ?? null)) !== JSON.stringify(canonical({ ...intent }))) throw new Error("Diagnostic replay parameters do not match the durable intent.");
		return claimed;
	}

	diagnosticReceipt(diagnosticId: string): JsonValue | undefined {
		return read(path.join(this.directory, "diagnostics", hash(diagnosticId), "receipt.json"));
	}

	completeDiagnostic(diagnosticId: string, receipt: DiagnosticReceipt): void {
		publishOnce(path.join(this.directory, "diagnostics", hash(diagnosticId), "receipt.json"), receipt);
	}
}
