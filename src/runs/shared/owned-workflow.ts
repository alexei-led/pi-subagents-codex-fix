import { Type, type Static } from "typebox";
import { Compile } from "typebox/compile";
import type { AcceptanceInput, ExecutionLifetime, ExecutionOwnership, OutputMode } from "../../shared/types.ts";
import type { SubagentParamsLike } from "../foreground/subagent-executor.ts";
import { validateAcceptanceInput } from "./acceptance.ts";
import { validateToolBudgetConfig } from "./tool-budget.ts";

export const ExecutionOwnershipSchema = Type.Unsafe<ExecutionOwnership>({
	type: "object", properties: { mode: { type: "string", enum: ["kernel"] } }, required: ["mode"], additionalProperties: false,
});
const lifetimeSchema = Type.Unsafe<ExecutionLifetime>({
	anyOf: [
		{ type: "object", properties: { mode: { type: "string", enum: ["unbounded"] } }, required: ["mode"], additionalProperties: false },
		{ type: "object", properties: { mode: { type: "string", enum: ["bounded"] }, timeoutMs: { type: "integer", minimum: 1, maximum: 2_147_483_647 } }, required: ["mode", "timeoutMs"], additionalProperties: false },
	],
});
const ownedTaskSchema = Type.Object({
	key: Type.String({ minLength: 1, maxLength: 128, pattern: "^[A-Za-z0-9][A-Za-z0-9._-]*$" }),
	agent: Type.String({ minLength: 1, maxLength: 256, pattern: "\\S" }),
	task: Type.String({ minLength: 1, maxLength: 1_048_576, pattern: "\\S" }),
	model: Type.Optional(Type.String({ minLength: 1, maxLength: 512, pattern: "\\S" })),
	skill: Type.Optional(Type.Union([Type.String(), Type.Array(Type.String(), { maxItems: 256 }), Type.Boolean()])),
	toolBudget: Type.Optional(Type.Object({ soft: Type.Optional(Type.Integer({ minimum: 1 })), hard: Type.Integer({ minimum: 1 }), block: Type.Optional(Type.Union([Type.Array(Type.String({ minLength: 1 }), { minItems: 1 }), Type.Unsafe<"*">({ type: "string", enum: ["*"] })])) }, { additionalProperties: false })),
	executionLifetime: Type.Optional(lifetimeSchema),
	output: Type.Optional(Type.Union([Type.String(), Type.Boolean()])),
	outputMode: Type.Optional(Type.Unsafe<OutputMode>({ type: "string", enum: ["inline", "file-only"] })),
	progress: Type.Optional(Type.Boolean()),
	acceptance: Type.Optional(Type.Unsafe<AcceptanceInput>({ anyOf: [{ type: "string" }, { type: "boolean" }, { type: "object", additionalProperties: true }] })),
}, { additionalProperties: false });

export const OwnedWorkflowSchema = Type.Object({
	version: Type.Integer({ minimum: 1, maximum: 1 }),
	kind: Type.Unsafe<"parallel">({ type: "string", enum: ["parallel"] }),
	tasks: Type.Array(ownedTaskSchema, { minItems: 1, maxItems: 64 }),
	concurrency: Type.Integer({ minimum: 1, maximum: 64 }),
}, { additionalProperties: false });

export type OwnedWorkflow = Static<typeof OwnedWorkflowSchema>;
const workflowValidator = Compile(OwnedWorkflowSchema);
const ownershipValidator = Compile(ExecutionOwnershipSchema);
type OwnedWorkflowParseResult = { ok: true; tasks: NonNullable<SubagentParamsLike["tasks"]>; concurrency: number; keys: string[] } | { ok: false; error: string };

// oxlint-disable-next-line anti-slop/no-unknown-parameters -- This is the schema parser for untrusted owned-workflow requests.
export function parseOwnedWorkflow(value: unknown): OwnedWorkflowParseResult {
	if (!workflowValidator.Check(value)) return { ok: false, error: "ownedWorkflow requires version:1, kind:'parallel', 1-64 keyed tasks, and concurrency:1-64; unsupported fields are rejected." };
	const keys = value.tasks.map((task) => task.key);
	if (new Set(keys).size !== keys.length) return { ok: false, error: "ownedWorkflow task keys must be unique." };
	for (const task of value.tasks) {
		const acceptanceErrors = validateAcceptanceInput(task.acceptance, `ownedWorkflow.tasks[${task.key}].acceptance`);
		if (acceptanceErrors.length) return { ok: false, error: acceptanceErrors.join(" ") };
		const budget = validateToolBudgetConfig(task.toolBudget, `ownedWorkflow.tasks[${task.key}].toolBudget`);
		if (budget.error) return { ok: false, error: budget.error };
	}
	return { ok: true, tasks: value.tasks.map(({ key: _key, ...task }) => task), concurrency: value.concurrency, keys };
}

// oxlint-disable-next-line anti-slop/no-unknown-parameters -- This is the ownership contract validation boundary.
export function validateExecutionOwnership(value: unknown): string | undefined {
	if (value !== undefined && !ownershipValidator.Check(value)) return "executionOwnership must be { mode: 'kernel' }.";
	return undefined;
}
