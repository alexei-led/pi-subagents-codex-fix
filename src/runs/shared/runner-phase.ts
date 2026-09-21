import type { RunnerPhase } from "../../shared/types.ts";

/** Phase evidence comes from child protocol events, never elapsed silence. */
export function observeRunnerPhase(event: { type?: string; message?: { role?: string } }, current: RunnerPhase = "unknown", activeTool?: { awaitingInput: boolean }): RunnerPhase {
	if (activeTool) return activeTool.awaitingInput ? "awaiting_input" : "tool_in_flight";
	if (event.type === "message_update") return "model_stream";
	if (event.type === "turn_start" || event.type === "agent_start" || (event.type === "message_start" && event.message?.role === "assistant")) return "model_request";
	if (event.type === "tool_execution_end" || event.type === "agent_end") return "unknown";
	return current;
}
