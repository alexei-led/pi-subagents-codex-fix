import assert from "node:assert/strict";
import test from "node:test";
import { stripParentOnlySubagentMessages } from "../../src/runs/shared/subagent-prompt-runtime.ts";

const CODEX_TOOL_ID_LIMIT = 64;
const COMPOSITE_TOOL_ID =
  "call_N7iYNRPXLl9czpXh3bDyMpIL|fc_0e76718634eca88f016a76fdc89aec81919763fa7858f67a0d";

test("sanitizes composite tool IDs to Codex's 64-character limit consistently", () => {
  const messages = [
    {
      role: "assistant",
      content: [
        {
          type: "toolCall",
          id: COMPOSITE_TOOL_ID,
          name: "bash",
          arguments: { command: "true" },
        },
      ],
    },
    {
      role: "toolResult",
      toolCallId: COMPOSITE_TOOL_ID,
      toolName: "bash",
      isError: false,
      content: [{ type: "text", text: "ok" }],
    },
  ];

  const sanitized = stripParentOnlySubagentMessages(messages) as Array<{
    role: string;
    content?: Array<{ id?: string }>;
    toolCallId?: string;
  }>;
  const toolCallId = sanitized[0]?.content?.[0]?.id;
  const toolResultId = sanitized[1]?.toolCallId;

  assert.ok(toolCallId);
  assert.match(toolCallId, /^[A-Za-z0-9_-]+$/);
  assert.ok(toolCallId.length <= CODEX_TOOL_ID_LIMIT);
  assert.equal(toolResultId, toolCallId);
});

test("keeps a portable tool ID at the provider limit unchanged", () => {
  const toolCallId = "a".repeat(CODEX_TOOL_ID_LIMIT);
  const messages = [
    {
      role: "assistant",
      content: [{ type: "toolCall", id: toolCallId, name: "bash" }],
    },
  ];

  const sanitized = stripParentOnlySubagentMessages(messages) as Array<{
    content?: Array<{ id?: string }>;
  }>;

  assert.equal(sanitized[0]?.content?.[0]?.id, toolCallId);
});
