import fs from "node:fs";
import path from "node:path";
import { syncBuiltinESMExports } from "node:module";
import { fileURLToPath } from "node:url";
import { createSubagentExecutor } from "../../src/runs/foreground/subagent-executor.ts";
import { registerSubagentRpcBridge, SUBAGENT_RPC_REQUEST_EVENT, subagentRpcReplyEvent } from "../../src/extension/rpc.ts";
import { setChildSessionFactoryModule } from "../../src/runs/shared/child-session.ts";
import { createEventBus, makeAgent, makeMinimalCtx } from "../support/helpers.ts";

const specification = JSON.parse(fs.readFileSync(process.argv[2], "utf8"));
const events = createEventBus();
const ctx = makeMinimalCtx(specification.cwd);
const executor = createSubagentExecutor({
  pi: { events: createEventBus(), getSessionName: () => undefined },
  state: { baseCwd: specification.cwd, currentSessionId: null, asyncJobs: new Map(), foregroundControls: new Map(), lastForegroundControlId: null },
  config: {}, asyncByDefault: false, tempArtifactsDir: specification.cwd,
  getSubagentSessionRoot: () => specification.cwd, expandTilde: value => value,
  discoverAgents: () => ({ agents: [makeAgent("worker")] }),
});
setChildSessionFactoryModule(fileURLToPath(new URL("../support/runner-child-session-factory.ts", import.meta.url)));
const rename = fs.renameSync;
fs.renameSync = (from, to) => {
  rename(from, to);
  if (specification.cut === "prepared" && path.basename(to) === "native-run.json") process.exit(73);
};
const link = fs.linkSync;
fs.linkSync = (from, to) => {
  link(from, to);
  if (specification.cut === "anchor" && path.basename(path.dirname(to)) === ".operation-index") process.exit(73);
  if (specification.cut === "dispatch" && path.basename(to) === "dispatch-decision.json") process.exit(73);
};
syncBuiltinESMExports();
registerSubagentRpcBridge({ events, operationDirRoot: specification.root, getContext: () => ctx, execute: async (...args) => {
  if (specification.cut === "claim") process.exit(73);
  if (specification.cut === "live-owner") {
    fs.writeFileSync(path.join(specification.root, "owner-ready"), "ready");
    while (!fs.existsSync(path.join(specification.root, "release-owner"))) await new Promise(resolve => setTimeout(resolve, 20));
  }
  return executor.executePublic(...args);
} });
events.on(subagentRpcReplyEvent("crash-launch"), value => { process.stderr.write(JSON.stringify(value)); process.exit(specification.cut === "live-owner" && value.success && !value.data.isError ? 0 : 74); });
events.emit(SUBAGENT_RPC_REQUEST_EVENT, { version: 1, requestId: "crash-launch", method: "spawn", params: specification.params });
