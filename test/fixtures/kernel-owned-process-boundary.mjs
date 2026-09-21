#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const [command, ...args] = process.argv.slice(2);
if (command === "admission-race") {
  const [directory, storeUrl] = args;
  const { publishRecord } = await import(storeUrl);
  const candidate = JSON.parse(fs.readFileSync(path.join(directory, "candidate.json"), "utf8"));
  process.stdout.write("ready\n");
  const deadline = Date.now() + 2500;
  while (!fs.existsSync(path.join(directory, "release"))) {
    if (Date.now() >= deadline) throw new Error("fixture release deadline exceeded");
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
  process.stdout.write(
    `${JSON.stringify({ won: publishRecord(path.join(directory, "decision.json"), candidate) })}\n`,
  );
} else {
  const directory = path.dirname(fileURLToPath(import.meta.url));
  const state = JSON.parse(fs.readFileSync(path.join(directory, "boundary-state.json"), "utf8"));
  fs.appendFileSync(
    path.join(directory, "boundary-calls.jsonl"),
    `${JSON.stringify({ command, args })}\n`,
    { mode: 0o600 },
  );
  if (state.delayCommand === command)
    await new Promise((resolve) => setTimeout(resolve, Math.min(state.delayMs, 2500)));
  let result;
  switch (command) {
    case "host":
      result = {
        ok: true,
        platform: "darwin",
        abi: 1,
        hostId: state.hostId,
        bootId: state.bootId,
        monotonicNs: state.monotonicNs,
      };
      break;
    case "coalition":
      for (const name of state.retirementRecords ?? [])
        fs.renameSync(path.join(directory, `pending-${name}.json`), path.join(directory, `${name}.json`));
      result = state.coalition;
      break;
    case "members":
      result = { ok: true, members: state.members, incomplete: false };
      break;
    case "inspect":
      result = {
        ok: true,
        identity: { pid: Number(args[0]), uniqueId: "9000", pidVersion: 2 },
        coalitionId: "4000",
      };
      break;
    case "signal":
      result = { ok: true };
      break;
    default:
      throw new Error("unexpected native fixture command");
  }
  process.stdout.write(`${JSON.stringify(result)}\n`);
  process.exitCode = result.ok ? 0 : 1;
}
