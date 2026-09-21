import { spawn } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  inspectKernelOwnedProcessMembership,
  launchKernelOwnedProcess,
} from "../../src/api/kernel-owned-process.mjs";

const [mode, output] = process.argv.slice(2);
const ownFile = fileURLToPath(import.meta.url);
const spawnDetached = (next) => {
  const child = spawn(process.execPath, [ownFile, next, output], {
    detached: true,
    stdio: "ignore",
    env: process.env,
  });
  child.unref();
};

if (mode === "natural") spawnDetached("middle");
else if (mode === "middle") spawnDetached("delayed");
else if (mode === "delayed") setTimeout(() => fs.writeFileSync(output, "detached-completed"), 350);
else if (mode === "linger-root") spawnDetached("linger");
else if (mode === "linger") {
  fs.writeFileSync(output, String(process.pid));
  setTimeout(() => {}, 8000);
} else if (mode === "counter") fs.appendFileSync(output, "executed\n");
else if (mode === "membership") {
  const marker = JSON.parse(process.env.PI_KERNEL_OWNED_OPERATION);
  const membership = await inspectKernelOwnedProcessMembership(marker.operationDirectory);
  let nestedRejected = false;
  try {
    await launchKernelOwnedProcess({
      operationDirectory: path.join(path.dirname(output), "nested"),
      argv: [process.execPath, "-e", "process.exit(0)"],
      cwd: process.cwd(),
      env: {},
      lifetime: { kind: "unbounded" },
    });
  } catch {
    nestedRejected = true;
  }
  fs.writeFileSync(output, JSON.stringify({ membership, nestedRejected }));
} else throw new Error("unknown-fixture-mode");
