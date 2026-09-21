# Lifetime and durable RPC operations

`executionLifetime` is an explicit launch contract:

```ts
type ExecutionLifetime =
  | { mode: "unbounded" }
  | { mode: "bounded"; timeoutMs: number };
```

An unbounded launch creates no elapsed execution deadline. The mode propagates to
workflow children, detached runners, nested Pi children, and recovery descriptors.
An omitted field preserves existing agent/configuration/default timeout behavior.
Explicit bounded timeouts must be positive integers within Node's timer range.
Control requests and explicitly budgeted host commands retain their own timeouts.

RPC `ping` advertises `executionLifetime: { version: 1, modes: ["unbounded",
"bounded"] }`, `durableOperations: { version: 1, lookup: true, replay: true,
cancelFence: true, scope: "repository" }`, and `workflowTerminalProof: { version: 1 }`.

Request kernel-owned execution with `executionOwnership: { mode: "kernel" }`.
On a supported macOS host, bounded preflight verifies the resource-coalition
backend before advertising `processTreeOwnership` with scope `"owned-process-tree"`,
escaped descendants `"contained"`, request mode `"kernel"`, and routes
`["single-async", "parallel-data"]`. Other hosts retain the weaker process-group
capability. A requested kernel-owned launch never falls back to a process group.

The single route accepts async `agent`/`task` parameters. The parallel route accepts
`ownedWorkflow: { version: 1, kind: "parallel", tasks, concurrency }`. Tasks are
structured launch data with stable keys. The trusted runner executes them inside
one owned root. Arbitrary workflow scripts, host commands, remote agents, external
job providers, and top-level foreground requests are unsupported by this strict
route. Nested processes inherit the root only after actual kernel membership has
been verified.

## Correlated launches

Send `operationId` and `digest` together on `spawn`, alongside the ordinary launch
parameters. Persist these values before sending the request. The runtime records
an immutable intent and allocates the run ID before dispatch. Repeating the same
identity returns the saved response or a pending observation. It never dispatches
another child. Changing the digest or launch parameters is rejected.

`lookup({ operationId, digest })` works across sessions in the same repository
scope. Its response contains `state` (`absent`, `pending`, `found`, or `cancelled`),
the identity, `runId`, `asyncDir`, and `effectiveExecutionLifetime` when known.
`statusPayload` contains the persisted native status, including workflow output
and child steps. `activity` exposes recorded tool/process observations; silence is
not proof of a stalled or exited child.

Intent records live under the working directory's `.pi/subagent-runtime/`, outside
the temporary runner directories. The scope is the extension context's working
directory. Kernel admission records and native-to-kernel identity mappings share
the persistent operation directory. An unresolved launch intent remains pending
when no runner evidence exists. A control timeout, missing
status file, or stale session never permits a second dispatch for that identity.

## Cancellation and exit evidence

`cancel({ operationId, digest })` persists a fence before requesting cancellation.
Replays cannot bypass the fence. `cancellationRequested: true` acknowledges the
request only. `neverStarted: true` is returned only when cancellation won the
atomic launch arbitration before dispatch. Otherwise callers must observe exit
evidence. Repeated lookup/cancel requests retry stop delivery when startup races
with cancellation.

For kernel-owned launches, observed `processTerminalProof` includes the full
`processTreeOwnership` descriptor, `nativeOperation: { operationId, digest }`,
the prepared `kernelBinding`, and the verified retired observation as `kernelProof`.
Native operation identity and kernel request digest are different namespaces.
The runtime checks their persisted mapping; adapters must preserve their own
caller-to-native mapping. Retirement requires the same host, boot, and previously
admitted coalition. Absence, silence, and empty group snapshots are insufficient.

Healthy descendants may outlive the runner and drain naturally without a deadline.
Explicit cancellation, bounded lifetime expiry, or confirmed runner failure
initiates cleanup inside the same owned operation. No replacement writer is
authorized until retirement is proven.

Legacy `processTerminalProof` preserves runner close and process-group evidence.
It is not inferred from the wrapper's terminal state. In-host workflows execute in the
host process, so their separate `workflowTerminalProof` requires a durable closed
dispatch record and full process-tree exit evidence for every detached child. Its
observed form is `{ version: 1, kind: "workflow", state: "observed", runId,
dispatchClosed: true, observedAt, children }`. Missing child identities, foreground
children without durable runner evidence, or an interrupted host without a
dispatch-closed record leave proof pending or unknown. Current POSIX writer
observations carry `containment: "unverified"` and cannot produce an observed
workflow handoff proof. `async: true` children retain durable runner observations,
but this does not remove the containment limitation.

Neither proof authorizes treating a failed task as successful. They establish
quiescence so the caller can safely decide what to run next.
