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

Full autonomous handoff is not supported yet. `processTreeOwnership` advertises
`{ version: 1, scope: "posix-process-group", escapedDescendants: "unverified" }`.
A client requiring containment of every descendant must reject this capability
before dispatch. An empty POSIX group does not prove that a child did not escape
and become reparented before the runner's observations.

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
directory. An unresolved launch
intent remains pending when no runner evidence exists. A control timeout, missing
status file, or stale session never permits a second dispatch for that identity.

## Cancellation and exit evidence

`cancel({ operationId, digest })` persists a fence before requesting cancellation.
Replays cannot bypass the fence. `cancellationRequested: true` acknowledges the
request only. `neverStarted: true` is returned only when cancellation won the
atomic launch arbitration before dispatch. Otherwise callers must observe exit
evidence. Repeated lookup/cancel requests retry stop delivery when startup races
with cancellation.

`processTerminalProof` preserves native runner close and process-group evidence.
It is not inferred from the wrapper's terminal state. Workflows execute in the
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
