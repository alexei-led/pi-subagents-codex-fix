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
Explicit unbounded mode also suppresses built-in, agent, configuration, and
environment tool-timeout defaults. A deliberate `toolTimeoutMs` on the launch
call remains an explicit command budget; control RPC timeouts remain bounded.

RPC `ping` advertises `executionLifetime: { version: 1, modes: ["unbounded",
"bounded"] }`, `durableOperations: { version: 1, lookup: true, replay: true,
cancelFence: true, scope: "runtime" }`, and `workflowTerminalProof: { version: 1 }`.

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

Owned parallel launches honor the effective worktree setting and the `isolation`
alias. Explicit false or `isolation: "none"` overrides configuration defaults.
Allocation and setup hooks run inside the owned root; each isolated child receives
its own cwd, while root status retains the source cwd and the handoff records child
paths, branches, and base commit. An omitted base ref resolves the source HEAD.
Explicit unbounded execution disables the implicit setup-hook deadline; an
explicitly configured hook command budget remains enforced.

The inherited environment marker is a JSON descriptor containing the operation
directory and its operation ID, request digest, host ID, and boot ID. Nested
launches validate every binding field and the current process's actual coalition
membership before using the existing root.

Recovery descriptors retain kernel ownership and the known operation directory.
Ordinary revival of owned runs is rejected before session leases or runners are
created, including retained workflow resumes and inherited foreground children.
Missing descriptor fields cannot override ownership in status, indexed results,
or kernel proof; malformed ownership evidence blocks revival. Continue with a
fresh correlated owned operation after verifying predecessor retirement. Live
follow-up control of an existing child remains available.

Strict workers remove inherited Git repository selectors and injected per-command
configuration before the kernel request is frozen. Author, committer, SSH, and
authentication settings remain available. The parent environment is unchanged.

## Correlated launches

Send `operationId` and `digest` together on `spawn`, alongside the ordinary launch
parameters. Persist these values before sending the request. The runtime records
an immutable intent and allocates the run ID before dispatch. Repeating the same
identity returns the saved response or a pending observation. It never dispatches
another child. Changing the digest or launch parameters is rejected.

`lookup({ operationId, digest })` works across sessions and working-directory
changes within the same private runtime store. Its response contains `state`
(`absent`, `pending`, `found`, or `cancelled`),
the identity, `runId`, `asyncDir`, and `effectiveExecutionLifetime` when known.
`statusPayload` contains the persisted native status, including workflow output
and child steps. `activity` exposes recorded tool/process observations; silence is
not proof of a stalled or exited child.

Intent records live in the configured private operation root, defaulting to the
agent state directory's `subagent-runtime/`. Compiler caches live beside private
operation artifacts. Neither is written into the candidate checkout. A global
operation-ID reservation freezes the original working-directory scope and run ID;
changing context cannot make a prior launch disappear or authorize a duplicate.
Kernel admission records and native-to-kernel identity mappings share
the persistent operation directory. An unresolved launch intent remains pending
when no runner evidence exists. A control timeout, missing
status file, or stale session never permits a second dispatch for that identity.

Admission may remain pending after the bounded startup acknowledgement wait.
Lookup reconciles the same prepared operation and startup permission while its
durable cancellation fence remains absent. Explicit unbounded runners have no
startup-permission deadline. A delayed admission does not become a failed task
merely because the service acknowledgement expired.

Direct consumers of `pi-subagents/kernel-owned-process` can call
`reconcileKernelOwnedProcess(operationDirectory): Promise<KernelOwnedProcessObservation>`
to retry admission for an already authorized immutable request. They must check
their own stop/pause fences first. Reconciliation preserves the prepared identity
and honors the kernel cancellation marker; `observeKernelOwnedProcess` alone
does not initiate admission.

## Cancellation and exit evidence

`cancel({ operationId, digest })` persists a fence before requesting cancellation.
Replays cannot bypass the fence. `cancellationRequested: true` acknowledges the
request only. `neverStarted: true` is returned only when cancellation won the
atomic launch arbitration before dispatch. Otherwise callers must observe exit
evidence. Repeated lookup/cancel requests retry stop delivery when startup races
with cancellation.

New kernel RPC launches arbitrate dispatch against rejection using one durable,
identity-bound decision. The runtime publishes the recoverable kernel request and
native mapping before claiming dispatch. Early validation and startup rejection
can report `neverStarted: true` only after atomically fencing that gate; late and
concurrent continuations cannot bypass it. Once dispatch wins, generic errors and
missing files do not establish no-start evidence.

Recovery resumes a prepared request under its original identity. A bare claim can
be fenced automatically only when the recorded launcher's host, boot, and kernel
process incarnation prove that owner has exited or been replaced. Another host's
lookup leaves a live owner pending. Explicit cancellation can fence a pending gate
immediately. Correlated kernel RPC roots require an external host; native nested
delegation continues inside its validated inherited root.

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

Verified retirement settles unfinished runner status even if the helper could not
publish an exit receipt. The run and unfinished steps become failed; confirmed
budget expiry records `execution_lifetime_expired`. Explicit cancellation retains
stopped status and takes precedence over that classification. Completed output is
preserved, and no exit code or signal is invented when its receipt is missing.

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

## Confirmed tool-failure guidance

Native SDK error events persist `activity.lastToolFailure` with the exact tool call
ID, tool name, observation time, and a sanitized message. Successful output that
contains error-like text and elapsed silence do not create this evidence.

`diagnose({ operationId, digest, diagnosticId, toolCallId, message })` can enqueue
follow-up guidance for that confirmed failure in a live child session. It never
revives a session, interrupts a healthy tool, or launches another worker. The
durable diagnostic ID permits at most one enqueue. Replays return `queued`,
`pending`, `cancelled`, or `rejected` with `guidanceOnly: true`; changed payloads
are rejected. A crash between claim and acknowledgement remains pending and does
not cause a repeated enqueue. Queued guidance is not evidence that the tool fault
was repaired. Cancellation is checked again immediately before enqueue.
