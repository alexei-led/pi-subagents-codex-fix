export interface KernelProcessIdentity {
  pid: number;
  uniqueId: string;
  pidVersion: number;
}

export interface KernelOperationBinding {
  operationId: string;
  requestDigest: string;
  hostId: string;
  bootId: string;
}

export interface KernelOperationIdentity extends KernelOperationBinding {
  version: 1;
  backend: "darwin-resource-coalition-v1";
  coalitionId: string;
  leader: KernelProcessIdentity;
}

export type KernelExecutionLifetime =
  | { kind: "unbounded" }
  | { kind: "bounded"; timeoutMs: number };

export interface KernelOwnedProcessRequest {
  operationDirectory: string;
  artifactDirectory?: string;
  argv: [string, ...string[]];
  cwd: string;
  env: Record<string, string>;
  lifetime: KernelExecutionLifetime;
}

export interface PreparedKernelOwnedProcess extends KernelOperationBinding {
  operationDirectory: string;
  stdoutPath: string;
  stderrPath: string;
}

export type KernelOwnedProcessProof =
  | (KernelOperationBinding & { kind: "never-started"; observedAt: string })
  | (KernelOperationBinding & {
      kind: "darwin-coalition-retired";
      identity: KernelOperationIdentity;
      observedAt: string;
    });

export interface KernelOwnedProcessObservation {
  status: "pending" | "never-started" | "active" | "retired" | "unknown";
  operationDirectory: string;
  binding?: KernelOperationBinding;
  identity?: KernelOperationIdentity;
  workloadIdentity?: KernelProcessIdentity;
  exitCode?: number | null;
  signal?: string | null;
  timedOut?: boolean;
  proof?: KernelOwnedProcessProof;
  reason?: string;
}

export interface KernelOwnedProcessHandle extends PreparedKernelOwnedProcess {
  identity?: KernelOperationIdentity;
  workloadIdentity?: KernelProcessIdentity;
  observation: KernelOwnedProcessObservation;
}

export interface KernelOwnedProcessCapability {
  supported: boolean;
  reason?: string;
  hostId?: string;
  bootId?: string;
  nativeExecutable?: string;
}

export function preflightKernelOwnedProcess(options: {
  artifactDirectory: string;
}): Promise<KernelOwnedProcessCapability>;

/** Persist the immutable request before exposing the launch intent to cancellation. */
export function prepareKernelOwnedProcess(
  request: KernelOwnedProcessRequest,
): Promise<PreparedKernelOwnedProcess>;

/** Replay is idempotent for the exact request; an earlier cancellation prevents execution. */
export function launchKernelOwnedProcess(
  request: KernelOwnedProcessRequest,
): Promise<KernelOwnedProcessHandle>;

/** Returned proofs have validated journal, request digest, host, boot, and kernel identity binding. */
export function observeKernelOwnedProcess(
  operationDirectory: string,
): Promise<KernelOwnedProcessObservation>;

/** Write a durable stop request. Safe within the operation; does not claim retirement. */
export function requestKernelOwnedProcessCancellation(operationDirectory: string): Promise<void>;

/** Fence admission, signal known generations, then require kernel coalition retirement. */
export function cancelKernelOwnedProcess(
  operationDirectory: string,
  options?: { deadlineMs?: number },
): Promise<KernelOwnedProcessObservation>;

/** Validate an inherited root against the process's actual kernel coalition and incarnation. */
export function inspectKernelOwnedProcessMembership(
  operationDirectory: string,
  pid?: number,
): Promise<{
  owned: boolean;
  identity?: KernelOperationIdentity;
  processIdentity?: KernelProcessIdentity;
  reason?: string;
}>;
