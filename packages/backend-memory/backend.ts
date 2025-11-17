import {
  Backend,
  CancelWorkflowRunParams,
  ClaimWorkflowRunParams,
  CreateStepAttemptParams,
  CreateWorkflowRunParams,
  DEFAULT_NAMESPACE_ID,
  DEFAULT_RETRY_POLICY,
  GetStepAttemptParams,
  GetWorkflowRunParams,
  HeartbeatWorkflowRunParams,
  JsonValue,
  ListStepAttemptsParams,
  MarkStepAttemptFailedParams,
  MarkStepAttemptSucceededParams,
  MarkWorkflowRunFailedParams,
  MarkWorkflowRunSucceededParams,
  SleepWorkflowRunParams,
  StepAttempt,
  WorkflowRun,
} from "openworkflow";
import { randomUUID } from "node:crypto";

interface BackendMemoryOptions {
  namespaceId?: string;
}

/**
 * In-memory backend for OpenWorkflow. Stores all workflow runs and step
 * attempts in memory without requiring any database.
 *
 * This backend is suitable for development, testing, and scenarios where
 * clients maintain workflow state (e.g., sending JSON patches via HTTP SSE).
 *
 * Note: All data is lost when the process exits.
 */
export class BackendMemory implements Backend {
  private namespaceId: string;
  private workflowRuns = new Map<string, WorkflowRun>();
  private stepAttempts = new Map<string, StepAttempt>();
  // Track which steps belong to which workflow runs for efficient querying
  private workflowRunSteps = new Map<string, Set<string>>();
  // Lock for atomic claim operations
  private claimLock: Promise<unknown> = Promise.resolve();

  constructor(options?: BackendMemoryOptions) {
    this.namespaceId = options?.namespaceId ?? DEFAULT_NAMESPACE_ID;
  }

  /**
   * Create a new BackendMemory instance.
   */
  static create(options?: BackendMemoryOptions): BackendMemory {
    return new BackendMemory(options);
  }

  async createWorkflowRun(
    params: CreateWorkflowRunParams,
  ): Promise<WorkflowRun> {
    const id = randomUUID();
    const now = new Date();

    const workflowRun: WorkflowRun = {
      namespaceId: this.namespaceId,
      id,
      workflowName: params.workflowName,
      version: params.version,
      status: "pending",
      idempotencyKey: params.idempotencyKey,
      config: params.config,
      context: params.context,
      input: params.input,
      output: null,
      error: null,
      attempts: 0,
      parentStepAttemptNamespaceId: null,
      parentStepAttemptId: null,
      workerId: null,
      availableAt: params.availableAt ?? now,
      deadlineAt: params.deadlineAt,
      startedAt: null,
      finishedAt: null,
      createdAt: now,
      updatedAt: now,
    };

    this.workflowRuns.set(id, workflowRun);
    this.workflowRunSteps.set(id, new Set());

    return structuredClone(workflowRun);
  }

  async getWorkflowRun(
    params: GetWorkflowRunParams,
  ): Promise<WorkflowRun | null> {
    const workflowRun = this.workflowRuns.get(params.workflowRunId);

    if (!workflowRun || workflowRun.namespaceId !== this.namespaceId) {
      return null;
    }

    return structuredClone(workflowRun);
  }

  async claimWorkflowRun(
    params: ClaimWorkflowRunParams,
  ): Promise<WorkflowRun | null> {
    // Use a lock to ensure atomic claim operation
    const result = await (this.claimLock = this.claimLock.then(async () => {
      const now = new Date();

      // First, mark any deadline-expired workflow runs as failed
      for (const [id, run] of this.workflowRuns.entries()) {
        if (
          run.namespaceId === this.namespaceId &&
          ["pending", "running", "sleeping"].includes(run.status) &&
          run.deadlineAt &&
          run.deadlineAt <= now
        ) {
          this.workflowRuns.set(id, {
            ...run,
            status: "failed",
            error: { message: "Workflow run deadline exceeded" },
            workerId: null,
            availableAt: null,
            finishedAt: now,
            updatedAt: now,
          });
        }
      }

      // Find an available workflow run to claim
      // Sort by: pending first, then by availableAt, then by createdAt
      const candidates = Array.from(this.workflowRuns.values())
        .filter(
          (run) =>
            run.namespaceId === this.namespaceId &&
            ["pending", "running", "sleeping"].includes(run.status) &&
            run.availableAt &&
            run.availableAt <= now &&
            (!run.deadlineAt || run.deadlineAt > now),
        )
        .sort((a, b) => {
          // Pending runs first
          if (a.status === "pending" && b.status !== "pending") return -1;
          if (a.status !== "pending" && b.status === "pending") return 1;

          // Then by availableAt
          const aTime = a.availableAt?.getTime() ?? 0;
          const bTime = b.availableAt?.getTime() ?? 0;
          if (aTime !== bTime) return aTime - bTime;

          // Then by createdAt
          return a.createdAt.getTime() - b.createdAt.getTime();
        });

      if (candidates.length === 0) {
        return null;
      }

      const candidate = candidates[0];
      const leaseDuration = new Date(now.getTime() + params.leaseDurationMs);

      const claimed: WorkflowRun = {
        ...candidate,
        status: "running",
        attempts: candidate.attempts + 1,
        workerId: params.workerId,
        availableAt: leaseDuration,
        startedAt: candidate.startedAt ?? now,
        updatedAt: now,
      };

      this.workflowRuns.set(candidate.id, claimed);

      return structuredClone(claimed);
    }) as Promise<WorkflowRun | null>);

    return result;
  }

  async heartbeatWorkflowRun(
    params: HeartbeatWorkflowRunParams,
  ): Promise<WorkflowRun> {
    const workflowRun = this.workflowRuns.get(params.workflowRunId);

    if (
      !workflowRun ||
      workflowRun.namespaceId !== this.namespaceId ||
      workflowRun.status !== "running" ||
      workflowRun.workerId !== params.workerId
    ) {
      throw new Error("Failed to heartbeat workflow run");
    }

    const now = new Date();
    const leaseDuration = new Date(now.getTime() + params.leaseDurationMs);

    const updated: WorkflowRun = {
      ...workflowRun,
      availableAt: leaseDuration,
      updatedAt: now,
    };

    this.workflowRuns.set(params.workflowRunId, updated);

    return structuredClone(updated);
  }

  async sleepWorkflowRun(params: SleepWorkflowRunParams): Promise<WorkflowRun> {
    const workflowRun = this.workflowRuns.get(params.workflowRunId);

    if (
      !workflowRun ||
      workflowRun.namespaceId !== this.namespaceId ||
      ["succeeded", "failed", "canceled"].includes(workflowRun.status) ||
      workflowRun.workerId !== params.workerId
    ) {
      throw new Error("Failed to sleep workflow run");
    }

    const now = new Date();

    const updated: WorkflowRun = {
      ...workflowRun,
      status: "sleeping",
      availableAt: params.availableAt,
      workerId: null,
      updatedAt: now,
    };

    this.workflowRuns.set(params.workflowRunId, updated);

    return structuredClone(updated);
  }

  async markWorkflowRunSucceeded(
    params: MarkWorkflowRunSucceededParams,
  ): Promise<WorkflowRun> {
    const workflowRun = this.workflowRuns.get(params.workflowRunId);

    if (
      !workflowRun ||
      workflowRun.namespaceId !== this.namespaceId ||
      workflowRun.status !== "running" ||
      workflowRun.workerId !== params.workerId
    ) {
      throw new Error("Failed to mark workflow run succeeded");
    }

    const now = new Date();

    const updated: WorkflowRun = {
      ...workflowRun,
      status: "succeeded",
      output: params.output,
      error: null,
      workerId: params.workerId,
      availableAt: null,
      finishedAt: now,
      updatedAt: now,
    };

    this.workflowRuns.set(params.workflowRunId, updated);

    return structuredClone(updated);
  }

  async markWorkflowRunFailed(
    params: MarkWorkflowRunFailedParams,
  ): Promise<WorkflowRun> {
    const workflowRun = this.workflowRuns.get(params.workflowRunId);

    if (
      !workflowRun ||
      workflowRun.namespaceId !== this.namespaceId ||
      workflowRun.status !== "running" ||
      workflowRun.workerId !== params.workerId
    ) {
      throw new Error("Failed to mark workflow run failed");
    }

    const now = new Date();
    const { initialIntervalMs, backoffCoefficient, maximumIntervalMs } =
      DEFAULT_RETRY_POLICY;

    // Calculate next retry delay
    const backoffMs =
      initialIntervalMs *
      Math.pow(backoffCoefficient, workflowRun.attempts - 1);
    const retryDelayMs = Math.min(backoffMs, maximumIntervalMs);
    const nextRetryAt = new Date(now.getTime() + retryDelayMs);

    // Check if next retry would exceed deadline
    const wouldExceedDeadline =
      workflowRun.deadlineAt && nextRetryAt >= workflowRun.deadlineAt;

    const updated: WorkflowRun = {
      ...workflowRun,
      status: wouldExceedDeadline ? "failed" : "pending",
      availableAt: wouldExceedDeadline ? null : nextRetryAt,
      finishedAt: wouldExceedDeadline ? now : null,
      error: params.error,
      workerId: null,
      startedAt: null,
      updatedAt: now,
    };

    this.workflowRuns.set(params.workflowRunId, updated);

    return structuredClone(updated);
  }

  async cancelWorkflowRun(
    params: CancelWorkflowRunParams,
  ): Promise<WorkflowRun> {
    const workflowRun = this.workflowRuns.get(params.workflowRunId);

    if (!workflowRun || workflowRun.namespaceId !== this.namespaceId) {
      throw new Error(`Workflow run ${params.workflowRunId} does not exist`);
    }

    // If already canceled, just return it
    if (workflowRun.status === "canceled") {
      return structuredClone(workflowRun);
    }

    // Cannot cancel succeeded or failed workflows
    if (["succeeded", "failed"].includes(workflowRun.status)) {
      throw new Error(
        `Cannot cancel workflow run ${params.workflowRunId} with status ${workflowRun.status}`,
      );
    }

    const now = new Date();

    const updated: WorkflowRun = {
      ...workflowRun,
      status: "canceled",
      workerId: null,
      availableAt: null,
      finishedAt: now,
      updatedAt: now,
    };

    this.workflowRuns.set(params.workflowRunId, updated);

    return structuredClone(updated);
  }

  async listStepAttempts(
    params: ListStepAttemptsParams,
  ): Promise<StepAttempt[]> {
    const stepIds = this.workflowRunSteps.get(params.workflowRunId);

    if (!stepIds) {
      return [];
    }

    const steps = Array.from(stepIds)
      .map((id) => this.stepAttempts.get(id))
      .filter((step): step is StepAttempt => step !== undefined)
      .filter((step) => step.namespaceId === this.namespaceId)
      .sort((a, b) => a.createdAt.getTime() - b.createdAt.getTime());

    return structuredClone(steps);
  }

  async createStepAttempt(
    params: CreateStepAttemptParams,
  ): Promise<StepAttempt> {
    const id = randomUUID();
    const now = new Date();

    const stepAttempt: StepAttempt = {
      namespaceId: this.namespaceId,
      id,
      workflowRunId: params.workflowRunId,
      stepName: params.stepName,
      kind: params.kind,
      status: "running",
      config: params.config,
      context: params.context,
      output: null,
      error: null,
      childWorkflowRunNamespaceId: null,
      childWorkflowRunId: null,
      startedAt: now,
      finishedAt: null,
      createdAt: now,
      updatedAt: now,
    };

    this.stepAttempts.set(id, stepAttempt);

    // Add to workflow run's step set
    let stepSet = this.workflowRunSteps.get(params.workflowRunId);
    if (!stepSet) {
      stepSet = new Set();
      this.workflowRunSteps.set(params.workflowRunId, stepSet);
    }
    stepSet.add(id);

    return structuredClone(stepAttempt);
  }

  async getStepAttempt(
    params: GetStepAttemptParams,
  ): Promise<StepAttempt | null> {
    const stepAttempt = this.stepAttempts.get(params.stepAttemptId);

    if (!stepAttempt || stepAttempt.namespaceId !== this.namespaceId) {
      return null;
    }

    return structuredClone(stepAttempt);
  }

  async markStepAttemptSucceeded(
    params: MarkStepAttemptSucceededParams,
  ): Promise<StepAttempt> {
    const stepAttempt = this.stepAttempts.get(params.stepAttemptId);
    const workflowRun = this.workflowRuns.get(params.workflowRunId);

    if (
      !stepAttempt ||
      stepAttempt.namespaceId !== this.namespaceId ||
      stepAttempt.workflowRunId !== params.workflowRunId ||
      stepAttempt.status !== "running" ||
      !workflowRun ||
      workflowRun.status !== "running" ||
      workflowRun.workerId !== params.workerId
    ) {
      throw new Error("Failed to mark step attempt succeeded");
    }

    const now = new Date();

    const updated: StepAttempt = {
      ...stepAttempt,
      status: "succeeded",
      output: params.output,
      error: null,
      finishedAt: now,
      updatedAt: now,
    };

    this.stepAttempts.set(params.stepAttemptId, updated);

    return structuredClone(updated);
  }

  async markStepAttemptFailed(
    params: MarkStepAttemptFailedParams,
  ): Promise<StepAttempt> {
    const stepAttempt = this.stepAttempts.get(params.stepAttemptId);
    const workflowRun = this.workflowRuns.get(params.workflowRunId);

    if (
      !stepAttempt ||
      stepAttempt.namespaceId !== this.namespaceId ||
      stepAttempt.workflowRunId !== params.workflowRunId ||
      stepAttempt.status !== "running" ||
      !workflowRun ||
      workflowRun.status !== "running" ||
      workflowRun.workerId !== params.workerId
    ) {
      throw new Error("Failed to mark step attempt failed");
    }

    const now = new Date();

    const updated: StepAttempt = {
      ...stepAttempt,
      status: "failed",
      output: null,
      error: params.error,
      finishedAt: now,
      updatedAt: now,
    };

    this.stepAttempts.set(params.stepAttemptId, updated);

    return structuredClone(updated);
  }
}
