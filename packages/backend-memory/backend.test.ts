import { describe, test, expect, beforeEach } from "vitest";
import { BackendMemory } from "./backend.js";
import type { WorkflowRun, StepAttempt } from "openworkflow";

describe("BackendMemory", () => {
  let backend: BackendMemory;

  beforeEach(() => {
    backend = BackendMemory.create();
  });

  describe("createWorkflowRun", () => {
    test("creates a new workflow run", async () => {
      const run = await backend.createWorkflowRun({
        workflowName: "test-workflow",
        version: null,
        idempotencyKey: null,
        config: {},
        context: null,
        input: { foo: "bar" },
        availableAt: null,
        deadlineAt: null,
      });

      expect(run.id).toBeDefined();
      expect(run.workflowName).toBe("test-workflow");
      expect(run.status).toBe("pending");
      expect(run.input).toEqual({ foo: "bar" });
      expect(run.attempts).toBe(0);
    });

    test("sets availableAt to now when not provided", async () => {
      const before = new Date();
      const run = await backend.createWorkflowRun({
        workflowName: "test-workflow",
        version: null,
        idempotencyKey: null,
        config: {},
        context: null,
        input: null,
        availableAt: null,
        deadlineAt: null,
      });
      const after = new Date();

      expect(run.availableAt).toBeDefined();
      expect(run.availableAt!.getTime()).toBeGreaterThanOrEqual(
        before.getTime(),
      );
      expect(run.availableAt!.getTime()).toBeLessThanOrEqual(after.getTime());
    });

    test("uses provided availableAt", async () => {
      const futureDate = new Date(Date.now() + 10_000);
      const run = await backend.createWorkflowRun({
        workflowName: "test-workflow",
        version: null,
        idempotencyKey: null,
        config: {},
        context: null,
        input: null,
        availableAt: futureDate,
        deadlineAt: null,
      });

      expect(run.availableAt).toEqual(futureDate);
    });
  });

  describe("getWorkflowRun", () => {
    test("returns workflow run by id", async () => {
      const created = await backend.createWorkflowRun({
        workflowName: "test-workflow",
        version: null,
        idempotencyKey: null,
        config: {},
        context: null,
        input: { test: 123 },
        availableAt: null,
        deadlineAt: null,
      });

      const retrieved = await backend.getWorkflowRun({
        workflowRunId: created.id,
      });

      expect(retrieved).toBeDefined();
      expect(retrieved!.id).toBe(created.id);
      expect(retrieved!.input).toEqual({ test: 123 });
    });

    test("returns null for non-existent workflow run", async () => {
      const retrieved = await backend.getWorkflowRun({
        workflowRunId: "non-existent-id",
      });

      expect(retrieved).toBeNull();
    });
  });

  describe("claimWorkflowRun", () => {
    test("claims an available workflow run", async () => {
      const created = await backend.createWorkflowRun({
        workflowName: "test-workflow",
        version: null,
        idempotencyKey: null,
        config: {},
        context: null,
        input: null,
        availableAt: null,
        deadlineAt: null,
      });

      const claimed = await backend.claimWorkflowRun({
        workerId: "worker-1",
        leaseDurationMs: 30_000,
      });

      expect(claimed).toBeDefined();
      expect(claimed!.id).toBe(created.id);
      expect(claimed!.status).toBe("running");
      expect(claimed!.workerId).toBe("worker-1");
      expect(claimed!.attempts).toBe(1);
      expect(claimed!.startedAt).toBeDefined();
    });

    test("returns null when no workflow runs are available", async () => {
      const claimed = await backend.claimWorkflowRun({
        workerId: "worker-1",
        leaseDurationMs: 30_000,
      });

      expect(claimed).toBeNull();
    });

    test("does not claim future workflow runs", async () => {
      const futureDate = new Date(Date.now() + 10_000);
      await backend.createWorkflowRun({
        workflowName: "test-workflow",
        version: null,
        idempotencyKey: null,
        config: {},
        context: null,
        input: null,
        availableAt: futureDate,
        deadlineAt: null,
      });

      const claimed = await backend.claimWorkflowRun({
        workerId: "worker-1",
        leaseDurationMs: 30_000,
      });

      expect(claimed).toBeNull();
    });

    test("prioritizes pending runs over running runs", async () => {
      // Create a pending run
      const pending = await backend.createWorkflowRun({
        workflowName: "pending-workflow",
        version: null,
        idempotencyKey: null,
        config: {},
        context: null,
        input: { type: "pending" },
        availableAt: null,
        deadlineAt: null,
      });

      // Create a running run with expired lease
      const running = await backend.createWorkflowRun({
        workflowName: "running-workflow",
        version: null,
        idempotencyKey: null,
        config: {},
        context: null,
        input: { type: "running" },
        availableAt: new Date(Date.now() - 1000), // Past
        deadlineAt: null,
      });

      // Claim it first to make it running
      await backend.claimWorkflowRun({
        workerId: "worker-0",
        leaseDurationMs: -1, // Expired lease
      });

      // Now claim again - should get the pending run
      const claimed = await backend.claimWorkflowRun({
        workerId: "worker-1",
        leaseDurationMs: 30_000,
      });

      expect(claimed).toBeDefined();
      expect(claimed!.id).toBe(pending.id);
    });

    test("marks deadline-expired runs as failed", async () => {
      const pastDeadline = new Date(Date.now() - 1000);
      const created = await backend.createWorkflowRun({
        workflowName: "test-workflow",
        version: null,
        idempotencyKey: null,
        config: {},
        context: null,
        input: null,
        availableAt: null,
        deadlineAt: pastDeadline,
      });

      // Try to claim - should mark as failed and not return it
      const claimed = await backend.claimWorkflowRun({
        workerId: "worker-1",
        leaseDurationMs: 30_000,
      });

      expect(claimed).toBeNull();

      // Verify it was marked as failed
      const run = await backend.getWorkflowRun({
        workflowRunId: created.id,
      });
      expect(run!.status).toBe("failed");
      expect(run!.error).toEqual({ message: "Workflow run deadline exceeded" });
    });
  });

  describe("heartbeatWorkflowRun", () => {
    test("updates availableAt timestamp", async () => {
      const created = await backend.createWorkflowRun({
        workflowName: "test-workflow",
        version: null,
        idempotencyKey: null,
        config: {},
        context: null,
        input: null,
        availableAt: null,
        deadlineAt: null,
      });

      const claimed = await backend.claimWorkflowRun({
        workerId: "worker-1",
        leaseDurationMs: 30_000,
      });

      const before = new Date();
      const heartbeat = await backend.heartbeatWorkflowRun({
        workflowRunId: claimed!.id,
        workerId: "worker-1",
        leaseDurationMs: 60_000,
      });

      expect(heartbeat.availableAt).toBeDefined();
      expect(heartbeat.availableAt!.getTime()).toBeGreaterThan(before.getTime());
    });

    test("throws when workflow run is not running", async () => {
      const created = await backend.createWorkflowRun({
        workflowName: "test-workflow",
        version: null,
        idempotencyKey: null,
        config: {},
        context: null,
        input: null,
        availableAt: null,
        deadlineAt: null,
      });

      await expect(
        backend.heartbeatWorkflowRun({
          workflowRunId: created.id,
          workerId: "worker-1",
          leaseDurationMs: 30_000,
        }),
      ).rejects.toThrow("Failed to heartbeat workflow run");
    });

    test("throws when worker id does not match", async () => {
      const created = await backend.createWorkflowRun({
        workflowName: "test-workflow",
        version: null,
        idempotencyKey: null,
        config: {},
        context: null,
        input: null,
        availableAt: null,
        deadlineAt: null,
      });

      await backend.claimWorkflowRun({
        workerId: "worker-1",
        leaseDurationMs: 30_000,
      });

      await expect(
        backend.heartbeatWorkflowRun({
          workflowRunId: created.id,
          workerId: "worker-2",
          leaseDurationMs: 30_000,
        }),
      ).rejects.toThrow("Failed to heartbeat workflow run");
    });
  });

  describe("sleepWorkflowRun", () => {
    test("sets workflow run to sleeping status", async () => {
      const created = await backend.createWorkflowRun({
        workflowName: "test-workflow",
        version: null,
        idempotencyKey: null,
        config: {},
        context: null,
        input: null,
        availableAt: null,
        deadlineAt: null,
      });

      const claimed = await backend.claimWorkflowRun({
        workerId: "worker-1",
        leaseDurationMs: 30_000,
      });

      const resumeAt = new Date(Date.now() + 60_000);
      const sleeping = await backend.sleepWorkflowRun({
        workflowRunId: claimed!.id,
        workerId: "worker-1",
        availableAt: resumeAt,
      });

      expect(sleeping.status).toBe("sleeping");
      expect(sleeping.availableAt).toEqual(resumeAt);
      expect(sleeping.workerId).toBeNull();
    });

    test("throws when workflow is in terminal state", async () => {
      const created = await backend.createWorkflowRun({
        workflowName: "test-workflow",
        version: null,
        idempotencyKey: null,
        config: {},
        context: null,
        input: null,
        availableAt: null,
        deadlineAt: null,
      });

      const claimed = await backend.claimWorkflowRun({
        workerId: "worker-1",
        leaseDurationMs: 30_000,
      });

      // Mark as succeeded
      await backend.markWorkflowRunSucceeded({
        workflowRunId: claimed!.id,
        workerId: "worker-1",
        output: null,
      });

      // Try to sleep
      await expect(
        backend.sleepWorkflowRun({
          workflowRunId: claimed!.id,
          workerId: "worker-1",
          availableAt: new Date(),
        }),
      ).rejects.toThrow("Failed to sleep workflow run");
    });
  });

  describe("markWorkflowRunSucceeded", () => {
    test("marks workflow run as succeeded", async () => {
      const created = await backend.createWorkflowRun({
        workflowName: "test-workflow",
        version: null,
        idempotencyKey: null,
        config: {},
        context: null,
        input: null,
        availableAt: null,
        deadlineAt: null,
      });

      const claimed = await backend.claimWorkflowRun({
        workerId: "worker-1",
        leaseDurationMs: 30_000,
      });

      const succeeded = await backend.markWorkflowRunSucceeded({
        workflowRunId: claimed!.id,
        workerId: "worker-1",
        output: { result: "success" },
      });

      expect(succeeded.status).toBe("succeeded");
      expect(succeeded.output).toEqual({ result: "success" });
      expect(succeeded.finishedAt).toBeDefined();
      expect(succeeded.availableAt).toBeNull();
    });
  });

  describe("markWorkflowRunFailed", () => {
    test("retries workflow run on failure", async () => {
      const created = await backend.createWorkflowRun({
        workflowName: "test-workflow",
        version: null,
        idempotencyKey: null,
        config: {},
        context: null,
        input: null,
        availableAt: null,
        deadlineAt: null,
      });

      const claimed = await backend.claimWorkflowRun({
        workerId: "worker-1",
        leaseDurationMs: 30_000,
      });

      const failed = await backend.markWorkflowRunFailed({
        workflowRunId: claimed!.id,
        workerId: "worker-1",
        error: { message: "Test error" },
      });

      expect(failed.status).toBe("pending");
      expect(failed.error).toEqual({ message: "Test error" });
      expect(failed.availableAt).toBeDefined();
      expect(failed.availableAt!.getTime()).toBeGreaterThan(Date.now());
      expect(failed.finishedAt).toBeNull();
    });

    test("marks as failed when retry would exceed deadline", async () => {
      const deadline = new Date(Date.now() + 100);
      const created = await backend.createWorkflowRun({
        workflowName: "test-workflow",
        version: null,
        idempotencyKey: null,
        config: {},
        context: null,
        input: null,
        availableAt: null,
        deadlineAt: deadline,
      });

      const claimed = await backend.claimWorkflowRun({
        workerId: "worker-1",
        leaseDurationMs: 30_000,
      });

      // Wait a bit to ensure deadline is exceeded
      await new Promise((resolve) => setTimeout(resolve, 150));

      const failed = await backend.markWorkflowRunFailed({
        workflowRunId: claimed!.id,
        workerId: "worker-1",
        error: { message: "Test error" },
      });

      expect(failed.status).toBe("failed");
      expect(failed.availableAt).toBeNull();
      expect(failed.finishedAt).toBeDefined();
    });
  });

  describe("cancelWorkflowRun", () => {
    test("cancels a pending workflow run", async () => {
      const created = await backend.createWorkflowRun({
        workflowName: "test-workflow",
        version: null,
        idempotencyKey: null,
        config: {},
        context: null,
        input: null,
        availableAt: null,
        deadlineAt: null,
      });

      const canceled = await backend.cancelWorkflowRun({
        workflowRunId: created.id,
      });

      expect(canceled.status).toBe("canceled");
      expect(canceled.finishedAt).toBeDefined();
      expect(canceled.availableAt).toBeNull();
    });

    test("returns already canceled workflow", async () => {
      const created = await backend.createWorkflowRun({
        workflowName: "test-workflow",
        version: null,
        idempotencyKey: null,
        config: {},
        context: null,
        input: null,
        availableAt: null,
        deadlineAt: null,
      });

      const canceled1 = await backend.cancelWorkflowRun({
        workflowRunId: created.id,
      });

      const canceled2 = await backend.cancelWorkflowRun({
        workflowRunId: created.id,
      });

      expect(canceled2.status).toBe("canceled");
      expect(canceled2.id).toBe(canceled1.id);
    });

    test("throws when canceling succeeded workflow", async () => {
      const created = await backend.createWorkflowRun({
        workflowName: "test-workflow",
        version: null,
        idempotencyKey: null,
        config: {},
        context: null,
        input: null,
        availableAt: null,
        deadlineAt: null,
      });

      const claimed = await backend.claimWorkflowRun({
        workerId: "worker-1",
        leaseDurationMs: 30_000,
      });

      await backend.markWorkflowRunSucceeded({
        workflowRunId: claimed!.id,
        workerId: "worker-1",
        output: null,
      });

      await expect(
        backend.cancelWorkflowRun({
          workflowRunId: created.id,
        }),
      ).rejects.toThrow("Cannot cancel workflow run");
    });
  });

  describe("Step Attempts", () => {
    let workflowRun: WorkflowRun;

    beforeEach(async () => {
      workflowRun = await backend.createWorkflowRun({
        workflowName: "test-workflow",
        version: null,
        idempotencyKey: null,
        config: {},
        context: null,
        input: null,
        availableAt: null,
        deadlineAt: null,
      });

      await backend.claimWorkflowRun({
        workerId: "worker-1",
        leaseDurationMs: 30_000,
      });
    });

    test("createStepAttempt creates a new step attempt", async () => {
      const step = await backend.createStepAttempt({
        workflowRunId: workflowRun.id,
        workerId: "worker-1",
        stepName: "test-step",
        kind: "function",
        config: {},
        context: null,
      });

      expect(step.id).toBeDefined();
      expect(step.stepName).toBe("test-step");
      expect(step.status).toBe("running");
      expect(step.workflowRunId).toBe(workflowRun.id);
    });

    test("listStepAttempts returns all steps for a workflow run", async () => {
      await backend.createStepAttempt({
        workflowRunId: workflowRun.id,
        workerId: "worker-1",
        stepName: "step-1",
        kind: "function",
        config: {},
        context: null,
      });

      await backend.createStepAttempt({
        workflowRunId: workflowRun.id,
        workerId: "worker-1",
        stepName: "step-2",
        kind: "function",
        config: {},
        context: null,
      });

      const steps = await backend.listStepAttempts({
        workflowRunId: workflowRun.id,
      });

      expect(steps).toHaveLength(2);
      expect(steps[0].stepName).toBe("step-1");
      expect(steps[1].stepName).toBe("step-2");
    });

    test("markStepAttemptSucceeded marks step as succeeded", async () => {
      const step = await backend.createStepAttempt({
        workflowRunId: workflowRun.id,
        workerId: "worker-1",
        stepName: "test-step",
        kind: "function",
        config: {},
        context: null,
      });

      const succeeded = await backend.markStepAttemptSucceeded({
        workflowRunId: workflowRun.id,
        stepAttemptId: step.id,
        workerId: "worker-1",
        output: { value: 42 },
      });

      expect(succeeded.status).toBe("succeeded");
      expect(succeeded.output).toEqual({ value: 42 });
      expect(succeeded.finishedAt).toBeDefined();
    });

    test("markStepAttemptFailed marks step as failed", async () => {
      const step = await backend.createStepAttempt({
        workflowRunId: workflowRun.id,
        workerId: "worker-1",
        stepName: "test-step",
        kind: "function",
        config: {},
        context: null,
      });

      const failed = await backend.markStepAttemptFailed({
        workflowRunId: workflowRun.id,
        stepAttemptId: step.id,
        workerId: "worker-1",
        error: { message: "Step failed" },
      });

      expect(failed.status).toBe("failed");
      expect(failed.error).toEqual({ message: "Step failed" });
      expect(failed.finishedAt).toBeDefined();
    });
  });

  describe("Namespace isolation", () => {
    test("different namespaces do not see each other's data", async () => {
      const backend1 = BackendMemory.create({ namespaceId: "namespace-1" });
      const backend2 = BackendMemory.create({ namespaceId: "namespace-2" });

      const run1 = await backend1.createWorkflowRun({
        workflowName: "test-workflow",
        version: null,
        idempotencyKey: null,
        config: {},
        context: null,
        input: null,
        availableAt: null,
        deadlineAt: null,
      });

      // backend2 should not see run1
      const retrieved = await backend2.getWorkflowRun({
        workflowRunId: run1.id,
      });

      expect(retrieved).toBeNull();
    });
  });

  describe("State export and import", () => {
    test("exports and imports workflow runs", async () => {
      const backend1 = BackendMemory.create();

      // Create some workflow runs
      const run1 = await backend1.createWorkflowRun({
        workflowName: "workflow-1",
        version: null,
        idempotencyKey: null,
        config: {},
        context: null,
        input: { data: "test1" },
        availableAt: null,
        deadlineAt: null,
      });

      const run2 = await backend1.createWorkflowRun({
        workflowName: "workflow-2",
        version: null,
        idempotencyKey: null,
        config: {},
        context: null,
        input: { data: "test2" },
        availableAt: null,
        deadlineAt: null,
      });

      // Export state
      const state = backend1.exportState();

      expect(state.workflowRuns).toHaveLength(2);
      expect(state.stepAttempts).toHaveLength(0);

      // Create new backend and import state
      const backend2 = BackendMemory.create({ initialState: state });

      // Verify runs were imported
      const retrievedRun1 = await backend2.getWorkflowRun({
        workflowRunId: run1.id,
      });
      const retrievedRun2 = await backend2.getWorkflowRun({
        workflowRunId: run2.id,
      });

      expect(retrievedRun1).not.toBeNull();
      expect(retrievedRun1!.input).toEqual({ data: "test1" });
      expect(retrievedRun2).not.toBeNull();
      expect(retrievedRun2!.input).toEqual({ data: "test2" });
    });

    test("exports and imports step attempts", async () => {
      const backend1 = BackendMemory.create();

      const run = await backend1.createWorkflowRun({
        workflowName: "test-workflow",
        version: null,
        idempotencyKey: null,
        config: {},
        context: null,
        input: null,
        availableAt: null,
        deadlineAt: null,
      });

      await backend1.claimWorkflowRun({
        workerId: "worker-1",
        leaseDurationMs: 30_000,
      });

      // Create step attempts
      const step1 = await backend1.createStepAttempt({
        workflowRunId: run.id,
        workerId: "worker-1",
        stepName: "step-1",
        kind: "function",
        config: {},
        context: null,
      });

      await backend1.markStepAttemptSucceeded({
        workflowRunId: run.id,
        stepAttemptId: step1.id,
        workerId: "worker-1",
        output: { value: 42 },
      });

      // Export state
      const state = backend1.exportState();

      expect(state.workflowRuns).toHaveLength(1);
      expect(state.stepAttempts).toHaveLength(1);

      // Create new backend and import state
      const backend2 = BackendMemory.create({ initialState: state });

      // Verify steps were imported
      const steps = await backend2.listStepAttempts({
        workflowRunId: run.id,
      });

      expect(steps).toHaveLength(1);
      expect(steps[0].stepName).toBe("step-1");
      expect(steps[0].output).toEqual({ value: 42 });
    });

    test("importState replaces existing data", async () => {
      const backend = BackendMemory.create();

      // Create initial data
      const run1 = await backend.createWorkflowRun({
        workflowName: "old-workflow",
        version: null,
        idempotencyKey: null,
        config: {},
        context: null,
        input: { data: "old" },
        availableAt: null,
        deadlineAt: null,
      });

      // Export state with different data
      const backend2 = BackendMemory.create();
      const run2 = await backend2.createWorkflowRun({
        workflowName: "new-workflow",
        version: null,
        idempotencyKey: null,
        config: {},
        context: null,
        input: { data: "new" },
        availableAt: null,
        deadlineAt: null,
      });
      const newState = backend2.exportState();

      // Import new state into first backend
      backend.importState(newState);

      // Old run should be gone
      const oldRun = await backend.getWorkflowRun({
        workflowRunId: run1.id,
      });
      expect(oldRun).toBeNull();

      // New run should exist
      const retrievedRun = await backend.getWorkflowRun({
        workflowRunId: run2.id,
      });
      expect(retrievedRun).not.toBeNull();
      expect(retrievedRun!.input).toEqual({ data: "new" });
    });

    test("exported state can be serialized to JSON", async () => {
      const backend = BackendMemory.create();

      const run = await backend.createWorkflowRun({
        workflowName: "test-workflow",
        version: "v1",
        idempotencyKey: "key-123",
        config: { timeout: 5000 },
        context: { requestId: "req-456" },
        input: { userId: 123 },
        availableAt: new Date("2025-01-01T00:00:00Z"),
        deadlineAt: new Date("2025-01-01T01:00:00Z"),
      });

      const state = backend.exportState();

      // Serialize to JSON
      const json = JSON.stringify(state);
      expect(json).toBeDefined();

      // Deserialize from JSON
      const parsedState = JSON.parse(json);

      // Create new backend with deserialized state
      const backend2 = BackendMemory.create({ initialState: parsedState });

      const retrievedRun = await backend2.getWorkflowRun({
        workflowRunId: run.id,
      });

      expect(retrievedRun).not.toBeNull();
      expect(retrievedRun!.workflowName).toBe("test-workflow");
      expect(retrievedRun!.version).toBe("v1");
      expect(retrievedRun!.idempotencyKey).toBe("key-123");
      expect(retrievedRun!.input).toEqual({ userId: 123 });
    });
  });
});
