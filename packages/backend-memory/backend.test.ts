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
      const futureDate = new Date(Date.now() + 10000);
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
        leaseDurationMs: 30000,
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
        leaseDurationMs: 30000,
      });

      expect(claimed).toBeNull();
    });

    test("does not claim future workflow runs", async () => {
      const futureDate = new Date(Date.now() + 10000);
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
        leaseDurationMs: 30000,
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
        leaseDurationMs: 30000,
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
        leaseDurationMs: 30000,
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
        leaseDurationMs: 30000,
      });

      const before = new Date();
      const heartbeat = await backend.heartbeatWorkflowRun({
        workflowRunId: claimed!.id,
        workerId: "worker-1",
        leaseDurationMs: 60000,
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
          leaseDurationMs: 30000,
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
        leaseDurationMs: 30000,
      });

      await expect(
        backend.heartbeatWorkflowRun({
          workflowRunId: created.id,
          workerId: "worker-2",
          leaseDurationMs: 30000,
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
        leaseDurationMs: 30000,
      });

      const resumeAt = new Date(Date.now() + 60000);
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
        leaseDurationMs: 30000,
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
        leaseDurationMs: 30000,
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
        leaseDurationMs: 30000,
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
        leaseDurationMs: 30000,
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
        leaseDurationMs: 30000,
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
        leaseDurationMs: 30000,
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
});
