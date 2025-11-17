import {
  BackendMemory,
  type BackendMemoryState,
} from "@openworkflow/backend-memory";
import { OpenWorkflow } from "openworkflow";

/**
 * Example demonstrating proper HTTP server setup with in-memory backend.
 *
 * This shows the CORRECT pattern:
 * - Create backend and OpenWorkflow instance ONCE at startup
 * - Define workflows ONCE at startup
 * - For each request, use importState() to restore state into the SAME backend
 *
 * This avoids creating new instances for every request.
 */

// ============================================================================
// APPLICATION SETUP (ONCE AT STARTUP)
// ============================================================================

// Create backend and OpenWorkflow ONCE at application startup
const backend = BackendMemory.create();
const ow = new OpenWorkflow({ backend });

// Define workflows ONCE at application startup
const processOrder = ow.defineWorkflow(
  { name: "process-order" },
  async ({ input, step }) => {
    const orderId = input.orderId as string;

    const validated = await step.run({ name: "validate-order" }, async () => {
      console.log(`Validating order ${orderId}`);
      return { valid: true, orderId };
    });

    const charged = await step.run({ name: "charge-payment" }, async () => {
      console.log(`Charging payment for order ${orderId}`);
      return { paymentId: `pay_${orderId}`, amount: 99.99 };
    });

    const shipped = await step.run({ name: "ship-order" }, async () => {
      console.log(`Shipping order ${orderId}`);
      return { trackingNumber: `track_${orderId}` };
    });

    return {
      orderId,
      status: "completed",
      paymentId: charged.paymentId,
      trackingNumber: shipped.trackingNumber,
    };
  },
);

// Start worker ONCE at application startup
const worker = ow.newWorker({ concurrency: 10 });
await worker.start();

// ============================================================================
// HTTP REQUEST HANDLERS (CALLED FOR EACH REQUEST)
// ============================================================================

/**
 * Handler for POST /workflows - Start a new workflow
 */
async function handleStartWorkflow(orderId: string): Promise<{
  workflowRunId: string;
  state: BackendMemoryState;
}> {
  console.log(`\n[POST /workflows] Starting workflow for order ${orderId}`);

  // Start the workflow using the existing workflow definition
  const handle = await processOrder.run({ orderId });

  // Export the state to send to the client
  const state = backend.exportState();

  console.log(`  → Exported state with ${state.workflowRuns.length} runs`);

  return {
    workflowRunId: handle.workflowRun.id,
    state,
  };
}

/**
 * Handler for POST /workflows/resume - Resume workflow with updated state
 */
async function handleResumeWorkflow(
  workflowRunId: string,
  receivedState: BackendMemoryState,
): Promise<{
  workflowRunId: string;
  state: BackendMemoryState;
}> {
  console.log(`\n[POST /workflows/resume] Resuming workflow ${workflowRunId}`);

  // Import the state into the EXISTING backend
  // This replaces the current state with the received state
  backend.importState(receivedState);

  console.log(
    `  → Imported state with ${receivedState.workflowRuns.length} runs, ${receivedState.stepAttempts.length} steps`,
  );

  // Check the workflow status
  const run = await backend.getWorkflowRun({ workflowRunId });
  console.log(`  → Workflow status: ${run?.status}`);

  // Export the current state to send back
  const state = backend.exportState();

  return {
    workflowRunId,
    state,
  };
}

/**
 * Handler for GET /workflows/:id - Get workflow status
 */
async function handleGetWorkflow(workflowRunId: string): Promise<{
  workflowRunId: string;
  status: string;
  output: unknown;
}> {
  console.log(`\n[GET /workflows/${workflowRunId}] Getting workflow status`);

  const run = await backend.getWorkflowRun({ workflowRunId });

  if (!run) {
    throw new Error(`Workflow ${workflowRunId} not found`);
  }

  console.log(`  → Status: ${run.status}`);

  return {
    workflowRunId,
    status: run.status,
    output: run.output,
  };
}

// ============================================================================
// DEMONSTRATION
// ============================================================================

async function main() {
  console.log("===== HTTP Server Pattern Example =====\n");
  console.log("This demonstrates the CORRECT pattern for HTTP servers:");
  console.log("- Backend and OpenWorkflow created ONCE at startup");
  console.log("- Workflows defined ONCE at startup");
  console.log("- Each request uses importState() on the SAME backend\n");

  // Simulate: Client makes POST /workflows request
  const { workflowRunId, state: initialState } =
    await handleStartWorkflow("order-123");

  console.log("\n--- Simulating state transfer to client (e.g., via HTTP) ---");
  const serialized = JSON.stringify(initialState);
  console.log(`Serialized state size: ${serialized.length} bytes`);

  // Simulate: Client processes some steps and sends state back
  console.log("\n--- Client executes workflow (not shown) ---");

  // Wait a bit for the workflow to complete
  await new Promise((resolve) => setTimeout(resolve, 100));

  // Simulate: Client makes POST /workflows/resume request
  const currentState = backend.exportState();
  const { state: updatedState } = await handleResumeWorkflow(
    workflowRunId,
    currentState,
  );

  // Simulate: Client makes GET /workflows/:id request
  const result = await handleGetWorkflow(workflowRunId);

  console.log("\n===== Result =====");
  console.log(JSON.stringify(result, null, 2));

  console.log("\n===== Cleanup =====");
  await worker.stop();
  console.log("Worker stopped");
}

await main().catch((error: unknown) => {
  console.error(error);
  process.exitCode = 1;
});
