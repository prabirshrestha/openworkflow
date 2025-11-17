import {
  BackendMemory,
  type BackendMemoryState,
} from "@openworkflow/backend-memory";
import { OpenWorkflow } from "openworkflow";

/**
 * Example demonstrating workflow state export/import for HTTP SSE scenarios.
 *
 * This shows how the in-memory backend can:
 * 1. Export workflow state to send to clients (e.g., via HTTP SSE)
 * 2. Import state received from clients to resume workflows
 *
 * This pattern enables horizontal scaling without persistent storage,
 * as clients maintain the workflow state.
 */

// Define a simple workflow
const backend = BackendMemory.create();
const ow = new OpenWorkflow({ backend });

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

/**
 * Simulate a server-side handler that starts a workflow and exports state.
 */
async function serverStartWorkflow(orderId: string): Promise<{
  workflowRunId: string;
  state: BackendMemoryState;
}> {
  console.log("\n=== SERVER: Starting workflow ===");

  // Start the workflow
  const handle = await processOrder.run({ orderId });

  // Export the state to send to the client
  const state = backend.exportState();

  console.log(`Server exported state with ${state.workflowRuns.length} runs`);

  return {
    workflowRunId: handle.workflowRun.id,
    state,
  };
}

/**
 * Simulate a client receiving the state and executing the workflow.
 */
async function clientExecuteWorkflow(
  initialState: BackendMemoryState,
): Promise<BackendMemoryState> {
  console.log("\n=== CLIENT: Executing workflow ===");

  // Create a new backend with the received state
  const clientBackend = BackendMemory.create({ initialState });
  const clientOw = new OpenWorkflow({ backend: clientBackend });

  // Re-define the workflow (the client has the same code)
  clientOw.defineWorkflow(
    { name: "process-order" },
    async ({ input, step }) => {
      const orderId = input.orderId as string;

      const validated = await step.run({ name: "validate-order" }, async () => {
        console.log(`[CLIENT] Validating order ${orderId}`);
        return { valid: true, orderId };
      });

      const charged = await step.run({ name: "charge-payment" }, async () => {
        console.log(`[CLIENT] Charging payment for order ${orderId}`);
        return { paymentId: `pay_${orderId}`, amount: 99.99 };
      });

      const shipped = await step.run({ name: "ship-order" }, async () => {
        console.log(`[CLIENT] Shipping order ${orderId}`);
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

  // Start a worker and execute
  const worker = clientOw.newWorker();
  await worker.start();

  // Wait a bit for execution (in real scenario, you'd poll or use events)
  await new Promise((resolve) => setTimeout(resolve, 100));

  await worker.stop();

  // Export the updated state to send back to server
  const updatedState = clientBackend.exportState();
  console.log(
    `Client executed workflow, exporting state with ${updatedState.stepAttempts.length} steps`,
  );

  return updatedState;
}

/**
 * Simulate server receiving updated state from client.
 */
async function serverReceiveState(
  state: BackendMemoryState,
  workflowRunId: string,
): Promise<void> {
  console.log("\n=== SERVER: Receiving updated state ===");

  // Import the state
  backend.importState(state);

  // Check the workflow status
  const run = await backend.getWorkflowRun({ workflowRunId });
  console.log(`Workflow status: ${run?.status}`);

  if (run?.status === "succeeded") {
    console.log("Workflow output:", JSON.stringify(run.output, null, 2));
  }

  // List completed steps
  const steps = await backend.listStepAttempts({ workflowRunId });
  console.log(`\nCompleted steps:`);
  for (const step of steps) {
    console.log(
      `  - ${step.stepName}: ${step.status} ${step.output ? `(${JSON.stringify(step.output)})` : ""}`,
    );
  }
}

/**
 * Run the example.
 */
async function main() {
  console.log("===== HTTP SSE State Transfer Example =====\n");
  console.log(
    "This demonstrates how workflow state can be transferred between",
  );
  console.log("server and client via HTTP SSE or similar mechanisms.\n");

  // 1. Server starts workflow and sends state to client
  const { workflowRunId, state } = await serverStartWorkflow("order-123");

  console.log("\n--- Simulating state transfer to client (e.g., via HTTP) ---");
  const serialized = JSON.stringify(state);
  console.log(`Serialized state size: ${serialized.length} bytes`);

  // 2. Client receives state, executes workflow, and sends back updated state
  const updatedState = await clientExecuteWorkflow(state);

  console.log("\n--- Simulating state transfer back to server ---");

  // 3. Server receives updated state
  await serverReceiveState(updatedState, workflowRunId);

  console.log("\n===== Example Complete =====");
}

await main().catch((error: unknown) => {
  console.error(error);
  process.exitCode = 1;
});
