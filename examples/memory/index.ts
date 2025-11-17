import { BackendMemory } from "@openworkflow/backend-memory";
import { OpenWorkflow } from "openworkflow";

// Create an in-memory backend
const backend = BackendMemory.create();
const ow = new OpenWorkflow({ backend });

interface GreetingInput {
  name: string;
}

interface GreetingOutput {
  message: string;
  timestamp: string;
}

/**
 * A simple workflow that demonstrates the in-memory backend.
 */
const greetWorkflow = ow.defineWorkflow<GreetingInput, GreetingOutput>(
  { name: "greet" },
  async ({ input, step }) => {
    console.log(`Starting workflow for ${input.name}`);

    const greeting = await step.run({ name: "create-greeting" }, async () => {
      console.log("Creating greeting...");
      return `Hello, ${input.name}!`;
    });

    const timestamp = await step.run({ name: "get-timestamp" }, async () => {
      console.log("Getting timestamp...");
      return new Date().toISOString();
    });

    console.log(`Workflow completed for ${input.name}`);

    return {
      message: greeting,
      timestamp,
    };
  },
);

/**
 * Run the example workflow.
 */
async function main() {
  console.log("Starting worker...");
  const worker = ow.newWorker({ concurrency: 2 });
  await worker.start();

  console.log("Running workflows...");
  const handles = await Promise.all([
    greetWorkflow.run({ name: "Alice" }),
    greetWorkflow.run({ name: "Bob" }),
  ]);

  console.log("Waiting for results...");
  const results = await Promise.all(handles.map((h) => h.result()));

  console.log("\nResults:");
  for (const result of results) {
    console.log(`  ${result.message} (${result.timestamp})`);
  }

  console.log("\nStopping worker...");
  await worker.stop();

  console.log("Done!");
}

await main().catch((error: unknown) => {
  console.error(error);
  process.exitCode = 1;
});
