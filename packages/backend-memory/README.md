# @openworkflow/backend-memory

In-memory backend for OpenWorkflow. Store workflow runs and step attempts in memory without requiring any database.

## Features

- **No database required**: All state stored in memory
- **Fast and simple**: Perfect for development, testing, and prototyping
- **Fully compatible**: Implements the complete Backend interface
- **Namespace support**: Isolate workflows by namespace

## Installation

```bash
npm install @openworkflow/backend-memory openworkflow
```

## Usage

```typescript
import { BackendMemory } from "@openworkflow/backend-memory";
import { OpenWorkflow } from "openworkflow";

// Create an in-memory backend
const backend = BackendMemory.create();
const ow = new OpenWorkflow({ backend });

// Define and run workflows as normal
const myWorkflow = ow.defineWorkflow(
  { name: "my-workflow" },
  async ({ input, step }) => {
    const result = await step.run({ name: "my-step" }, async () => {
      return "Hello, World!";
    });
    return { result };
  },
);

// Start a worker
const worker = ow.newWorker();
await worker.start();

// Run a workflow
const handle = await myWorkflow.run({ data: "test" });
const result = await handle.result();

// Clean up
await worker.stop();
```

## Configuration

You can configure the in-memory backend with options:

```typescript
const backend = BackendMemory.create({
  namespaceId: "production", // Optional: namespace for workflow isolation
});
```

## Important Notes

⚠️ **Data is not persisted**: All workflow state is lost when the process exits. This backend is ideal for:

- Development and testing
- Ephemeral workflows
- Scenarios where clients maintain state (e.g., via HTTP SSE with JSON patches)

For production workloads requiring durability, use [@openworkflow/backend-postgres](../backend-postgres).

## API

### `BackendMemory.create(options?)`

Creates a new in-memory backend instance.

**Options:**
- `namespaceId` (optional): String identifier to isolate workflows. Defaults to `"default"`.

**Returns:** `BackendMemory` instance implementing the `Backend` interface.

## License

Apache-2.0
