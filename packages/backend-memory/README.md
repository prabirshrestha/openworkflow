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

**Important**: Create the backend and OpenWorkflow instance **once** at application startup, not for every request.

```typescript
import { BackendMemory } from "@openworkflow/backend-memory";
import { OpenWorkflow } from "openworkflow";

// Create these ONCE at application startup
const backend = BackendMemory.create();
const ow = new OpenWorkflow({ backend });

// Define workflows ONCE at startup
const myWorkflow = ow.defineWorkflow(
  { name: "my-workflow" },
  async ({ input, step }) => {
    const result = await step.run({ name: "my-step" }, async () => {
      return "Hello, World!";
    });
    return { result };
  },
);

// Start worker ONCE at startup
const worker = ow.newWorker();
await worker.start();

// For each request, use the same backend and workflow definitions
// If receiving state from a client, use importState()
app.post("/resume", (req, res) => {
  backend.importState(req.body.state); // Import state into existing backend
  // ... handle request
});
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
- `initialState` (optional): Initial state to populate the backend. Use this to restore state from a previous export.

**Returns:** `BackendMemory` instance implementing the `Backend` interface.

### `backend.exportState()`

Exports the current state of the backend as a serializable object.

**Returns:** `BackendMemoryState` object containing all workflow runs and step attempts.

**Example:**
```typescript
const state = backend.exportState();
const json = JSON.stringify(state); // Can be sent to clients via HTTP
```

### `backend.importState(state)`

Imports state into the backend, replacing all existing data.

**Parameters:**
- `state`: `BackendMemoryState` object to import

**Example:**
```typescript
const receivedState = JSON.parse(jsonFromClient);
backend.importState(receivedState);
```

## State Transfer Pattern

The in-memory backend enables a powerful pattern for stateless, horizontally scalable workflow execution.

**Important**: Create the backend and OpenWorkflow instance **once** at application startup. For each request, use `importState()` to restore state into the **same** backend instance.

```typescript
// At application startup (ONCE)
const backend = BackendMemory.create();
const ow = new OpenWorkflow({ backend });
const myWorkflow = ow.defineWorkflow({ name: "my-workflow" }, async ({ input, step }) => {
  // ... workflow logic
});
const worker = ow.newWorker();
await worker.start();

// For each HTTP request (MANY TIMES)
app.post("/workflows/resume", (req, res) => {
  // Import state into the EXISTING backend
  backend.importState(req.body.state);
  
  // Process and export updated state
  const updatedState = backend.exportState();
  res.json({ state: updatedState });
});
```

### How it works:

1. **Server**: Start a workflow and export its state
2. **Transfer**: Send the state to a client (e.g., via HTTP SSE, WebSocket, or REST API)
3. **Client**: Import the state, execute workflow steps, and export the updated state
4. **Transfer**: Send the updated state back to the server
5. **Server**: Import the updated state using `importState()` on the **same** backend instance

This pattern allows:
- **Zero database overhead**: No persistent storage required
- **Horizontal scaling**: Any server can handle any request with client-provided state
- **Client-side execution**: Offload workflow execution to clients when appropriate
- **Flexible deployment**: Mix server-side and client-side execution as needed

See the [examples/memory-http-server](../../examples/memory-http-server) and [examples/memory-sse](../../examples/memory-sse) directories for complete examples.

## License

Apache-2.0
