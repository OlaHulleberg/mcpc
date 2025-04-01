# MCPC Protocol Details

MCPC extends the MCP protocol while using the same underlying transport layer. This design choice ensures maximum compatibility and minimal setup overhead.

## Transport Layer

MCPC messages flow through the existing MCP transport (whether STDIO, SSE, or other implementations). The protocol wraps the standard MCP transport with event listeners that enable:

- Server-to-client communication during task execution
- Real-time updates and notifications
- Asynchronous task completion signals
  All while maintaining full backward compatibility with standard MCP implementations.

## MCPC Message Structure

MCPC messages have the following structure:

```python
class MCPCMessage:
    type: Literal["task", "server_event"] # Type of message
    event: str             # Event type (restricted for task messages)
    session_id: str | None # Unique session identifier
    task_id: str | None    # Unique task identifier (required for task messages)
    tool_name: str | None  # Name of the tool being called (required for task messages)
    result: Any = None     # Result or update data
    protocol: str = "mcpc" # Protocol identifier
```

## Message Types and Events

MCPC defines two types of messages with different event restrictions:

### Task Messages

- Type: `task`
- Events:
  - `created`: Initial acknowledgment when task begins
  - `update`: Progress updates during task execution
  - `complete`: Final result when task completes successfully
  - `failed`: Error information when task fails

### Server Event Messages

- Type: `server_event`
- Events: Any string is allowed, as they are not tied to a specific task lifecycle
- Common examples include: `notification`, `alert`, `update`, `error`, etc.

## Example Task Message

```python
# Task progress update
task_message = mcpc.create_task_event(
    event="update",
    tool_name="process_data",
    session_id="session_123",
    task_id="fe6762d0-ffe9-46b1-96e8-b1df2dcc08a9",
    result={
        "status": "Processing step 3/5",
        "progress": 60
    },
)
```

## Example Server Event Message

```python
# Server-initiated Kafka notification
server_event = mcpc.create_server_event(
    event="notification",  # Event must be explicitly specified
    session_id="session123",
    result={
        "topic": "user_updates",
        "event": "user_created",
        "user_id": "user456",
        "timestamp": "2024-03-20T10:00:00Z"
    },
)
```

## Protocol Negotiation

MCPC clients and servers use a negotiation process to establish capabilities:

1. Client checks for MCPC support by calling the special tool `_is_mcpc_enabled`
2. Server responds with protocol information if supported
3. Client adapts behavior based on server capabilities

This ensures backward compatibility with standard MCP components.
