# MCPC API Reference

## MCPCHelper

The `MCPCHelper` class provides features for implementing MCPC in server applications:

1. **Task Management**

   - `start_task()`: Run a background task with automatic thread management, with configurable timeout for standard MCP clients
   - `check_task()`: Get the status of a running task
   - `stop_task()`: Request a task to stop gracefully
   - `cleanup_task()`: Remove a completed task from tracking

2. **Message Handling**

   - `create_message()`: Create standardized MCPC protocol messages
   - `create_server_event()`: Shorthand for creating server event messages
   - `send()`: Send messages through the configured transport

3. **Protocol Information**
   - `get_protocol_info()`: Return MCPC protocol compatibility information

## MCPC Messaging API

The MCPC helper provides a simple API for creating and sending messages:

```python
# Supports either FastMCP or Server
server = FastMCP("provider-name") # or Server("provider-name")
mcpc = MCPCHelper(server)

# Create a task message
task_message = mcpc.create_task_event(
    event="update",  # one of: created, update, complete, failed
    tool_name="tool_name",
    session_id="session_123",
    task_id="task_456",
    result="Processing data..."  # can be any JSON-serializable object
)

# Create a server event message
server_event = mcpc.create_server_event(
    event="database_updated",
    session_id="session_123",
    result={"tables": ["users", "products"]}
)

# Send a message through the configured transport
await mcpc.send(task_message)
```

## MCPCHandler

The `MCPCHandler` class provides client-side functionality:

- `wrap_streams()`: Wraps I/O streams to intercept MCPC messages
- `add_event_listener()`: Add a callback function for MCPC messages
- `check_mcpc_support()`: Check if the server supports MCPC
- `add_metadata()`: Add MCPC metadata to tool arguments
