# MCPC - Model Context Protocol Callback

[![PyPI version](https://badge.fury.io/py/mcpc.svg)](https://badge.fury.io/py/mcpc)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python Versions](https://img.shields.io/pypi/pyversions/mcpc.svg)](https://pypi.org/project/mcpc/)

An extension to the MCP (Model-Context-Protocol) protocol that enables asynchronous real-time callbacks and streaming updates from MCP tools.

## Quick Start

### Prerequisites

MCPC extends the [MCP protocol](https://github.com/modelcontextprotocol/python-sdk), so you need to have MCP installed first.

### Installation

UV is the preferred package manager for installing MCPC due to its speed and reliability, but you can use any of your favorite package managers (pip, poetry, conda, etc.) to install and manage MCPC.

```bash
uv add mcpc
```

For projects using traditional pip:

```bash
pip install mcpc
```

### Client Usage

```python
from mcpc import MCPCHandler, MCPCMessage
from mcp import ClientSession
from mcp.client.stdio import stdio_client

# Define your callback function
async def my_mcpc_callback(mcpc_message: MCPCMessage) -> None:
    print(f"Received MCPC message: {mcpc_message}")
    # Handle the message based on status
    if mcpc_message.type == "task" and mcpc_message.event == "complete":
        print(f"Task {mcpc_message.task_id} completed with result: {mcpc_message.result}")

# Initialize the MCPC handler with your callback
mcpc_handler = MCPCHandler("my-provider", my_mcpc_callback)

# In your connection logic:
async def connect_to_mcp():
    # Connect to MCP provider
    transport = await stdio_client(parameters)

    # Wrap the transport with MCPC event listeners
    wrapped_transport = await mcpc_handler.wrap_streams(*transport)

    # Create a ClientSession with the wrapped transport
    session = await ClientSession(*wrapped_transport)

    # Initialize the session
    await session.initialize()

    # Check if MCPC is supported
    mcpc_supported = await mcpc_handler.check_mcpc_support(session)
    if mcpc_supported:
        print(f"MCPC protocol v{mcpc_handler.protocol_version} supported")

    return session

# When calling tools, add MCPC metadata
async def run_tool(session, tool_name, tool_args, session_id):
    # Add MCPC metadata if supported
    enhanced_args = mcpc_handler.add_metadata(tool_args, session_id)

    # Call the tool with enhanced arguments
    return await session.call_tool(tool_name, enhanced_args)
```

## Why MCPC Exists

I created MCPC to solve a critical limitation in LLM tool interactions: **maintaining conversational flow while running background tasks**.

The standard MCP protocol follows a synchronous request-response pattern, which blocks the conversation until a tool completes. This creates poor UX when:

1. You want to chat with an LLM while a long-running task executes
2. You need real-time progress updates from background operations
3. You're running tasks that potentially continue forever (like monitoring)

MCPC addresses these limitations by enabling:

- Continuous conversation with LLMs during tool execution
- Real-time updates from background processes
- Asynchronous notifications when operations complete
- Support for indefinitely running tasks with streaming updates
- LLMs can react to events and take action (e.g., "Database migration finished, let me verify the tables" or "File arrived, I'll start processing it")

For example, you might start a data processing task, continue discussing with the LLM about the expected results, receive progress updates throughout, and get notified when processing completes - all without interrupting the conversation flow.

MCPC also enables powerful interactive patterns that weren't possible before in MCP:

- **Modifying running tasks**: You can adjust parameters or change the behavior of a task while it's running (e.g., "focus on this subset of data instead" or "I see that you're misunderstanding some relations, can you please parse the PDF first?")
- **Tool-initiated prompts**: A tool can ask for clarification when it encounters ambiguity or needs additional input (e.g., "I found multiple matches, which one did you mean?" or "I need additional authorization to proceed")
- **Conversation branching**: Start multiple background tasks and selectively respond to their updates while maintaining conversational context
- **Proactive AI Actions**: Your MCP server can notify the LLM of events, allowing it to take action (e.g., "Database migration completed" → LLM runs verification query → "Table missing" → LLM starts targeted migration)

These capabilities create a much more natural interaction model where tools feel like collaborative participants in the conversation rather than black-box functions.

## How MCPC Works

MCPC extends MCP by:

1. **Adding metadata to tool calls**: Session and task identifiers
2. **Defining a message structure**: Standardized format for callbacks
3. **Providing stream interception**: Monitors I/O streams for MCPC messages
4. **Implementing task management**: Handles background tasks and messaging

The protocol is fully backward compatible with MCP, allowing MCPC-enabled clients to work with standard MCP servers, and vice versa.

## Server Implementation

For implementing MCPC in your MCP servers, use the `MCPCHelper` class to handle message creation, background tasks, and progress updates.

```python
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent
from mcpc import MCPCHelper
import asyncio
import uuid

# Initialize MCPC helper
PROVIDER_NAME = "my-processor"
mcpc = MCPCHelper(PROVIDER_NAME)

async def serve():
    """Run the MCP server with MCPC support."""
    server = Server(PROVIDER_NAME)

    @server.list_tools()
    async def list_tools():
        return [
            Tool(
                name="process_data",
                description="Process data with real-time progress updates.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "data_id": {"type": "string"},
                        "process_type": {"type": "string"}
                    },
                    "required": ["data_id"]
                }
            )
        ]

    @server.call_tool()
    async def call_tool(name, arguments):
        # Extract MCPC metadata
        metadata = arguments.pop("_metadata", {})
        session_id = metadata.get("mcpc_session_id", "default")
        task_id = metadata.get("mcpc_task_id", str(uuid.uuid4()))

        # Handle MCPC protocol info request
        if name == "is_mcpc_enabled":
            info = mcpc.get_protocol_info()
            return [TextContent(type="text", text=info.model_dump_json())]

        # Handle the tool call
        if name == "process_data":
            data_id = arguments.get("data_id")

            # Define the background task that will provide real-time updates
            async def process_data_task():
                try:
                    # Send initial update
                    await mcpc.send_direct(mcpc.create_message(
                        type="task",
                        event="update",
                        tool_name="process_data",
                        session_id=session_id,
                        task_id=task_id,
                        result="Starting data processing"
                    ).model_dump_json())

                    # Simulate work with progress updates
                    total_steps = 5
                    for step in range(1, total_steps + 1):
                        # Send progress update
                        await mcpc.send_direct(mcpc.create_message(
                            type="task",
                            event="update",
                            tool_name="process_data",
                            session_id=session_id,
                            task_id=task_id,
                            result={
                                "status": f"Processing step {step}/{total_steps}",
                                "progress": step / total_steps * 100
                            }
                        ).model_dump_json())

                        # Simulate work
                        await asyncio.sleep(1)

                    # Send completion message
                    await mcpc.send_direct(mcpc.create_message(
                        type="task",
                        event="complete",
                        tool_name="process_data",
                        session_id=session_id,
                        task_id=task_id,
                        result={
                            "status": "Complete",
                            "data_id": data_id,
                            "summary": "Processing completed successfully"
                        }
                    ).model_dump_json())

                except Exception as e:
                    # Send error message
                    await mcpc.send_direct(mcpc.create_message(
                        type="task",
                        event="failed",
                        tool_name="process_data",
                        session_id=session_id,
                        task_id=task_id,
                        result=f"Error: {str(e)}"
                    ).model_dump_json())

                finally:
                    # Clean up task
                    mcpc.cleanup_task(task_id)

            # Start the background task
            mcpc.start_task(task_id, process_data_task)

            # Return immediate response
            response = mcpc.create_message(
                type="task",
                event="created",
                tool_name="process_data",
                session_id=session_id,
                task_id=task_id,
                result=f"Started processing data_id={data_id}. Updates will stream in real-time."
            )

            return [TextContent(type="text", text=response.model_dump_json())]

    # Start the server
    options = server.create_initialization_options()
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, options)

if __name__ == "__main__":
    asyncio.run(serve())
```

## Advanced Server Features

The `MCPCHelper` class provides additional features for complex server implementations:

1. **Task Management**

   - `start_task()`: Run a background task with automatic thread management
   - `check_task()`: Get the status of a running task
   - `stop_task()`: Request a task to stop gracefully
   - `cleanup_task()`: Remove a completed task from tracking

2. **Message Creation**

   - `create_message()`: Create standardized MCPC protocol messages
   - `send_direct()`: Send messages directly to clients over stdout

3. **Protocol Information**
   - `get_protocol_info()`: Return MCPC protocol compatibility information

## MCPC Message Structure

MCPC messages have the following structure:

```python
class MCPCMessage:
    session_id: str      # Unique session identifier
    task_id: str | None  # Unique task identifier (required for task messages)
    tool_name: str | None # Name of the tool being called (required for task messages)
    result: Any = None   # Result or update data
    event: str          # Event type (restricted for task messages)
    type: Literal["task", "server_event"] = "task"  # Type of message
    protocol: str = "mcpc"  # Protocol identifier
```

### Example Server Event Message

```python
# Server-initiated Kafka notification
server_event = mcpc.create_server_event(
    session_id="session123",
    result={
        "topic": "user_updates",
        "event": "user_created",
        "user_id": "user456",
        "timestamp": "2024-03-20T10:00:00Z"
    },
    event="notification"  # Event must be explicitly specified
)
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
- **Proactive AI Responses**: Server events could trigger LLM actions:
  - System events ("Database migration finished" → LLM verifies tables)
  - File events ("PDF arrived" → LLM starts processing)
  - Task results ("Analysis complete" → LLM reviews findings)
  - State changes ("API updated" → LLM tests new endpoints)
  - Any event that might require AI attention or action

## Use Cases

MCPC is ideal for:

- **Interactive AI Agents**: Chat with LLMs while tasks run in the background
- **Data Processing**: Stream progress updates during large file processing
- **Content Generation**: Receive partial results as they're generated
- **Long-Running Operations**: Support for tasks that run indefinitely
- **Distributed Systems**: Coordinate asynchronous operations across services
- **Proactive AI**: Let LLMs respond to events and take action automatically
- **Automated Workflows**: Create self-managing systems that adapt to events
- **Intelligent Monitoring**: AI agents that actively respond to system changes

## Compatibility

MCPC is designed to be fully backward compatible with the MCP protocol:

- MCPC-enabled clients can communicate with standard MCP servers
- MCPC-enabled servers can respond to standard MCP clients
- The protocol negotiation ensures graceful fallback to standard MCP when needed

## License

MIT
