# MCPC - Model Context Protocol Callback

[![PyPI version](https://badge.fury.io/py/mcpc.svg)](https://badge.fury.io/py/mcpc)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python Versions](https://img.shields.io/pypi/pyversions/mcpc.svg)](https://pypi.org/project/mcpc/)

An extension to the MCP (Model-Context-Protocol) protocol that enables asynchronous real-time callbacks and streaming updates from MCP tools.

## Compatibility Matrix

| Feature                           | Status               | Notes                                               |
| --------------------------------- | -------------------- | --------------------------------------------------- |
| STDIO Transport                   | ✅ Implemented       | Full support for standard input/output transport    |
| SSE Transport                     | ⚠️ Limited Support   | Standard MCP operations only, MCPC features pending |
| MCPC Server → Standard MCP Client | ✅ Implemented       | Full backward compatibility                         |
| Standard MCP Client → MCPC Server | ⚠️ Partially Working | Task initiation works, result streaming in progress |

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

# Define your event listener function
async def my_mcpc_listener(mcpc_message: MCPCMessage) -> None:
    print(f"Received MCPC message: {mcpc_message}")
    # Handle the message based on status
    if mcpc_message.type == "task" and mcpc_message.event == "complete":
        print(f"Task {mcpc_message.task_id} completed with result: {mcpc_message.result}")

# Initialize the MCPC handler
mcpc_handler = MCPCHandler("my-provider")

# Add your event listener for MCPCMessage
mcpc_handler.add_event_listener(my_mcpc_listener)

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

## What is MCPC?

MCPC is an **extension** to the MCP protocol, not a replacement. It builds upon the existing MCP infrastructure to add real-time callback capabilities while maintaining full compatibility with standard MCP implementations.

### Key Points

- MCPC extends MCP, it does not replace it
- MCP servers can optionally add MCPC support
- MCP clients can optionally add MCPC support
- You can mix and match MCPC-enabled and standard MCP components

This means you can:

- Use an MCPC-enabled server with standard MCP clients for standard MCP capabilities.
- Use an MCPC-enabled client with standard MCP servers for standard MCP capabilities.
- Use MCPC-enabled components on both sides for full real-time capabilities

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

# Initialize MCPC helper with stdio transport
PROVIDER_NAME = "my-processor"
mcpc = MCPCHelper(PROVIDER_NAME, transport_type="stdio")

# Or use SSE transport when available in future releases
# mcpc = MCPCHelper(PROVIDER_NAME, transport_type="sse")  # Not yet implemented

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
                    await mcpc.send(mcpc.create_message(
                        type="task",
                        event="update",
                        tool_name="process_data",
                        session_id=session_id,
                        task_id=task_id,
                        result="Starting data processing"
                    ))

                    # Simulate work with progress updates
                    total_steps = 5
                    for step in range(1, total_steps + 1):
                        # Send progress update
                        await mcpc.send(mcpc.create_message(
                            type="task",
                            event="update",
                            tool_name="process_data",
                            session_id=session_id,
                            task_id=task_id,
                            result={
                                "status": f"Processing step {step}/{total_steps}",
                                "progress": step / total_steps * 100
                            }
                        ))

                        # Simulate work
                        await asyncio.sleep(1)

                    # Send completion message
                    await mcpc.send(mcpc.create_message(
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
                    ))

                except Exception as e:
                    # Send error message
                    await mcpc.send(mcpc.create_message(
                        type="task",
                        event="failed",
                        tool_name="process_data",
                        session_id=session_id,
                        task_id=task_id,
                        result=f"Error: {str(e)}"
                    ))

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

            # Send the initial response message
            await mcpc.send(response)

            # Also return through the standard MCP channel
            # This is optional but provides maximum compatibility
            return [TextContent(type="text", text=response.model_dump_json())]

    # Start the server
    options = server.create_initialization_options()
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, options)

if __name__ == "__main__":
    asyncio.run(serve())
```

## MCPC Messaging API

The MCPC helper provides a simple API for creating and sending messages:

```python
# Initialize with the desired transport
mcpc = MCPCHelper("my-provider", transport_type="stdio")  # "sse" planned for future releases

# Create a task message
task_message = mcpc.create_message(
    type="task",
    event="update",  # one of: created, update, complete, failed
    tool_name="tool_name",
    session_id="session_123",
    task_id="task_456",
    result="Processing data..."  # can be any JSON-serializable object
)

# Create a server event message
server_event = mcpc.create_message(
    type="server_event",
    event="database_updated",
    session_id="session_123",
    result={"tables": ["users", "products"]}
)

# Or use the shorthand for server events
server_event = mcpc.create_server_event(
    session_id="session_123",
    event="database_updated",
    result={"tables": ["users", "products"]}
)

# Send a message through the configured transport
await mcpc.send(task_message)
```

The `send` method:

1. Validates required fields based on message type
2. Prepares the JSON-RPC format
3. Routes the message through the appropriate transport

This abstracts away the complexity of the JSON-RPC protocol, allowing you to focus on your application logic.

## Advanced Server Features

The `MCPCHelper` class provides additional features for complex server implementations:

1. **Transport Options**

   - Initialize with different transport types: `
