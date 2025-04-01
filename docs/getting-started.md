# Getting Started with MCPC

## Prerequisites

MCPC extends the [MCP protocol](https://github.com/modelcontextprotocol/python-sdk), so you need to have MCP installed first.

## Installation

UV is the preferred package manager for installing MCPC due to its speed and reliability, but you can use any of your favorite package managers (pip, poetry, conda, etc.) to install and manage MCPC.

```bash
uv add mcpc
```

For projects using traditional pip:

```bash
pip install mcpc
```

## Client Usage

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
    mcpc_supported = await mcpc_handler.init_mcpc(session)
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
