## Basic Server Usage (Standard MCP SDK Server)

```python
import sys
from mcp import stdio_server, Tool
from mcp.server import Server
from mcpc import MCPCHelper, MCPCToolParameters
from typing import Dict, Any, List
import uuid
import asyncio
import logging

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s - %(name)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stderr)]
)
logger = logging.getLogger("mcp_data_process")

# Initialize MCPC helper
server = Server("my-provider")
mcpc = MCPCHelper(server)

@server.list_tools()
async def list_tools() -> List[Tool]:
    return [
        Tool(
            name="process_data",
            description="Processes data",
            inputSchema={
                "type": "object",
                "properties": {
                    "message": {
                        "type": "string",
                        "description": "The message content to screen"
                    }
                },
                "required": ["message"]
            }
        )
    ]

@server.call_tool()
async def call_tool(name: str, arguments: Dict[str, Any]) -> List[TextContent]:
    # Standard MCP SDK Servers still have to initialize their own _mcp_init - unfortunately
    if name == "_mcpc_init":
        mcpc_info = arguments.get("mcpc_info", {})
        return mcpc.handle_protocol_info_request(mcpc_info)

    mcpc_params = MCPCToolParameters(**arguments.pop("mcpc_params", {}))
    data_id = str(uuid.uuid4())

    if name == "process_data":
        async def process_data_task():
            yield mcpc.create_task_event(
                event="update",
                tool_name="process_data",
                session_id=mcpc_params.session_id,
                task_id=mcpc_params.task_id,
                result="Processing data..."
            )
            await asyncio.sleep(3) # Simulate processing time
            yield mcpc.create_task_event(
                event="complete",
                tool_name="process_data",
                session_id=mcpc_params.session_id,
                task_id=mcpc_params.task_id,
                result={
                    "data_id": data_id,
                    "processed_data": "Processed data"
                }
            )
        # Start a background task
        collected_messages = await mcpc.start_task(mcpc_params.task_id, process_data_task)

    # For standard MCP clients, return collected complete/failed messages
    if collected_messages:
        return mcpc.messages_to_text_content(collected_messages) # Standard MCP SDK Server requires results to be wrapped in TextContent

    # For MCPC clients, return immediate acknowledgment
    response = mcpc.create_task_event(
        event="created",
        tool_name="process_data",
        session_id=session_id,
        task_id=task_id,
        result=f"Started processing data_id={data_id}. Updates will stream in real-time."
    )
    return mcpc.messages_to_text_content([response]) # Standard MCP SDK Server requires results to be wrapped in TextContent

if __name__ == "__main__":
    async def start():
        options = server.create_initialization_options()
        async with stdio_server() as (read_stream, write_stream):
            await server.run(read_stream, write_stream, options, raise_exceptions=True)
    # Open streams
    asyncio.run(start())
```
