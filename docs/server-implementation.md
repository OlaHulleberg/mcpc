# Server Implementation Guide

MCPC servers enable real-time updates, progress notifications, and asynchronous responses through your standard MCP transport - no additional transport mechanisms needed.

For implementing MCPC in your MCP servers, use the `MCPCHelper` class to handle message creation, background tasks, and progress updates. The helper automatically manages bidirectional communication through your existing MCP transport while maintaining backward compatibility.

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
        if name == "_is_mcpc_enabled":
            # Use the helper method to handle the protocol info request
            client_info = arguments.get("mcpc_info")
            return mcpc.handle_protocol_info_request(client_info)

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

            # Start the background task or run it synchronously based on client support
            collected_messages = mcpc.start_task(task_id, process_data_task)

            # For standard MCP clients, return collected complete/failed messages
            if collected_messages:
                # Convert all collected messages to TextContent objects
                return mcpc.messages_to_text_content(collected_messages)

            # For MCPC clients, return immediate acknowledgment
            response = mcpc.create_message(
                type="task",
                event="created",
                tool_name="process_data",
                session_id=session_id,
                task_id=task_id,
                result=f"Started processing data_id={data_id}. Updates will stream in real-time."
            )

            # Also return through the standard MCP channel
            return [TextContent(type="text", text=response.model_dump_json())]

    # Start the server
    options = server.create_initialization_options()
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, options)

if __name__ == "__main__":
    asyncio.run(serve())
```
