#!/usr/bin/env python3
"""
MCPC Protocol Helper Functions

Provides streamlined functionality for implementing the MCPC protocol
in MCP servers using direct streaming capabilities.
"""

import inspect
import logging
import sys
import asyncio
import time
from typing import Any, Callable, Dict, Literal, List, Optional

from mcp import Tool
from mcp.types import TextContent, JSONRPCResponse, JSONRPCMessage
from mcp.server import FastMCP, Server
from .models import MCPCMessage, MCPCInformation
# Configure logging
logger = logging.getLogger("mcpc")

class MCPCHelper:
    """
    Streamlined helper class for MCPC protocol implementation.
    """
    
    def __init__(self, server: Server | FastMCP):
        """
        Initialize an MCPC helper.
        
        Args:
            server: MCP server instance
        """
        self._write_stream = sys.stdout
        self.provider_name = server.name
        self.background_tasks: Dict[str, Dict[str, Any]] = {}
        self.client_mcpc_version: Optional[str] = None  

        # Hide mcpc tools and params from the LLM - Aplpied on FastMCP only
        def filter_mcpc_tools(tools: list[Tool]) -> list[Tool]:
            filtered_tools = [tool for tool in tools if tool.name != "_mcpc_init"]
            for tool in filtered_tools:
                params = tool.parameters if hasattr(tool, 'parameters') else tool.inputSchema
                params['properties'] = {k: v for k, v in params.get('properties', {}).items() if k != "mcpc_params"}
            return filtered_tools

        # FastMCP lets us register tools dynamically 😍
        if isinstance(server, FastMCP):
            @server.tool()
            async def _mcpc_init(mcpc_info: dict):
                return self.handle_protocol_info_request(mcpc_info)
            
            original_list_tools = server._tool_manager.list_tools
            def wrapped_list_tools():
                tools = original_list_tools()
                return filter_mcpc_tools(tools)
            server._tool_manager.list_tools = wrapped_list_tools
        logger.debug(f"Initialized MCPC helper for provider: {self.provider_name}")

    def _set_client_mcpc_info(self, client_info: MCPCInformation) -> None:
        """
        Set the MCPC protocol information received from the client.
        
        Args:
            client_info: The client's MCPC information object
        """
        self.client_mcpc_version = client_info.mcpc_version
        client_provider = client_info.mcpc_provider
        logger.info(f"Client MCPC protocol version set to: {self.client_mcpc_version}, provider: {client_provider}")

    async def start_task(
        self,
        task_id: str, 
        worker_func: Callable, 
        args: tuple = (), 
        kwargs: dict = None,
        timeout: float = 60.0
    ) -> List[MCPCMessage]:
        """
        Start a task with a generator function that yields messages.
        
        Args:
            task_id: Unique identifier for the task
            worker_func: Generator function that yields MCPCMessage objects
            args: Positional arguments for the function
            kwargs: Keyword arguments for the function
            timeout: Maximum time to wait for task completion in seconds (for standard MCP only)
            
        Returns:
            List[MCPCMessage]: Collected messages if standard MCP client, empty list if MCPC client
        """
        kwargs = kwargs or {}
        collected_messages = []
        is_async = asyncio.iscoroutinefunction(worker_func) or inspect.isasyncgenfunction(worker_func)

        is_standard_mcp = self.client_mcpc_version is None
        
        async def process_messages():
            try:
                if is_async:
                    async for message in worker_func(*args, **kwargs):
                        if not isinstance(message, MCPCMessage):
                            raise ValueError("Worker function must yield MCPCMessage objects")
                        if is_standard_mcp:
                            collected_messages.append(message)
                        else:
                            await self.send(message)
                else:
                    for message in worker_func(*args, **kwargs):
                        if not isinstance(message, MCPCMessage):
                            raise ValueError("Worker function must return MCPCMessage objects")
                        if is_standard_mcp:
                            collected_messages.append(message)
                        else:
                            await self.send(message)
            finally:
                self.cleanup_task(task_id)
        
        # Create and start task
        loop = asyncio.get_event_loop()
        task = loop.create_task(process_messages())
        self.background_tasks[task_id] = {
            "task": task,
            "start_time": time.time(),
            "status": "running"
        }
        
        # For standard MCP clients, wait for completion and return collected results
        if is_standard_mcp:
            try:
                await asyncio.wait_for(task, timeout=timeout)
            except asyncio.TimeoutError:
                logger.warning(f"Task {task_id} timed out after {timeout} seconds")
                if task_id in self.background_tasks:
                    self.background_tasks[task_id]["status"] = "timeout"
            return collected_messages
        
        # For MCPC clients, return immediately
        return []

    def check_task(self, task_id: str) -> Dict[str, Any]:
        """Check task status and information."""
        task_info = self.background_tasks.get(task_id, {})
        
        if task_info:
            thread = task_info.get("thread")
            if thread:
                task_info["is_running"] = thread.is_alive()
            
        return task_info
        
    def stop_task(self, task_id: str) -> bool:
        """Request task stop. Returns success status."""
        if task_id in self.background_tasks:
            self.background_tasks[task_id]["status"] = "stopping"
            return True
        return False

    def cleanup_task(self, task_id: str) -> None:
        """Remove task from registry."""
        if task_id in self.background_tasks:
            self.background_tasks.pop(task_id, None)
            logger.debug(f"Cleaned up task {task_id}")

    def create_message(
        self,
        type: Literal["task", "server_event"],
        event: str,
        session_id: str | None = None,
        tool_name: str | None = None,
        task_id: str | None = None,
        result: Any = None
    ) -> MCPCMessage:
        """Create a standardized MCPC message."""
        if not event:
            raise ValueError("event is required for all messages")
        if type == "server_event":
            if not session_id:
                raise ValueError("session_id is required for server event messages")
            return self.create_server_event(
                event=event,
                session_id=session_id,
                result=result,
            )
        else:
            if not all([tool_name, session_id, task_id]):
                raise ValueError("tool_name, session_id, and task_id are required for task messages")
            return self.create_task_event(
                event=event,
                tool_name=tool_name,
                session_id=session_id,
                task_id=task_id,
                result=result,
            )

    def create_server_event(
        self,
        event: str,
        session_id: str,
        result: Any
    ) -> MCPCMessage:
        """Create a server-initiated event message."""
        if not event:
            raise ValueError("event is required for server event messages")
        return MCPCMessage(
            type="server_event",
            event=event,
            session_id=session_id,
            result=result
        )
    
    def create_task_event(
        self,
        event: Literal["created", "update", "complete", "failed"],
        tool_name: str,
        session_id: str,
        task_id: str,
        result: Any = None
    ) -> MCPCMessage:
        """Create a task-initiated event message."""
        return MCPCMessage(
            type="task",
            event=event,
            session_id=session_id,
            task_id=task_id,
            tool_name=tool_name,
            result=result
        )
    
    async def send(self, message: MCPCMessage) -> bool:
        """
        Send a single MCPCMessage directly through the transport.
        
        Args:
            message: The MCPCMessage to send
            
        Returns:
            bool: Success status
        """
        try:
            # Ensure message has required fields
            if message.type == "task" and not all([message.session_id, message.task_id, message.tool_name]):
                raise ValueError("Task messages must include session_id, task_id, and tool_name")
            elif message.type == "server_event" and not message.session_id:
                raise ValueError("Server event messages must include session_id")

            # Convert message to JSON string
            message_json = message.model_dump_json()
            
            # Create message content and response
            text_content = TextContent(text=message_json, type="text")
            mcpc_message = {"content": [text_content], "isError": False}
            
            jsonrpc_response = JSONRPCResponse(
                jsonrpc="2.0",
                id="MCPC_CALLBACK",
                result=mcpc_message
            )
            
            # Serialize
            json_message = JSONRPCMessage(jsonrpc_response)
            
            # Route to the appropriate transport
            return await self._send_direct(json_message)
        except Exception as e:
            logger.error(f"Error sending direct message: {e}")
            return False

    async def _send_direct(self, message: JSONRPCMessage) -> bool:
        """
        Send a pre-formatted JSON-RPC message directly via stdout.
        
        Args:
            message: The serialized JSON-RPC message to send
            
        Returns:
            bool: Success status
        """
        try:
            # Write to stdout and flush
            serialized = message.model_dump_json()
            self._write_stream.write(f"{serialized}\n")
            self._write_stream.flush()
            logger.debug(f"Sent direct message: {serialized[:100]}...")
            return True
            
        except Exception as e:
            logger.error(f"Error sending direct message: {e}")
            return False
        
    def messages_to_text_content(self, messages: List[MCPCMessage]) -> List[TextContent]:
        """
        Convert a list of MCPCMessage objects to a list of TextContent objects.
        
        Args:
            messages: List of MCPCMessage objects to convert
            
        Returns:
            List[TextContent]: A list of TextContent objects containing serialized messages
        """
        return [TextContent(type="text", text=message.model_dump_json()) for message in messages]

    def get_protocol_info(self) -> MCPCInformation:
        """Get MCPC protocol information."""
        return MCPCInformation(mcpc_provider=self.provider_name)
        
    def handle_protocol_info_request(self, client_info: dict) -> List[TextContent]:
        """
        Handle the _is_mcpc_enabled tool call to verify MCPC protocol support.
        
        Args:
            client_info: The client's MCPC information object as a dictionary
            
        Returns:
            List[TextContent]: A response for the MCP protocol
        """
        try:
            # Process the client's MCPC information
            mcpc_info = MCPCInformation.model_construct(**client_info)
            self._set_client_mcpc_info(mcpc_info)
                
            # Return server protocol information
            info = self.get_protocol_info()
            return [TextContent(type="text", text=info.model_dump_json())]
        except Exception as e:
            logger.error(f"Error handling protocol info request: {e}")
            return [TextContent(type="text", text=f"Error: {str(e)}")]