#!/usr/bin/env python3
"""
MCPC Protocol Helper Functions

Provides streamlined functionality for implementing the MCPC protocol
in MCP servers using direct streaming capabilities.
"""

import logging
import sys
import threading
import asyncio
import time
from typing import Any, Callable, Dict, Literal, List, Optional

from mcp import Tool
from mcp.types import TextContent, JSONRPCResponse, JSONRPCMessage, ListToolsRequest, ListToolsResult
from mcp.server import FastMCP, Server
from .models import MCPCMessage, MCPCInformation
# Configure logging
logger = logging.getLogger("mcpc")

# Define transport types
TransportType = Literal["stdio", "sse"]

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
        self._collected_messages: List[MCPCMessage] = []        

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

    def set_client_mcpc_version(self, version: str) -> None:
        """
        Set the MCPC protocol version supported by the client.
        
        Args:
            version: The MCPC protocol version string
        """
        self.client_mcpc_version = version
        logger.info(f"Client MCPC protocol version set to: {version}")

    def _set_client_mcpc_info(self, client_info: MCPCInformation) -> None:
        """
        Set the MCPC protocol information received from the client.
        
        Args:
            client_info: The client's MCPC information object
        """
        self.client_mcpc_version = client_info.mcpc_version
        client_provider = client_info.mcpc_provider
        logger.info(f"Client MCPC protocol version set to: {self.client_mcpc_version}, provider: {client_provider}")

    def start_task(
        self,
        task_id: str, 
        worker_func: Callable, 
        args: tuple = (), 
        kwargs: dict = None,
        timeout: float = 60.0
    ) -> List[MCPCMessage]:
        """
        Start a task, either asynchronously (MCPC client) or synchronously (standard MCP client).
        
        Args:
            task_id: Unique identifier for the task
            worker_func: The function to execute
            args: Positional arguments for the function
            kwargs: Keyword arguments for the function
            timeout: Maximum time to wait for task completion in seconds (for standard MCP only)
            
        Returns:
            List[MCPCMessage]: Collected messages if standard MCP client, empty list if MCPC client
        """
        kwargs = kwargs or {}
        self._collected_messages = []  # Reset collection
        
        is_async = asyncio.iscoroutinefunction(worker_func)
        is_standard_mcp = self.client_mcpc_version is None
        
        # Define the thread worker that will run in all cases
        def thread_worker():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                if is_async:
                    loop.run_until_complete(worker_func(*args, **kwargs))
                else:
                    worker_func(*args, **kwargs)
            finally:
                # Clean up task after completion
                self.cleanup_task(task_id)
                loop.close()
        
        # Create and start thread
        thread = threading.Thread(target=thread_worker, daemon=True)
        self.background_tasks[task_id] = {
            "thread": thread,
            "start_time": time.time(),
            "status": "running"
        }
        thread.start()
        logger.debug(f"Started task {task_id} in background thread")
        
        # For standard MCP clients, wait for completion and collect results
        if is_standard_mcp:
            logger.debug(f"Waiting for task {task_id} to complete (standard MCP client, timeout: {timeout}s)")
            thread.join(timeout=timeout)
            
            # Check if thread is still alive after timeout
            if thread.is_alive():
                logger.warning(f"Task {task_id} timed out after {timeout} seconds")
                if task_id in self.background_tasks:
                    self.background_tasks[task_id]["status"] = "timeout"
            
            # Get collected messages
            collected = self._collected_messages.copy()
            self._collected_messages = []  # Clear after copying
            return collected
        
        # For MCPC clients, return immediately
        logger.debug("Returning immediately for MCPC client")
        return []  # Empty list indicates async operation

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
        Send an MCPC message through the appropriate transport or collect it for standard MCP client.
        
        Args:
            message: The MCPCMessage to send
            
        Returns:
            bool: Success status
        """
        # Ensure message has required fields
        if message.type == "task" and not all([message.session_id, message.task_id, message.tool_name]):
            raise ValueError("Task messages must include session_id, task_id, and tool_name")
        elif message.type == "server_event" and not message.session_id:
            raise ValueError("Server event messages must include session_id")
            
        try:
            # For standard MCP clients, collect messages instead of sending
            if self.client_mcpc_version is None:
                # Only collect complete messages for task type
                if message.type == "task" and (message.event == "complete" or message.event == "failed"):
                    self._collected_messages.append(message)
                    logger.debug(f"Collected {message.event} message for standard MCP client")
                return True
            
            # For MCPC clients, send as normal
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
            logger.error(f"Error preparing message for send: {e}")
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