#!/usr/bin/env python3
"""
MCPC Protocol Helper Functions

Provides streamlined functionality for implementing the MCPC protocol
in MCP servers using direct streaming capabilities.
"""

import logging
import threading
import asyncio
import sys
import time
from typing import Any, Callable, Dict, Literal

from mcp.types import TextContent, JSONRPCResponse, JSONRPCMessage

from .models import MCPCMessage, MCPCInformation

# Configure logging
logger = logging.getLogger("mcpc")

# Define transport types
TransportType = Literal["stdio", "sse"]

class MCPCHelper:
    """
    Streamlined helper class for MCPC protocol implementation.
    """
    
    def __init__(self, provider_name: str, transport_type: TransportType):
        """
        Initialize an MCPC helper.
        
        Args:
            provider_name: Name of the provider
            transport_type: Transport method to use for sending messages ("stdio" or "sse")
        """
        self.provider_name = provider_name
        self.transport_type = transport_type
        self.background_tasks: Dict[str, Dict[str, Any]] = {}
        logger.debug(f"Initialized MCPC helper for provider: {provider_name} using {transport_type} transport")

        # TODO: Implement SSE transport
        if self.transport_type == "sse":
            raise NotImplementedError("SSE transport is not yet implemented")

    def start_task(
        self,
        task_id: str, 
        worker_func: Callable, 
        args: tuple = (), 
        kwargs: dict = None
    ) -> None:
        """Start a background task."""
        kwargs = kwargs or {}
        
        # Handle async vs sync functions
        if asyncio.iscoroutinefunction(worker_func):
            def async_wrapper():
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    loop.run_until_complete(worker_func(*args, **kwargs))
                finally:
                    loop.close()
            target_func = async_wrapper
        else:
            def sync_wrapper():
                return worker_func(*args, **kwargs)
            target_func = sync_wrapper
        
        # Create and start thread
        thread = threading.Thread(target=target_func, daemon=True)
        self.background_tasks[task_id] = {
            "thread": thread,
            "start_time": time.time(),
            "status": "running"
        }
        thread.start()
        logger.debug(f"Started task {task_id}")
        
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
        if type == "server_event":
            if not session_id:
                raise ValueError("session_id is required for server event messages")
            return MCPCMessage(
                session_id=session_id,
                result=result,
                event=event,
                type="server_event"
            )
        else:
            if not all([tool_name, session_id, task_id]):
                raise ValueError("tool_name, session_id, and task_id are required for task messages")
            if event not in ["created", "update", "complete", "failed"]:
                raise ValueError("task messages must use one of: created, update, complete, failed")
            return MCPCMessage(
                session_id=session_id,
                task_id=task_id,
                tool_name=tool_name,
                result=result,
                event=event,
                type="task"
            )

    def create_server_event(
        self,
        session_id: str,
        result: Any,
        event: str
    ) -> MCPCMessage:
        """Create a server-initiated event message."""
        return self.create_message(
            type="server_event",
            event=event,
            session_id=session_id,
            result=result
        )

    async def send(self, message: MCPCMessage) -> bool:
        """
        Send an MCPC message through the appropriate transport.
        Routes to the configured transport type (stdio or sse).
        
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
            serialized = json_message.model_dump_json()
            
            # Route to the appropriate transport
            if self.transport_type == "stdio":
                return await self._send_direct(serialized)
            elif self.transport_type == "sse":
                raise NotImplementedError("SSE transport is not yet implemented")
            else:
                raise ValueError(f"Unsupported transport type: {self.transport_type}")
            
        except Exception as e:
            logger.error(f"Error preparing message for send: {e}")
            return False

    async def _send_direct(self, message: str) -> bool:
        """
        Send a pre-formatted JSON-RPC message directly via stdout.
        
        Args:
            message: The serialized JSON-RPC message to send
            
        Returns:
            bool: Success status
        """
        try:
            # Write to stdout and flush
            sys.stdout.write(message + "\n")
            sys.stdout.flush()
            
            logger.debug(f"Sent direct message: {message[:100]}...")
            return True
            
        except Exception as e:
            logger.error(f"Error sending direct message: {e}")
            return False

    def get_protocol_info(self) -> MCPCInformation:
        """Get MCPC protocol information."""
        return MCPCInformation(mcpc_provider=self.provider_name)