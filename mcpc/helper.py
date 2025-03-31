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

class MCPCHelper:
    """
    Streamlined helper class for MCPC protocol implementation.
    """
    
    def __init__(self, provider_name: str):
        """Initialize an MCPC helper."""
        self.provider_name = provider_name
        self.background_tasks: Dict[str, Dict[str, Any]] = {}
        logger.debug(f"Initialized MCPC helper for provider: {provider_name}")

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

    async def send_direct(self, message: str) -> bool:
        """Send message directly via stdout using JSON-RPC."""
        try:
            # Create message content and response
            text_content = TextContent(text=message, type="text")
            mcpc_message = {"content": [text_content], "isError": False}
            
            jsonrpc_response = JSONRPCResponse(
                jsonrpc="2.0",
                id="MCPC_CALLBACK",
                result=mcpc_message
            )
            
            # Serialize and send
            json_message = JSONRPCMessage(jsonrpc_response)
            serialized = json_message.model_dump_json()
            
            # Write to stdout and flush
            sys.stdout.write(serialized + "\n")
            sys.stdout.flush()
            
            logger.debug(f"Sent direct message: {serialized[:100]}...")
            return True
            
        except Exception as e:
            logger.error(f"Error sending direct message: {e}")
            return False

    def get_protocol_info(self) -> MCPCInformation:
        """Get MCPC protocol information."""
        return MCPCInformation(mcpc_provider=self.provider_name)