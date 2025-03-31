"""
MCPC (MCP Callback Protocol) handler for processing tool results asynchronously.
This module provides the core functionality for MCPC protocol handling.
"""

import logging
import json
import uuid
from typing import Any, Callable, Dict, Set
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
from mcp.types import TextContent
import asyncio

from .models import MCPCMessage

# Configure logger
logger = logging.getLogger("mcpc")

class MCPCHandler:
    """Handler for MCP Callback Protocol (MCPC) messages."""
    
    def __init__(self, provider_name: str, **config):
        """
        Initialize the MCPC handler.
        
        Args:
            provider_name: Name of the MCP provider
            config: Additional configuration options
        """
        self.provider_name = provider_name
        self.config = config
        self.supports_mcpc = False
        self.protocol_version = None
        
        # Set up logging
        log_level = config.get('log_level', logging.INFO)
        logger.setLevel(log_level)
        
        # Event listeners for MCPC messages
        self._event_listeners: Set[Callable[[MCPCMessage], None]] = set()
        
        logger.info("MCPC handler initialized")
    
    def add_event_listener(self, listener: Callable[[MCPCMessage], None]) -> None:
        """
        Add an event listener for MCPC messages.
        
        Args:
            listener: Callback function that receives validated MCPCMessage objects
        """
        self._event_listeners.add(listener)
        logger.debug("Added MCPC event listener")
    
    def remove_event_listener(self, listener: Callable[[MCPCMessage], None]) -> None:
        """
        Remove an event listener.
        
        Args:
            listener: The listener to remove
        """
        if listener in self._event_listeners:
            self._event_listeners.remove(listener)
            logger.debug("Removed MCPC event listener")
    
    async def _notify_listeners(self, message: MCPCMessage) -> None:
        """
        Notify all listeners of a validated MCPC message asynchronously.
        Listeners are executed in the background without blocking.
        
        Args:
            message: The validated MCPCMessage to notify listeners about
        """
        for listener in self._event_listeners:
            async def notify_listener():
                try:
                    # Handle both sync and async listeners
                    if asyncio.iscoroutinefunction(listener):
                        await listener(message)
                    else:
                        listener(message)
                except Exception as e:
                    logger.error(f"Error in event listener: {e}")
            
            # Fire off the notification without waiting
            asyncio.create_task(notify_listener())
    
    async def wrap_streams(self, reader, writer):
        """
        Wrap streams with event listeners.
        
        This wraps the raw streams with wrappers that notify listeners
        of all data passing through, while allowing MCPC messages to be
        intercepted and processed.
        
        Args:
            reader: The original reader stream
            writer: The original writer stream
            
        Returns:
            tuple: (wrapped_reader, wrapped_writer)
        """
        # Create wrapper classes for the streams
        class WrappedReader(MemoryObjectReceiveStream):
            def __init__(self, original, handler):
                self._original = original
                self._handler = handler
                
            async def receive(self):
                data = await self._original.receive()
                if data:
                    await self._handler._process_stream_data(data)
                return data
                
            # Forward other methods
            def __getattr__(self, name):
                return getattr(self._original, name)
                
        class WrappedWriter(MemoryObjectSendStream):
            def __init__(self, original, handler):
                self._original = original
                self._handler = handler
                
            async def send(self, data):
                await self._original.send(data)
                
            # Forward other methods
            def __getattr__(self, name):
                return getattr(self._original, name)
        
        # Create wrapped streams
        wrapped_reader = WrappedReader(reader, self)
        wrapped_writer = WrappedWriter(writer, self)
        
        logger.debug("Streams wrapped with MCPC event listeners")
        return wrapped_reader, wrapped_writer

    async def _process_stream_data(self, data) -> None:
        """
        Process incoming stream data to extract and validate MCPC messages.
        
        Args:
            data: The raw stream data to process
        """
        try:
            # Early return if data structure is invalid
            if not hasattr(data, 'root') or not hasattr(data.root, 'result'):
                return
            
            contents = data.root.result.get("content")
            if not contents:
                return
            
            for content in contents:
                # Extract text content
                try:
                    mcp_message = TextContent.model_construct(**content).text
                except Exception as e:
                    logger.debug(f"Error extracting text content: {e}")
                    continue

                # Parse and validate MCPC message
                try:
                    mcpc_data = json.loads(mcp_message)
                    mcpc_message = MCPCMessage.model_construct(**mcpc_data)
                except Exception as e:
                    logger.debug(f"Error validating MCPC message: {e}")
                    continue

                # Validate protocol
                if mcpc_message.protocol != "mcpc":
                    logger.debug("Received MCPC message with incorrect protocol")
                    continue

                # Validate required fields based on message type
                if mcpc_message.type == "task":
                    if not all([mcpc_message.session_id, mcpc_message.task_id, mcpc_message.tool_name]):
                        logger.debug("Task message missing required fields")
                        continue
                elif mcpc_message.type != "server_event":
                    continue

                logger.info(f"Processing MCPC callback: type={mcpc_message.type}, event={mcpc_message.event}")
                await self._notify_listeners(mcpc_message)
                    
        except Exception as e:
            logger.error(f"Error processing stream data: {e}")
            logger.debug(f"Data that caused error: {data}")
    
    async def check_mcpc_support(self, session) -> bool:
        """
        Check if the connected MCP server supports MCPC protocol.
        
        Args:
            session: The MCP client session
            
        Returns:
            bool: True if MCPC is supported, False otherwise
        """
        try:
            # Call the MCPC information endpoint
            result = await session.call_tool("is_mcpc_enabled", {})
            
            # Extract MCPC information from the result
            if result and hasattr(result, 'content') and result.content:
                # Parse the MCPC information
                content_text = result.content[0].text if hasattr(result.content[0], "text") else ""
                if content_text:
                    try:
                        mcpc_info = json.loads(content_text)
                    except Exception as e:
                        # Silently ignore non-MCPC messages
                        logger.debug(f"Error parsing MCPC info: {e}")
                        return False

                    self.supports_mcpc = mcpc_info.get("mcpc_enabled", False)
                    self.protocol_version = mcpc_info.get("mcpc_version", None)
                    
                    if self.supports_mcpc:
                        logger.info(f"{self.provider_name} MCPC protocol v{self.protocol_version} supported")
                    return self.supports_mcpc
                    
            return False
        except Exception as e:
            logger.warning(f"Error checking MCPC support: {e}")
            return False
    
    def add_metadata(self, args: Dict[str, Any], session_id: str) -> Dict[str, Any]:
        """
        Add MCPC metadata to tool arguments.
        
        Args:
            args: The original tool arguments
            session_id: The session ID to include in metadata
            
        Returns:
            dict: The modified arguments with MCPC metadata added
        """
        if not self.supports_mcpc:
            return args
            
        args_copy = args.copy() if args else {}
        task_id = str(uuid.uuid4())
        
        # Add MCPC session info to arguments
        if "_metadata" not in args_copy:
            args_copy["_metadata"] = {}
            
        args_copy["_metadata"]["mcpc_session_id"] = session_id
        args_copy["_metadata"]["mcpc_task_id"] = task_id
        
        logger.debug(f"Added MCPC metadata: session={session_id}, task={task_id}")
        return args_copy 