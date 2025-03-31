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
logger = logging.getLogger(__name__)

class MCPCHandler:
    """Handler for MCP Callback Protocol (MCPC) messages."""
    
    def __init__(self, provider_name: str, callback_fn: Callable[[MCPCMessage], None], **config):
        """
        Initialize the MCPC handler.
        
        Args:

            callback_fn: Async function that receives validated MCPC messages
            config: Additional configuration options
        """
        self.provider_name = provider_name
        self.callback_fn = callback_fn
        self.config = config
        self.supports_mcpc = False
        self.protocol_version = None
        
        # Set up logging
        log_level = config.get('log_level', logging.INFO)
        logger.setLevel(log_level)
        
        # Event listeners for stream data
        self._event_listeners: Set[Callable[[str, Any], None]] = set()
        
        # Register the MCPC callback listener by default
        self._mcpc_listener = self._mcpc_message_listener
        self.add_event_listener(self._mcpc_listener)
        
        logger.info("MCPC handler initialized")
    
    def add_event_listener(self, listener: Callable[[str, Any], None]) -> None:
        """Add an event listener for stream data."""
        self._event_listeners.add(listener)
        logger.debug("Added event listener to MCPC handler")
    
    def remove_event_listener(self, listener: Callable[[str, Any], None]) -> None:
        """Remove an event listener."""
        if listener in self._event_listeners:
            self._event_listeners.remove(listener)
            logger.debug("Removed event listener from MCPC handler")
    
    async def _notify_listeners(self, direction: str, data: Any) -> None:
        """Notify all listeners of stream data."""
        for listener in self._event_listeners:
            try:
                # Handle both sync and async listeners
                if asyncio.iscoroutinefunction(listener):
                    await listener(direction, data)
                else:
                    listener(direction, data)
            except Exception as e:
                logger.error(f"Error in event listener: {e}")
    
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
                    await self._handler._notify_listeners('received', data)
                return data
                
            # Forward other methods
            def __getattr__(self, name):
                return getattr(self._original, name)
                
        class WrappedWriter(MemoryObjectSendStream):
            def __init__(self, original, handler):
                self._original = original
                self._handler = handler
                
            async def send(self, data):
                if data:
                    await self._handler._notify_listeners('sent', data)
                await self._original.send(data)
                
            # Forward other methods
            def __getattr__(self, name):
                return getattr(self._original, name)
        
        # Create wrapped streams
        wrapped_reader = WrappedReader(reader, self)
        wrapped_writer = WrappedWriter(writer, self)
        
        logger.debug("Streams wrapped with MCPC event listeners")
        return wrapped_reader, wrapped_writer
    
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
    
    async def _mcpc_message_listener(self, direction: str, data) -> None:
        """
        Process MCPC callbacks from stream data.
        
        This function checks if incoming messages conform to the MCPC protocol,
        and if so, calls the user's callback function with the validated message.
        
        Args:
            direction: 'sent' or 'received'
            data: The data being sent or received
        """
        # Only process received data
        if direction != "received":
            return
        
        try:
            if not hasattr(data, 'root') or not hasattr(data.root, 'result'):
                return
            
            contents = data.root.result.get("content")
            if not contents:
                return
            
            for content in contents:
                try:
                    mcp_message = TextContent.model_construct(**content)
                    mcp_message = mcp_message.text
                except Exception as e:
                    # Silently ignore non-text MCP messages
                    logger.debug(f"Error extracting text content: {e}")
                    continue

                try:
                    # Parse and validate as MCPC message
                    mcpc_data = json.loads(mcp_message)
                    mcpc_message = MCPCMessage.model_construct(**mcpc_data)

                    if mcpc_message.protocol != "mcpc":
                        logger.debug("Received MCPC message with incorrect protocol")
                        continue
                        
                    # Handle both task and server event messages
                    if mcpc_message.type == "server_event":
                        # Server events don't require session_id
                        pass
                    else:
                        if not all([mcpc_message.session_id, mcpc_message.task_id, mcpc_message.tool_name]):
                            logger.debug("Task message missing required fields")
                            continue
                            
                except Exception as e:
                    # Silently ignore non-MCPC messages
                    logger.debug(f"Error validating MCPC message: {e}")
                    continue

                # Only check session_id for task messages
                if mcpc_message.type == "task" and not mcpc_message.session_id:
                    logger.debug("Task message missing session_id")
                    continue
                
                logger.info(f"Processing MCPC callback: type={mcpc_message.type}, event={mcpc_message.event}")
            
                # Call the user's callback function with the validated message
                try:
                    if asyncio.iscoroutinefunction(self.callback_fn):
                        await self.callback_fn(mcpc_message)
                    else:
                        self.callback_fn(mcpc_message)
                except Exception as e:
                    logger.error(f"Error in user callback function: {e}")
                    # Log the full message for debugging
                    logger.debug(f"Message that caused error: {mcpc_message.model_dump_json()}")
                    
        except Exception as e:
            logger.error(f"Error in MCPC message listener: {e}")
            # Log the full data for debugging
            logger.debug(f"Data that caused error: {data}")
    
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