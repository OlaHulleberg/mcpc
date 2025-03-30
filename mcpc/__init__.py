"""
MCPC (MCP Callback Protocol) extension for MCP clients.
Provides handler for asynchronous tool callbacks following the MCPC protocol.
"""

from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version("mcpc")
except PackageNotFoundError:
    __version__ = "0.1.0"  # Default during development

from .handler import MCPCHandler
from .helper import MCPCHelper
from .models import MCPCMessage, MCPCInformation

__all__ = [
    "MCPCHandler",
    "MCPCMessage",
    "MCPCInformation",
    "MCPCHelper"
]