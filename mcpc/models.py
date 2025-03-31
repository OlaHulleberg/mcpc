from pydantic import BaseModel
from typing import Any, Literal

from . import __version__

class MCPCInformation(BaseModel):
    """Information about MCPC protocol."""
    mcpc_enabled: bool = True
    mcpc_version: str = __version__
    mcpc_provider: str
    mcpc_direct_updates: bool = True

class MCPCMessage(BaseModel):
    """A message in the MCPC protocol."""
    session_id: str
    task_id: str | None = None
    tool_name: str | None = None
    result: Any = None
    event: str
    type: Literal["task", "server_event"] = "task"  # Type of message
    protocol: str = "mcpc"  # Protocol identifier