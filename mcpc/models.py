from pydantic import BaseModel
from typing import Any

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
    task_id: str
    tool_name: str
    result: Any | None = None
    status: str
    type: str = "mcpc"