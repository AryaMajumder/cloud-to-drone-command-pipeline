"""
Minimal command schema.
Structure without behavior.
"""

from dataclasses import dataclass
from typing import Dict, Any
from datetime import datetime
import uuid
import json


@dataclass
class CommandEnvelope:
    """
    Command envelope - defines identity and traceability.
    
    Does NOT enforce:
    - Allowed intents
    - Parameter semantics
    - PX4 logic
    
    Only enforces:
    - Structure exists
    - Fields are present
    """
    command_id: str
    created_at: str
    origin: str
    payload: Dict[str, Any]
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dict for transport"""
        return {
            "command_id": self.command_id,
            "created_at": self.created_at,
            "origin": self.origin,
            "payload": self.payload
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'CommandEnvelope':
        """Deserialize from dict"""
        return cls(
            command_id=data["command_id"],
            created_at=data["created_at"],
            origin=data["origin"],
            payload=data["payload"]
        )


def wrap_payload(payload: Dict[str, Any]) -> CommandEnvelope:
    """
    Producer function: Turn input into structured envelope.
    
    Does NOT:
    - Interpret intent
    - Validate business logic
    - Restrict command types
    - Translate protocols
    
    Only:
    - Generates command_id
    - Adds timestamp
    - Adds origin marker
    - Wraps in consistent structure
    """
    return CommandEnvelope(
        command_id=str(uuid.uuid4()),
        created_at=datetime.utcnow().isoformat() + "Z",
        origin="cloud",
        payload=payload
    )