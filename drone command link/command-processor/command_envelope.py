"""
Minimal command schema.
Structure without behavior.

group_id: set when a command is part of a fan-out batch.
          All sibling commands share the same group_id.
          None for single-target commands.
          Observability only — no routing logic reads it.
"""

from dataclasses import dataclass, field
from typing import Dict, Any, Optional
from datetime import datetime
import uuid


@dataclass
class CommandEnvelope:
    """
    Command envelope — defines identity and traceability.

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
    origin:     str
    payload:    Dict[str, Any]
    group_id:   Optional[str] = field(default=None)

    def to_dict(self) -> Dict[str, Any]:
        d = {
            "command_id": self.command_id,
            "created_at": self.created_at,
            "origin":     self.origin,
            "payload":    self.payload,
        }
        if self.group_id is not None:
            d["group_id"] = self.group_id
        return d

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CommandEnvelope":
        return cls(
            command_id=data["command_id"],
            created_at=data["created_at"],
            origin=data["origin"],
            payload=data["payload"],
            group_id=data.get("group_id"),   # backwards-compatible
        )


def wrap_payload(payload: Dict[str, Any],
                 group_id: Optional[str] = None) -> "CommandEnvelope":
    """
    Producer function: wrap input intent into a structured envelope.

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
    - Optionally tags with group_id (set by the fan-out Processor, never by Producer)
    """
    return CommandEnvelope(
        command_id=str(uuid.uuid4()),
        created_at=datetime.utcnow().isoformat() + "Z",
        origin="cloud",
        payload=payload,
        group_id=group_id,
    )