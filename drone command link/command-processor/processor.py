"""
Processor - Structural integrity only.
No behavior. No policy. Just shape.
"""

import logging
from typing import Dict, Any
from command_envelope import CommandEnvelope

logger = logging.getLogger(__name__)


class StructuralError(Exception):
    """Command structure is invalid"""
    pass


def validate_structure(command: CommandEnvelope) -> None:
    """
    Guarantee structural integrity.
    
    Does NOT:
    - Enforce allowed commands
    - Inspect PX4 semantics
    - Validate payload contents
    - Inject policy
    
    Only checks:
    - Required envelope fields exist
    - Fields are correct type
    """
    # Envelope fields must exist
    if not command.command_id:
        raise StructuralError("command_id is required")
    
    if not command.created_at:
        raise StructuralError("created_at is required")
    
    if not command.origin:
        raise StructuralError("origin is required")
    
    if command.payload is None:
        raise StructuralError("payload is required")
    
    # Payload must be dict (but we don't inspect contents)
    if not isinstance(command.payload, dict):
        raise StructuralError("payload must be a dictionary")
    
    # That's it. Structure preserved. No behavior imposed.


def process(command: CommandEnvelope) -> CommandEnvelope:
    """
    Process command: validate structure, pass through unchanged.
    
    This is your structural firewall.
    Future insertion point for:
    - Expiry checking
    - Rate limiting
    - Audit logging
    - Replay queuing
    
    But today: just structure.
    """
    logger.info(f"Processing command: {command.command_id}")
    
    # Validate structure
    validate_structure(command)
    
    logger.info(f"Command structure valid: {command.command_id}")
    
    # Pass through unchanged
    return command