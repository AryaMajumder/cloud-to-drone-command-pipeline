"""
Processor — Structural integrity + intent reasoning.

Two responsibilities, in order:

1. Structural validation (unchanged)
   Guarantee the envelope is well-formed before any reasoning happens.

2. Intent expansion (new: fan-out)
   If the payload targets multiple drones, expand into N per-drone
   envelopes. Each child envelope is structurally identical to what a
   single-drone Producer would have created. The Dispatcher never knows
   fan-out happened.

Why fan-out lives here and not in Producer:
    Producer invariant: "I have intent. Here it is."
    Processor invariant: "I reasoned about that intent."

    Fan-out is reasoning — one intent becomes N work items.
    Future processors (task decomposition, AI planning, rate limiting,
    safety gating) belong here for the same reason.

    If fan-out lived in Producer, all future reasoning would creep there
    too. Keeping it here preserves the boundary.

What Processor is NOT allowed to do:
    - Talk to devices
    - Know MQTT exists
    - Reinterpret intent (it may reject or expand, never rewrite meaning)
    - Make policy decisions about what commands are allowed
"""

import logging
from typing import Dict, Any, List, Union

from command_envelope import CommandEnvelope
from fanout import is_fanout, expand, FanOutError

logger = logging.getLogger(__name__)


class StructuralError(Exception):
    """Command structure is invalid."""
    pass


# ─────────────────────────────────────────────────────────────────────────────
# Structural validation (unchanged from original)
# ─────────────────────────────────────────────────────────────────────────────

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
    if not command.command_id:
        raise StructuralError("command_id is required")

    if not command.created_at:
        raise StructuralError("created_at is required")

    if not command.origin:
        raise StructuralError("origin is required")

    if command.payload is None:
        raise StructuralError("payload is required")

    if not isinstance(command.payload, dict):
        raise StructuralError("payload must be a dictionary")

    # That's it. Structure preserved. No behavior imposed.


# ─────────────────────────────────────────────────────────────────────────────
# Process entry point
# ─────────────────────────────────────────────────────────────────────────────

def process(command: CommandEnvelope) -> Union[CommandEnvelope, List[CommandEnvelope]]:
    """
    Process a command envelope.

    Step 1 — structural validation (always)
    Step 2 — intent reasoning (if needed)

    Returns:
    - Single CommandEnvelope  →  single-drone command, ready for DispatchQueue
    - List[CommandEnvelope]   →  fan-out expanded, each ready for DispatchQueue

    The caller (lambda_function) is responsible for routing the output
    to the DispatchQueue, using the correct MessageGroupId per envelope.

    Future reasoning steps (task decomposition, AI planning, rate limiting)
    slot in here as additional branches or pipeline stages. They all share
    the same contract: receive one CommandEnvelope, return one or many.
    """
    logger.info("Processing command: %s", command.command_id)

    # ── Step 1: Structural validation ────────────────────────────────────────
    validate_structure(command)
    logger.info("✓ Structure valid: %s", command.command_id)

    # ── Step 2: Intent reasoning ──────────────────────────────────────────────
    if is_fanout(command.payload):
        logger.info(
            "Fan-out detected for command_id=%s targets=%s",
            command.command_id,
            command.payload.get("target_ids"),
        )
        children = expand(command)
        logger.info(
            "✓ Expanded command_id=%s → %d child envelopes",
            command.command_id, len(children),
        )
        return children

    # Single-drone command — pass through unchanged
    logger.info("Single-target command: %s", command.command_id)
    return command