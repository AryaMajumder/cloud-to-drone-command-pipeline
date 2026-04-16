"""
fanout.py — Fan-out expansion. Processor role only.

Responsibility: expand one multi-target intent into N per-drone envelopes.

Why this lives in Processor, not Producer:
    Fan-out is a reasoning step — one piece of intent becomes N concrete
    work items. That is exactly what processors do.

    Producers create intent and put it on the bus. They do not reason about
    how that intent should be distributed. A future AI producer, a task-
    based producer, or a scheduled producer all share the same invariant:
    "I have intent. Here it is." They do not decide who receives it.

    If fan-out lived in Producer, every future expansion of reasoning
    (task decomposition, AI planning, conditional routing) would also
    creep into Producer. This module ensures that boundary stays clean.

Rules:
    - Called only from within the Processor role.
    - Each expanded envelope has its own command_id.
    - All sibling envelopes share a group_id (observability only).
    - The Dispatcher never knows fan-out happened — it only ever sees
      single-drone CommandEnvelopes from the DispatchQueue.
    - This module has zero AWS dependencies. Fully unit-testable.
"""

import uuid
import logging
from typing import Any, Dict, List

from command_envelope import CommandEnvelope, wrap_payload

logger = logging.getLogger(__name__)


class FanOutError(Exception):
    """Raised when a fan-out payload is structurally invalid."""
    pass


def is_fanout(payload: Dict[str, Any]) -> bool:
    """
    Return True if this payload requires fan-out expansion.

    Decision rule: 'target_ids' is a list with 2 or more entries.

    Single-drone commands use 'target_id' (str).
    Multi-drone commands use 'target_ids' (list).

    The Processor calls this to branch between single and fan-out paths.
    """
    target_ids = payload.get("target_ids")
    return isinstance(target_ids, list) and len(target_ids) >= 2


def validate_fanout_payload(payload: Dict[str, Any]) -> None:
    """
    Validate that a fan-out payload is well-formed before expansion.

    Raises FanOutError on any violation.
    """
    if "target_id" in payload:
        raise FanOutError(
            "Payload contains both 'target_id' and 'target_ids'. "
            "Use 'target_id' for single-drone commands, "
            "'target_ids' (list) for fan-out. Remove 'target_id'."
        )

    target_ids = payload.get("target_ids")

    if target_ids is None:
        raise FanOutError("Fan-out payload must contain 'target_ids' (list).")

    if not isinstance(target_ids, list):
        raise FanOutError(
            f"'target_ids' must be a list, got {type(target_ids).__name__}."
        )

    if len(target_ids) < 2:
        raise FanOutError(
            f"'target_ids' must contain at least 2 entries for fan-out "
            f"(got {len(target_ids)}). Use single-target path for 1 drone."
        )

    for i, tid in enumerate(target_ids):
        if not isinstance(tid, str) or not tid.strip():
            raise FanOutError(
                f"'target_ids[{i}]' must be a non-empty string, got {repr(tid)}."
            )

    if not payload.get("action"):
        raise FanOutError("Fan-out payload must contain a non-empty 'action'.")


def expand(envelope: CommandEnvelope) -> List[CommandEnvelope]:
    """
    Expand a multi-target CommandEnvelope into N per-drone envelopes.

    Called by the Processor after structural validation passes.

    Input envelope payload shape:
        {
            "action":     "RTL",
            "target_ids": ["drone-01", "drone-02"],
            "params":     {}          # optional, copied to each
        }

    Output: one CommandEnvelope per drone, each shaped identically to a
    single-drone command the Dispatcher already knows how to handle:
        {
            "target_id": "<drone-id>",
            "action":    "RTL",
            "params":    {}
        }

    Each output envelope:
    - Has its own unique command_id
    - Shares a group_id with all siblings (for DynamoDB observability only)
    - Is indistinguishable from a directly-issued single-drone command
      from the Dispatcher's perspective

    Invariant: fan-out is invisible below the Processor layer.
    """
    validate_fanout_payload(envelope.payload)

    target_ids: List[str] = envelope.payload["target_ids"]
    action: str           = envelope.payload["action"]
    params: Dict          = envelope.payload.get("params", {})

    # One group_id ties all sibling commands together.
    # Stored in DynamoDB for batch status queries. No routing logic uses it.
    group_id = str(uuid.uuid4())

    logger.info(
        "Fan-out expansion: origin_command_id=%s action=%s targets=%s group_id=%s",
        envelope.command_id, action, target_ids, group_id,
    )

    envelopes: List[CommandEnvelope] = []
    for drone_id in target_ids:
        child = wrap_payload(
            payload={
                "target_id": drone_id,
                "action":    action,
                "params":    params,
            },
            group_id=group_id,
        )
        envelopes.append(child)
        logger.info(
            "  → command_id=%s target=%s group_id=%s",
            child.command_id, drone_id, group_id,
        )

    return envelopes