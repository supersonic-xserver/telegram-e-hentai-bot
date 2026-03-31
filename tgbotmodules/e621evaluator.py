#!/usr/bin/env python3
"""
e621 Evaluator Module — SSX Dungeon Channel Moderation

Pure evaluation logic. Imports rules from e621ruleset (single source of truth).
No I/O - only applies rules and returns verdict.

SSX Architecture:
- 4 evaluator workers (application.create_task pattern)
- Receives enriched post records from fetcher workers
- Applies ssX Dungeon ruleset, pushes verdict to action_q
"""

import asyncio
import logging

from tgbotmodules.e621ruleset import (
    BLACKLIST_ILLEGAL,
    BLACKLIST_COMMUNITY,
    TAG_AI_GENERATED,
    TAG_AI_ASSISTED,
)

logger = logging.getLogger(__name__)


def evaluate_post(record: dict) -> tuple[str, list[str]]:
    """
    Apply ssX Dungeon ruleset to a post record.
    
    SSX Dungeon Rules (in order of precedence):
    1. Illegal/Exploitative tags -> DELETE (from safety_filter core)
    2. Community standard (real-person) -> DELETE
    3. AI art: ai_generated alone -> DELETE
    4. Otherwise -> KEEP
    
    Args:
        record: Enriched post record from fetcher
        
    Returns:
        Tuple of (verdict: "KEEP" or "DELETE", reasons: list of strings)
    """
    tags = record.get("tags_flat", set())
    ext = record.get("file_ext", "").lower()

    # =======================================================================
    # RULE 1: Illegal / Exploitative Content
    # =======================================================================
    # Imports from safety_filter.py via e621ruleset
    hits = tags & BLACKLIST_ILLEGAL
    if hits:
        return "DELETE", [f"illegal_tag:{tag}" for tag in sorted(hits)]

    # =======================================================================
    # RULE 2: Community Standard (Real-Person Content)
    # =======================================================================
    hits = tags & BLACKLIST_COMMUNITY
    if hits:
        return "DELETE", [f"community_tag:{tag}" for tag in sorted(hits)]

    # =======================================================================
    # RULE 3: AI Art Rule
    # =======================================================================
    # ai_generated alone -> DELETE (lazy AI dump, no human edit)
    # ai_generated + ai_assisted -> KEEP (human collaborated)
    # ai_assisted alone -> KEEP (explicitly allowed by ssX rules)
    has_ai_generated = TAG_AI_GENERATED in tags
    has_ai_assisted = TAG_AI_ASSISTED in tags
    
    if has_ai_generated and not has_ai_assisted:
        return "DELETE", ["ai_generated_no_human_edit"]

    # =======================================================================
    # DEFAULT: Pass
    # =======================================================================
    return "KEEP", []


async def evaluator_worker(
    evaluate_q: asyncio.Queue,
    action_q: asyncio.Queue,
    worker_id: int
) -> None:
    """
    Bucket worker: drain evaluate_q, apply ruleset, push verdict to action_q.
    
    Args:
        evaluate_q: Queue of enriched post records ready for evaluation
        action_q: Queue for posts with verdicts ready for execution
        worker_id: Identifier for this worker (for logging)
    """
    while True:
        record = await evaluate_q.get()
        
        if record is None:  # shutdown sentinel
            evaluate_q.task_done()
            logger.info("[e621eval worker %d] Received shutdown signal", worker_id)
            break

        post_id = record.get("post_id", "UNKNOWN")
        
        # Apply ruleset
        verdict, reasons = evaluate_post(record)
        record["verdict"] = verdict
        record["reasons"] = reasons

        if verdict == "DELETE":
            logger.info(
                "[e621eval worker %d] DELETE post %s — %s",
                worker_id, post_id, reasons
            )
        else:
            logger.debug(
                "[e621eval worker %d] KEEP post %s",
                worker_id, post_id
            )

        # Push to action queue for executor
        await action_q.put(record)
        evaluate_q.task_done()