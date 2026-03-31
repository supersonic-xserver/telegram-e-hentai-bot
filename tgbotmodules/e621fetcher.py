#!/usr/bin/env python3
"""
e621 Fetcher Module — SSX Dungeon Channel Moderation

Async e621/e6ai API fetch with stealth jitter.
Bucket worker pattern: drains pending_q, fetches API, pushes to evaluate_q.

SSX Architecture:
- 4 fetcher workers (application.create_task pattern)
- Rate limiting: 0.5s floor (e621 TOS: max 2 req/sec)
- Stealth jitter from generalcfg.ENABLE_STEALTH_MODE
"""

import asyncio
import logging
import random

import httpx

from tgbotmodules.spidermodules import generalcfg

logger = logging.getLogger(__name__)

# =======================================================================
# API ENDPOINTS
# =======================================================================
E621_API_BASE = "https://e621.net/posts/{post_id}.json"
E6AI_API_BASE = "https://e6ai.net/posts/{post_id}.json"

# e621 TOS: max 2 req/sec — with 4 workers = 0.5s min between each
_RATE_FLOOR = 0.5


async def fetch_post(session: httpx.AsyncClient, post_id: str, source: str = "e621") -> dict | None:
    """
    Fetch single post from e621 or e6ai API.
    Applies stealth jitter matching existing ENABLE_STEALTH_MODE behavior.
    
    Args:
        session: httpx AsyncClient instance
        post_id: The e621 post ID to fetch
        source: "e621" or "e6ai"
        
    Returns:
        Enriched post record or None if fetch failed
    """
    base = E6AI_API_BASE if source == "e6ai" else E621_API_BASE
    url = base.format(post_id=post_id)

    # Stealth jitter — mirrors ENABLE_STEALTH_MODE from generalcfg
    if generalcfg.ENABLE_STEALTH_MODE:
        jitter = random.uniform(generalcfg.JITTER_MIN, generalcfg.JITTER_MAX)
        await asyncio.sleep(max(_RATE_FLOOR, jitter))
    else:
        await asyncio.sleep(_RATE_FLOOR)

    try:
        resp = await session.get(
            url,
            timeout=10,
            headers={"User-Agent": generalcfg.E621_USER_AGENT}
        )
        
        if resp.status_code == 404:
            logger.warning("[e621fetch] Post %s not found (404)", post_id)
            return None
        
        resp.raise_for_status()
        data = resp.json()
        post = data.get("post", {})

        # Flatten all tag categories into one set
        tags_flat = set()
        for category in post.get("tags", {}).values():
            if isinstance(category, list):
                tags_flat.update(category)

        return {
            "post_id": post_id,
            "tags_flat": tags_flat,
            "rating": post.get("rating", ""),
            "file_ext": post.get("file", {}).get("ext", ""),
            "file_url": post.get("file", {}).get("url", ""),
            "sources": post.get("sources", []),
            "flags": post.get("flags", {}),
        }

    except httpx.HTTPError as e:
        logger.error("[e621fetch] HTTP error for post %s: %s", post_id, e)
        return None
    except Exception as e:
        logger.error("[e621fetch] Unexpected error for post %s: %s", post_id, e)
        return None


async def fetcher_worker(
    pending_q: asyncio.Queue,
    evaluate_q: asyncio.Queue,
    worker_id: int
) -> None:
    """
    Bucket worker: drain pending_q, fetch API, push to evaluate_q.
    
    Args:
        pending_q: Queue of post records waiting to be fetched
        evaluate_q: Queue for fetched posts ready for evaluation
        worker_id: Identifier for this worker (for logging)
    """
    async with httpx.AsyncClient(follow_redirects=True) as session:
        while True:
            record = await pending_q.get()
            
            if record is None:  # shutdown sentinel
                pending_q.task_done()
                logger.info("[e621fetch worker %d] Received shutdown signal", worker_id)
                break

            post_id = record["post_id"]
            source = record.get("source", "e621")
            
            logger.debug("[e621fetch worker %d] Fetching post %s from %s", 
                        worker_id, post_id, source)
            
            enriched = await fetch_post(session, post_id, source)

            if enriched:
                # Preserve original message context
                enriched.update({
                    k: record[k] for k in ("message_id", "chat_id", "sender_id")
                })
                await evaluate_q.put(enriched)
                logger.debug("[e621fetch worker %d] Queued post %s for evaluation", 
                            worker_id, post_id)
            else:
                logger.warning("[e621fetch worker %d] Failed to fetch post %s, skipping", 
                               worker_id, post_id)

            pending_q.task_done()