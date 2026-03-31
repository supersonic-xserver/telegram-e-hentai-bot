#!/usr/bin/env python3
"""
e621 Executor Module — SSX Dungeon Channel Moderation

Serialized execution of moderation actions.
Single worker (no parallelism) for Telegram flood protection.
Deletes violating messages, logs all actions via userdatastore.

SSX Architecture:
- 1 executor worker (serialized for flood protection)
- Telegram API rate limiting: 0.5s between calls
- Logs to mod channel if configured
- Persists mod log via userdatastore
"""

import asyncio
import logging
from datetime import datetime

from telegram import Bot
from telegram.error import TelegramError

from tgbotmodules import userdatastore

logger = logging.getLogger(__name__)

# Telegram API flood protection
_DELETE_RATE = 0.5


def _reason_summary(reasons: list[str]) -> str:
    """Generate short summary of delete reasons for logging."""
    if not reasons:
        return "no_reason"
    return ", ".join(r.replace("_", " ") for r in reasons[:3])


def _format_mod_log_entry(record: dict) -> dict:
    """
    Format a moderation action for the mod log.
    
    Args:
        record: The post record with verdict and reasons
        
    Returns:
        Dict formatted for userdatastore mod log
    """
    return {
        "timestamp": datetime.now().isoformat(),
        "action": record.get("verdict", "UNKNOWN"),
        "post_id": record.get("post_id", "UNKNOWN"),
        "reasons": record.get("reasons", []),
        "chat_id": record.get("chat_id", "UNKNOWN"),
        "message_id": record.get("message_id", "UNKNOWN"),
        "sender_id": record.get("sender_id", "UNKNOWN"),
    }


async def executor_worker(
    action_q: asyncio.Queue,
    bot,
    mod_channel_id: int | None = None
) -> None:
    """
    Single serialized worker — do NOT parallelize.
    Deletes violating messages, logs all actions.
    
    Args:
        action_q: Queue of post records with verdicts
        bot: Telegram Bot instance
        mod_channel_id: Optional channel for mod notifications
    """
    while True:
        record = await action_q.get()
        
        if record is None:  # shutdown sentinel
            action_q.task_done()
            logger.info("[e621exec] Received shutdown signal")
            break

        verdict = record.get("verdict", "KEEP")
        message_id = record.get("message_id")
        chat_id = record.get("chat_id")
        post_id = record.get("post_id", "UNKNOWN")
        reasons = record.get("reasons", [])

        if verdict == "DELETE" and message_id and chat_id:
            try:
                # Delete the message from Telegram
                await bot.delete_message(
                    chat_id=chat_id,
                    message_id=message_id
                )
                
                # Flood protection
                await asyncio.sleep(_DELETE_RATE)
                
                logger.info(
                    "[e621exec] Deleted post %s from chat %s — %s",
                    post_id, chat_id, _reason_summary(reasons)
                )
                
                # Send notification to mod channel if configured
                if mod_channel_id:
                    try:
                        summary = _reason_summary(reasons)
                        await bot.send_message(
                            chat_id=mod_channel_id,
                            text=f"🗑️ Removed e621 post {post_id}\nReason: {summary}"
                        )
                    except TelegramError as e:
                        logger.warning("[e621exec] Failed to send mod channel notification: %s", e)
                
            except TelegramError as e:
                logger.error(
                    "[e621exec] Failed to delete message %s from chat %s: %s",
                    message_id, chat_id, e
                )
        
        # Log the action regardless of outcome
        log_entry = _format_mod_log_entry(record)
        userdatastore.append_mod_log(log_entry)
        
        if verdict == "KEEP":
            logger.debug("[e621exec] Kept post %s (no action taken)", post_id)

        action_q.task_done()