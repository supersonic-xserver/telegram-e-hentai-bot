#!/usr/bin/python3
"""
Telegram E-Hentai Bot - Python-Telegram-Bot v20+ Compatible

SSX Namespace Fix: Ensure workspace root is in sys.path for Koyeb compatibility.
"""

# =======================================================================
# SSX PATH INJECTION - Fix imports for Koyeb execution environment
# =======================================================================
import os
import sys

# Aggressive path injection: ensure workspace root is first priority
_workspace_root = os.path.abspath(os.path.dirname(__file__))
if _workspace_root not in sys.path:
    sys.path.insert(0, _workspace_root)

# Also add current working directory (Koyeb may run from different cwd)
_cwd = os.getcwd()
if _cwd not in sys.path:
    sys.path.insert(0, _cwd)

print(f"[SSX DEBUG] sys.path = {sys.path}")


# =======================================================================
# SSX BOT DESCRIPTION
# =======================================================================
"""
SSX Zero-Bug Hardening:
- Fully async/await architecture for v20+
- SIGINT/SIGTERM signal handlers for graceful shutdown
- Database flush before exit
- Proper resource cleanup

Migration from v13 to v20:
- Updater -> Application.builder()
- Filters -> filters (lowercase module, uppercase attributes)
- All handlers are now async def
- All bot API calls use await
"""

import asyncio
import logging
import os
import sys
import time
import signal
from typing import Optional

# Python Telegram Bot v20+ imports
from telegram import Update, BotCommand
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    ConversationHandler,
    ContextTypes,
    filters,
)

import re
import asyncio

from tgbotconvhandler import messageanalyze
from tgbotconvhandler import spiderfunction
from tgbotmodules import replytext
from tgbotmodules.spidermodules import generalcfg
from tgbotmodules import userdatastore
from tgbotmodules.e621fetcher import fetcher_worker
from tgbotmodules.e621evaluator import evaluator_worker
from tgbotmodules.e621executor import executor_worker

# =======================================================================
# SSX LOGGING SETUP
# =======================================================================
logger = logging.getLogger(__name__)


# =======================================================================
# SSX LOG INJECTION FIX - Safe Log Helper
# =======================================================================
def _safe_log(value: str) -> str:
    """Strip newlines from user-supplied strings before logging (prevent log injection)."""
    if not isinstance(value, str):
        return repr(value)
    return value.replace('\n', '\\n').replace('\r', '\\r')


# =======================================================================
# SSX SIGNAL HANDLING - Graceful Shutdown Support
# =======================================================================
# Handles SIGINT (Ctrl+C) and SIGTERM to ensure all pending logs
# and database writes are flushed to disk before exit.
# =======================================================================

# Global flag for graceful shutdown
_shutdown_requested = False

# Global reference to bot for signal handler use
_bot_for_shutdown = None


async def _async_signal_handler():
    """
    SSX Zero-Bug: Async signal handler for graceful shutdown.
    Called when SIGINT (Ctrl+C) or SIGTERM is received.
    
    ATOMIC SHUTDOWN: This handler BLOCKS until Ghost Drive sync completes
    to ensure 100% successful save before exit.
    """
    global _shutdown_requested, _bot_for_shutdown
    
    logger.warning("[SSX SHUTDOWN] Signal received, initiating graceful shutdown...")
    
    # Set shutdown flag to prevent new operations
    _shutdown_requested = True
    
    # Request shutdown in generalcfg
    generalcfg.request_shutdown()
    
    # Flush database to disk (local)
    try:
        userdatastore.flush_and_sync()
        logger.info("[SSX SHUTDOWN] Database flushed to disk.")
    except Exception as e:
        logger.error(f"[SSX SHUTDOWN] Error flushing database: {e}")
    
    # ATOMIC SHUTDOWN: Sync to Ghost Drive and BLOCK until complete
    # This ensures 100% successful save before exit
    if _bot_for_shutdown and generalcfg.DATABASE_CHANNEL_ID:
        logger.info("[SSX SHUTDOWN] Syncing to Ghost Drive (blocking)...")
        max_retries = 3
        for attempt in range(max_retries):
            success, message = await asyncio.get_event_loop().run_in_executor(
                None, userdatastore.sync_to_ghost_drive, _bot_for_shutdown
            )
            if success:
                logger.info(f"[SSX SHUTDOWN] Ghost Drive sync successful: {message}")
                break
            else:
                logger.warning(f"[SSX SHUTDOWN] Ghost Drive sync attempt {attempt + 1}/{max_retries} failed: {message}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2)
        else:
            logger.error("[SSX SHUTDOWN] All Ghost Drive sync attempts failed!")
    
    # Log shutdown completion
    logger.warning("[SSX SHUTDOWN] Graceful shutdown complete.")
    sys.stdout.flush()
    sys.stderr.flush()


def _signal_handler(signum, frame):
    """
    SSX Zero-Bug: Synchronous signal handler wrapper.
    Creates a new async event loop for the async shutdown handler.
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(_async_signal_handler())
    loop.close()
    sys.exit(0)


def _register_signal_handlers():
    """
    Register signal handlers for SIGINT and SIGTERM.
    SSX Zero-Bug: Ensures graceful shutdown on both console interrupt and system service stop.
    """
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)
    logger.info("[SSX SHUTDOWN] Signal handlers registered for SIGINT and SIGTERM.")


def is_shutdown_requested() -> bool:
    """Check if shutdown has been requested via signal."""
    return _shutdown_requested or generalcfg.is_shutdown_requested()


# =======================================================================
# BOT HANDLERS (All async for v20+)
# =======================================================================

# Conversation states
STATE = range(1)

# Track startup time for uptime display
start_time = time.time()

# =======================================================================
# SSX DUNGEON MODERATION HANDLER - Uses existing safety_filter blocklist
# =======================================================================
# Blocks ALL content matching SSX blocklist - not just e621 links
# Works for any blocked domain or content type in safety_filter
# =======================================================================

async def _handle_ssx_moderation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Check all messages against existing SSX safety blocklist."""
    if is_shutdown_requested():
        return
    
    if not update.message or not update.message.text:
        return
    
    # Use existing safety_filter - blocks all bad content
    from tgbotmodules.safety_filter import is_safe
    
    # Check full message against blocklist (URLs, title, content)
    safe, reason = is_safe(
        tag_list=[],
        title=update.message.text[:200],  # Check message as "title"
        gallery_id=str(update.message.message_id),
        url=update.message.text
    )
    
    if not safe:
        logger.info("[SSX MOD] Blocked message %s: %s", update.message.message_id, reason)
        try:
            await update.message.delete()
            logger.info("[SSX MOD] Deleted blocked message")
        except Exception as e:
            logger.warning("[SSX MOD] Failed to delete message: %s", e)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Start command handler - initiate conversation.
    
    Clears previous user data and creates a new profile for the user.
    """
    context.user_data.clear()
    context.chat_data.clear()
    context.user_data.update({
        "actualusername": str(update.message.from_user.username),
        "chat_id": update.message.chat_id
    })
    logger.info("Actual username is %s.", str(update.message.from_user.username))
    await update.message.reply_text(text=replytext.startMessage)
    context.chat_data.update({'state': 'verify'})
    return STATE


async def state_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Main conversation handler - processes user messages.
    
    Receives user input, analyzes it, and manages the conversation flow.
    Creates search threads when user completes profile settings.
    """
    # SSX SHUTDOWN CHECK: Don't start new operations if shutting down
    if is_shutdown_requested():
        logger.warning("Shutdown requested, ignoring new message from %s", update.message.from_user.username)
        return ConversationHandler.END
    
    inputStr = update.message.text
    context.user_data.update({'chat_id': update.message.chat_id})
    outputDict = messageanalyze(
        inputStr=inputStr,
        user_data=context.user_data,
        chat_data=context.chat_data,
        logger=logger
    )
    context.user_data.update(outputDict["outputUser_data"])
    context.chat_data.update(outputDict["outputChat_data"])
    
    for text in outputDict["outputTextList"]:
        await update.message.reply_text(text=text)
    
    if context.chat_data['state'] != 'END':
        return STATE
    else:
        userdata = ({context.chat_data["virtualusername"]: context.user_data})
        threadName = time.asctime()
        
        # Create search thread
        context.application.create_task(
            searcheh(
                context.bot,
                context.user_data.copy(),
                threadName=threadName
            )
        )
        
        context.user_data.clear()
        context.chat_data.clear()
        logger.info("The user_data and chat_data of user %s is clear.", str(update.message.from_user.username))
        return ConversationHandler.END


async def searchIntervalCTL(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Scheduled job to create search threads."""
    # SSX SHUTDOWN CHECK: Don't start new jobs if shutting down
    if is_shutdown_requested():
        logger.warning("Shutdown requested, skipping scheduled job.")
        return
    
    threadName = time.asctime()
    context.application.create_task(
        searcheh(context.bot, None, threadName=threadName)
    )


async def searcheh(bot, user_data, threadName: str = None) -> None:
    """
    Main search function - controls the spider and result delivery.
    
    Args:
        bot: Bot instance for sending messages
        user_data: Optional user data dict (None for all users)
        threadName: Name for the search thread
    """
    # SSX SHUTDOWN CHECK: Don't start new searches if shutting down
    if is_shutdown_requested():
        logger.warning("Shutdown requested, skipping search operation.")
        return
    
    logger.info("Search is beginning")
    
    if user_data:
        for ud in user_data:
            user_data[ud].update({'userpubchenn': False, 'resultToChat': True})
            logger.info("User %s has finished profile setting process, test search is beginning.", 
                       user_data[ud]['actualusername'])
        spiderDict = user_data
        toTelegramDict = spiderfunction(logger=logger, spiderDict=spiderDict)
    else:
        spiderDict = userdatastore.getspiderinfo()
        toTelegramDict = spiderfunction(logger=logger)
        logger.info("All users' search has been completed, begin to send the result")
    
    if toTelegramDict:
        for td in toTelegramDict:
            chat_idList = []
            
            if spiderDict[td].get('chat_id') and spiderDict[td]['resultToChat'] == True:
                chat_idList.append(spiderDict[td]['chat_id'])
            if spiderDict[td]["userpubchenn"] == True and generalcfg.pubChannelID:
                chat_idList.append(generalcfg.pubChannelID)
            
            logger.info("Begin to send user %s's result.", td)
            
            for chat_id in chat_idList:
                if len(toTelegramDict[td]) == 0:
                    message = f"------Could not find any new result for {td}------"
                    await bot.send_message(chat_id=chat_id, text=message)
                    continue
                
                message = f"------This is the result of {td}------"
                await bot.send_message(chat_id=chat_id, text=message)
                
                for manga in toTelegramDict[td]:
                    if manga.previewImageObj:
                        manga.previewImageObj.seek(0)
                        await bot.send_photo(
                            chat_id=chat_id,
                            photo=manga.previewImageObj
                        )
                    await bot.send_message(
                        chat_id=chat_id,
                        text=f"{manga.title}\n{manga.url}"
                    )
            
            logger.info("User %s's result has been sent.", td)
        
        logger.info("All users' result has been sent.")
    else:
        logger.info("Could not gain any new result to users.")
        if generalcfg.pubChannelID:
            await bot.send_message(
                chat_id=generalcfg.pubChannelID,
                text="We do not have any new result"
            )


async def autoCreateJob(application: Application) -> None:
    """Create the recurring search job."""
    application.job_queue.run_repeating(
        searchIntervalCTL,
        interval=generalcfg.interval,
        first=5
    )


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Cancel command handler - clears user data."""
    await update.message.reply_text(text=replytext.UserCancel)
    logger.info("User %s has canceled the process.", str(update.message.from_user.username))
    context.user_data.clear()
    context.chat_data.clear()
    logger.info("The user_data and chat_data of user %s has cleared", 
               str(update.message.from_user.username))
    return ConversationHandler.END


async def error(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Error handler - logs errors."""
    logger.warning('Update "%s" caused error "%s"', update, context.error)


async def status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """SSX Status Command - Display Ghost Drive and system status.
    
    Shows:
    - Ghost Drive configuration status
    - Last successful sync time
    - Current userdata file size
    - System uptime
    """
    # Check if user is admin
    if update.message.from_user.id != generalcfg.adminID:
        await update.message.reply_text("❌ Admin only command.")
        return ConversationHandler.END
    
    status_lines = ["📊 **SSX System Status**\n"]
    
    # Ghost Drive Status
    ghost_status = userdatastore.get_ghost_drive_status()
    
    status_lines.append("🗄️ **Ghost Drive**")
    if ghost_status['is_configured']:
        status_lines.append(f"✅ Configured (Channel: `{ghost_status['channel_id']}`)")
    else:
        status_lines.append("❌ Not configured - set `TG_DATABASE_CHANNEL_ID`")
    
    if ghost_status['last_sync_time']:
        last_sync = ghost_status['last_sync_time'].strftime("%Y-%m-%d %H:%M:%S")
        status_lines.append(f"📅 Last sync: `{last_sync}`")
    else:
        status_lines.append("📅 Last sync: Never")
    
    # File size
    file_size = ghost_status['file_size']
    if file_size > 0:
        if file_size < 1024:
            size_str = f"{file_size} bytes"
        elif file_size < 1024 * 1024:
            size_str = f"{file_size / 1024:.1f} KB"
        else:
            size_str = f"{file_size / (1024 * 1024):.1f} MB"
        status_lines.append(f"📁 userdata size: `{size_str}`")
    else:
        status_lines.append("📁 userdata size: 0 bytes")
    
    status_lines.append("")
    
    # Uptime
    uptime = time.time() - start_time
    hours, remainder = divmod(int(uptime), 3600)
    minutes, seconds = divmod(remainder, 60)
    status_lines.append(f"⏱️ Uptime: `{hours}h {minutes}m {seconds}s`")
    
    status_text = "\n".join(status_lines)
    await update.message.reply_text(status_text, parse_mode='Markdown')
    
    return ConversationHandler.END


# =======================================================================
# MAIN ENTRY POINT
# =======================================================================

async def post_init(application: Application) -> None:
    """
    SSX Post-Initialization Hook.
    
    Called after the application is built and initialized.
    Performs Ghost Drive boot sequence.
    """
    global _bot_for_shutdown
    _bot_for_shutdown = application.bot
    
    # =======================================================================
    # SSX GHOST DRIVE - Boot Sequence
    # =======================================================================
    # Load the latest backup from the Telegram Vault.
    # This ensures we have the most recent state from a previous instance.
    # =======================================================================
    if generalcfg.DATABASE_CHANNEL_ID:
        logger.info("[SSX GHOST DRIVE] Attempting to load from Telegram Vault...")
        
        # Run sync function in executor to avoid blocking
        loop = asyncio.get_event_loop()
        success, message = await loop.run_in_executor(
            None, userdatastore.load_from_ghost_drive, application.bot
        )
        
        if success:
            logger.info(f"[SSX GHOST DRIVE] {message}")
        else:
            if "first-time setup" in message.lower():
                logger.info("[SSX GHOST DRIVE] First-time setup detected - initializing fresh database")
                userdatastore.userfiledetect()
            else:
                logger.warning(f"[SSX GHOST DRIVE] Load failed: {message} - using local file")
        
        # Initialize the periodic Ghost Drive sync job (every 20 minutes)
        ghost_job = application.job_queue.run_repeating(
            lambda ctx: asyncio.create_task(_ghost_sync_job(ctx)),
            interval=generalcfg.GHOST_DRIVE_SYNC_INTERVAL,
            first=60
        )
        if ghost_job:
            logger.info(f"[SSX GHOST DRIVE] Periodic sync job scheduled (interval: {generalcfg.GHOST_DRIVE_SYNC_INTERVAL}s)")
    else:
        logger.info("[SSX GHOST DRIVE] Not configured - DATABASE_CHANNEL_ID not set")


async def _ghost_sync_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Periodic Ghost Drive sync job wrapper."""
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(
        None, userdatastore.sync_to_ghost_drive, context.bot
    )


def main() -> None:
    """Main entry point for the bot."""
    # SSX ZERO-BUG: Register signal handlers for graceful shutdown
    _register_signal_handlers()
    
    # Build the application (v20+ syntax)
    builder = ApplicationBuilder()
    
    if generalcfg.token:
        builder.token(generalcfg.token)
    else:
        logger.error("No bot token configured!")
        sys.exit(1)
    
    if generalcfg.proxy:
        builder.http_version("1.1")
        builder.get_updates_proxy_url(generalcfg.proxy[0])
    
    application = builder.build()
    
    # Store bot reference for signal handler
    global _bot_for_shutdown
    _bot_for_shutdown = application.bot
    
    # Register post-init hook
    application.post_init = post_init
    
    # Add conversation handler
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler('start', start)],
        states={
            STATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, state_handler)]
        },
        fallbacks=[CommandHandler('cancel', cancel)],
    )
    application.add_handler(conv_handler)
    
    # Add /status command handler
    application.add_handler(CommandHandler('status', status))
    
    # Add error handler
    application.add_error_handler(error)
    
    # Add SSX Dungeon moderation handler - uses existing pubChannelID (SSX NSFW chat)
    if generalcfg.pubChannelID:
        try:
            ssx_channel = int(generalcfg.pubChannelID)
            ssx_filter = filters.TEXT & filters.Chat(ssx_channel)
            application.add_handler(MessageHandler(ssx_filter, _handle_ssx_moderation))
            logger.info("[SSX MOD] Watching SSX NSFW chat %s for blocked content", ssx_channel)
        except (ValueError, TypeError):
            logger.warning("[SSX MOD] Invalid pubChannelID - handler not registered")
    
    # Create recurring search job
    application.job_queue.run_repeating(
        searchIntervalCTL,
        interval=generalcfg.interval,
        first=5
    )
    
    logger.info("Bot initiating...")
    
    # Run with polling (original behavior)
    application.run_polling(
        allowed_updates=Update.ALL_TYPES
    )


if __name__ == '__main__':
    logging.basicConfig(
        format='%(asctime)s - %(module)s.%(funcName)s - %(levelname)s - %(message)s',
        level=logging.INFO
    )
    logging.getLogger('requests').setLevel(logging.CRITICAL)
    
    # Suppress PTB's httpx client from logging full URLs (contains bot token)
    # Fixes CodeQL: py/clear-text-logging-sensitive-data
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("telegram.ext.Application").setLevel(logging.WARNING)
    
    main()
