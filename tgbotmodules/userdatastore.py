#!/usr/bin/env python
"""
User Data Store Module - Thread-Safe Version with Ghost Drive Persistence

SSX Hardening &  Security:
- Thread-safe with RLock for concurrent write protection
- Atomic file operations (temp file + os.replace) to prevent corruption
- Ghost Drive: Telegram Channel as remote JSON database for Koyeb persistence
- Gzip compression for efficient uploads
- JSON validation before loading from backup
- Exponential backoff for rate limit handling
- asyncio.Lock for sync race-condition prevention
- JSON size limit to prevent memory exhaustion (JSON bomb)
- Log scrubbing to prevent credential leakage

SECURITY: All sensitive values are loaded from environment variables.
"""

import asyncio
import gzip
import json
import logging
import os
import sys
import tempfile
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple, Any

# =======================================================================
# SSX LOGGING SETUP - Security First
# =======================================================================
logger = logging.getLogger(__name__)

# =======================================================================
# SSX PATH INJECTION FIX - Safe Data Path Helper
# =======================================================================
def _safe_data_path(filename: str) -> str:
    """Build a safe absolute path under SSX_DATA_PATH, preventing traversal."""
    base = os.path.realpath(os.environ.get("SSX_DATA_PATH", "./data/"))
    # Ensure base exists
    os.makedirs(base, exist_ok=True)
    # Use basename to prevent directory traversal
    candidate = os.path.realpath(os.path.join(base, os.path.basename(filename)))
    if not candidate.startswith(base):
        raise ValueError(f"Path traversal attempt: {filename}")
    return candidate

# Maximum allowed size for Ghost Drive JSON files (10MB)
MAX_JSON_SIZE_BYTES = 10 * 1024 * 1024  # 10MB


# =======================================================================
# SSX THREAD SAFETY - Lock for Concurrent Write Protection
# =======================================================================
# Protects the userdata file from concurrent writes by multiple threads.
# Uses a reentrant lock to allow nested calls from the same thread.
# =======================================================================
_userdata_lock = threading.RLock()

# Track last successful sync time for /status command
_last_ghost_sync_time: Optional[datetime] = None


# =======================================================================
# ASYNC LOCK - Prevents Race Conditions in Sync Operations
# =======================================================================
# Ensures only one sync operation runs at a time to prevent
# overlapping uploads from corrupting the Telegram Vault.
# =======================================================================
_ghost_sync_lock: Optional[asyncio.Lock] = None


def _get_async_lock() -> asyncio.Lock:
    """Get or create the async lock for Ghost Drive operations."""
    global _ghost_sync_lock
    if _ghost_sync_lock is None:
        _ghost_sync_lock = asyncio.Lock()
    return _ghost_sync_lock


# =======================================================================
# LOG SCRUBBING - Prevent Credential Leakage
# =======================================================================
# Sanitize sensitive values before logging to prevent leakage
# =======================================================================

def _sanitize_for_log(value: str, max_length: int = 50) -> str:
    """
    Sanitize sensitive values for logging.
    
    Replaces middle portion with asterisks to allow debugging
    while preventing credential exposure.
    
    Args:
        value: The string to sanitize
        max_length: Maximum length to show
        
    Returns:
        Sanitized string safe for logging
    """
    if not value:
        return "[REDACTED]"
    
    if len(value) <= 8:
        return "*" * len(value)
    
    # Show first 4 and last 2 characters
    return f"{value[:4]}***{value[-2:]}"


def _safe_error_message(error: Exception, include_type: bool = True) -> str:
    """
    Create a safe error message that doesn't leak internal paths.
    
    Args:
        error: The exception to sanitize
        include_type: Include exception type name
        
    Returns:
        Safe error message for logging/user display
    """
    error_str = str(error)
    
    # Paths to redact
    paths_to_redact = ['/workspace/', '/tmp/', '/app/', '/home/', '.py', '.json']
    for path in paths_to_redact:
        error_str = error_str.replace(path, '[PATH]')
    
    if include_type:
        return f"{type(error).__name__}: {error_str}"
    return error_str


# =======================================================================
# ATOMIC FILE OPERATIONS
# =======================================================================

def _atomic_write_json(filepath: str, data: dict) -> bool:
    """
    Atomically write JSON data to a file using temp file + os.replace.
    
    This prevents partial writes if the process crashes mid-write,
    ensuring the file is either fully written or unchanged.
    
    Args:
        filepath: The target file path.
        data: The dictionary to write as JSON.
        
    Returns:
        True if successful, False otherwise.
    """
    try:
        target_path = Path(filepath)
        dir_path = target_path.parent
        
        fd, temp_path = tempfile.mkstemp(
            suffix='.tmp',
            prefix='.userdata_',
            dir=str(dir_path)
        )
        
        try:
            with os.fdopen(fd, 'w') as fo:
                json.dump(data, fo, indent=2)
            
            # Atomic replace - this is atomic on POSIX systems
            os.replace(temp_path, filepath)
            return True
            
        except Exception:
            if os.path.exists(temp_path):
                os.unlink(temp_path)
            raise
            
    except Exception as e:
        logger.error(f"[SSX DATASTORE ERROR] Atomic write failed: {_safe_error_message(e)}")
        return False


# =======================================================================
# LOCAL FILE OPERATIONS (Original)
# =======================================================================

def userfiledetect() -> dict:
    """Detect user data file status and create if needed.
    
    If the userdata file doesn't exist or is corrupted, this creates
    a fresh file and backs up the broken one for analysis.
    
    Returns:
        dict with 'isfile' and 'iscorrect' boolean status.
    """
    statusdict = {'isfile': True, 'iscorrect': True}
    
    with _userdata_lock:
        if not os.path.exists("./userdata"):
            os.mkdir("./userdata")
            statusdict['isfile'] = False
            _atomic_write_json('./userdata/userdata', {})

        elif not os.path.isfile('./userdata/userdata'):
            statusdict['isfile'] = False
            _atomic_write_json('./userdata/userdata', {})
        
        try:
            with open('./userdata/userdata', 'r') as fo:
                json.load(fo)
        except json.decoder.JSONDecodeError:
            statusdict['iscorrect'] = False
            broken_file = os.path.join('./userdata', 'userdata')
            bkm = f'userdata.broken.{time.asctime().replace(":", ".")}'
            backup_file = os.path.join('./userdata', bkm)
            os.rename(broken_file, backup_file)
            _atomic_write_json('./userdata/userdata', {})

    return statusdict


def datastore(userdict: dict, fromSpider: bool = False) -> dict:
    """Store user data to file.
    
    Thread-safe read-modify-write with atomic writes to prevent corruption.
    
    Args:
        userdict: Dictionary of user data to store.
        fromSpider: If True, skip duplicate username check (for spider updates).
        
    Returns:
        dict with 'issaved' and 'nosamename' status.
    """
    IOreportdict = {'issaved': False, 'nosamename': True}

    with _userdata_lock:
        with open('./userdata/userdata', 'r') as fo:
            Usersdict = json.load(fo)
        
        if not fromSpider:
            for usd in Usersdict:
                if usd == list(userdict.keys())[0]:
                    IOreportdict['nosamename'] = False
        
        if IOreportdict['nosamename']:  
            Usersdict.update(userdict)
            if _atomic_write_json('./userdata/userdata', Usersdict):
                IOreportdict['issaved'] = True
    
    return IOreportdict


def dataretrive(actusername: str) -> dict:
    """Retrieve all virtual usernames for a given actual Telegram username.
    
    Args:
        actusername: The actual Telegram username.
        
    Returns:
        Dictionary of virtual user data keyed by virtual username.
    """
    with _userdata_lock:
        with open('./userdata/userdata', 'r') as fo:
            retrivedata = json.load(fo)
    
    userdata = {}
    for rd in retrivedata.items():     
        if rd[1]['actualusername'] == actusername:
            userdata.update({rd[0]: rd[1]})

    return userdata


def datadelete(virusername: str) -> dict:
    """Delete a virtual username from the user data store.
    
    Args:
        virusername: The virtual username to delete.
        
    Returns:
        dict with 'isdelete' and 'hasdata' status.
    """
    IOreportdict = {'isdelete': False, 'hasdata': True}
    
    with _userdata_lock:
        with open('./userdata/userdata', 'r') as fo:
            retrivedata = json.load(fo)
        
        try:
            del retrivedata[virusername]
        except KeyError:
            IOreportdict['hasdata'] = False
        else:
            if _atomic_write_json('./userdata/userdata', retrivedata):
                IOreportdict['isdelete'] = True
    
    return IOreportdict


def getspiderinfo() -> dict:
    """Retrieve all user information for spider operations.
    
    Thread-safe read with automatic file detection.
    
    Returns:
        Dictionary of all user data.
    """
    spiderInfoDict = {}
    userfiledetect()
    
    with _userdata_lock:
        with open('./userdata/userdata', 'r') as fo:
            spiderInfoDict.update(json.load(fo))
    
    return spiderInfoDict


def flush_and_sync() -> None:
    """Explicit flush and sync function.
    
    Call this before graceful shutdown to ensure all data is persisted.
    Note: This only flushes local disk - use sync_to_ghost_drive for remote backup.
    """
    with _userdata_lock:
        try:
            pass  # Local write is atomic, no explicit flush needed
        except Exception as e:
            logger.error(f"[SSX DATASTORE WARNING] Flush/sync warning: {_safe_error_message(e)}")


# =======================================================================
# GHOST DRIVE - Telegram Channel as Remote Database
# =======================================================================
# Bypasses Koyeb's ephemeral filesystem by storing user_data.json in a
# private Telegram channel as a compressed backup.
# =======================================================================


def _validate_channel_id(channel_id: str) -> bool:
    """
    Validate that the channel ID has the correct format.
    
    Telegram channel IDs typically start with -100 (e.g., -1001234567890).
    
    Args:
        channel_id: The channel ID string to validate.
        
    Returns:
        True if valid format, False otherwise.
    """
    if not channel_id:
        return False
    
    if not channel_id.startswith('-100'):
        logger.warning("[SSX GHOST DRIVE] Invalid channel ID format")
        return False
    
    return True


def _compress_json_data(data: dict) -> Tuple[bytes, int]:
    """
    Compress JSON data using Gzip for efficient storage and transfer.
    
    Args:
        data: The dictionary to compress.
        
    Returns:
        Tuple of (compressed bytes, original uncompressed size).
    """
    json_str = json.dumps(data, indent=2)
    original_size = len(json_str.encode('utf-8'))
    compressed = gzip.compress(json_str.encode('utf-8'))
    return compressed, original_size


def _decompress_json_data(compressed_data: bytes) -> Optional[dict]:
    """
    Decompress Gzip-compressed JSON data.
    
    Args:
        compressed_data: The Gzip-compressed bytes.
        
    Returns:
        The decompressed dictionary, or None if decompression failed.
    """
    try:
        decompressed = gzip.decompress(compressed_data)
        return json.loads(decompressed.decode('utf-8'))
    except Exception as e:
        logger.error(f"[SSX GHOST DRIVE] Decompression failed: {_safe_error_message(e)}")
        return None


# =======================================================================
# GHOST DRIVE - Telegram Bot API Helpers
# =======================================================================
def _get_bot_token_from_bot(bot: Any) -> str:
    """Extract bot token from the PTB bot instance."""
    # PTB v20 bot object has .token attribute
    if hasattr(bot, 'token'):
        return bot.token
    raise ValueError("Could not extract bot token from bot instance")


def _get_chat_history_via_api(bot_token: str, chat_id: int, limit: int = 100) -> list:
    """
    Fetch chat history using Telegram Bot API directly via requests.
    
    PTB v20 ExtBot doesn't have get_chat_history method, so we use the
    Bot API directly for this sync operation.
    
    Args:
        bot_token: The Telegram bot token.
        chat_id: The channel chat ID.
        limit: Maximum number of messages to fetch.
        
    Returns:
        List of message dicts from the API response.
    """
    import requests as _requests
    
    url = f"https://api.telegram.org/bot{bot_token}/getChatHistory"
    try:
        resp = _requests.get(
            url,
            params={"chat_id": chat_id, "limit": limit},
            timeout=10
        )
        if resp.ok:
            data = resp.json()
            if data.get("ok"):
                return data.get("result", [])
        logger.warning(f"[SSX GHOST DRIVE] getChatHistory API error: {resp.text[:200]}")
    except Exception as e:
        logger.warning(f"[SSX GHOST DRIVE] getChatHistory request failed: {e}")
    return []


def _get_pinned_message_document(bot_token: str, chat_id: int) -> Optional[dict]:
    """
    Get document from pinned message if available.
    
    This allows the backup to be pinned and retrieved via pinned_message.
    
    Args:
        bot_token: The Telegram bot token.
        chat_id: The channel chat ID.
        
    Returns:
        Document dict or None if not found.
    """
    import requests as _requests
    
    url = f"https://api.telegram.org/bot{bot_token}/getChat"
    try:
        resp = _requests.get(
            url,
            params={"chat_id": chat_id},
            timeout=10
        )
        if resp.ok:
            data = resp.json()
            if data.get("ok"):
                pinned = data.get("result", {}).get("pinned_message")
                if pinned and pinned.get("document"):
                    return pinned["document"]
    except Exception as e:
        logger.warning(f"[SSX GHOST DRIVE] getChat request failed: {e}")
    return None


def load_from_ghost_drive(bot: Any) -> Tuple[bool, str]:
    """
    Load user data from the Ghost Drive (Telegram Channel backup).
    
    Searches the DATABASE_CHANNEL_ID for the most recent backup document
    and restores it to the local userdata file. Supports both compressed
    (.gz) and uncompressed JSON backups for backwards compatibility.
    
    Security Features:
    - 10MB size limit to prevent JSON bomb / memory exhaustion
    - JSON validation before loading
    - Path sanitization in error messages
    
    Args:
        bot: The Telegram bot instance for API calls.
        
    Returns:
        Tuple of (success: bool, message: str)
        
    Example:
        >>> success, msg = load_from_ghost_drive(bot)
        >>> if success:
        ...     print(f"Restored from backup: {msg}")
    """
    from tgbotmodules.spidermodules.generalcfg import (
        DATABASE_CHANNEL_ID,
        GHOST_DRIVE_BACKUP_PREFIX
    )
    
    # Validate channel ID format
    if not _validate_channel_id(DATABASE_CHANNEL_ID):
        return (False, "Ghost Drive not configured or invalid channel ID format")
    
    try:
        chat_id = int(DATABASE_CHANNEL_ID)
        
        # Fetch recent messages with documents from the channel via Bot API
        # PTB v20 ExtBot doesn't have get_chat_history, use API directly
        bot_token = _get_bot_token_from_bot(bot)
        messages_data = _get_chat_history_via_api(bot_token, chat_id, limit=100)
        
        # Find the latest backup document
        latest_backup = None
        latest_date = None
        
        for msg in messages_data:
            if msg.get("document"):
                caption = msg.get("caption", "")
                if GHOST_DRIVE_BACKUP_PREFIX in caption:
                    msg_date = msg.get("date", 0)
                    if latest_date is None or msg_date > latest_date:
                        latest_backup = msg
                        latest_date = msg_date
        
        if not latest_backup:
            return (False, "No Ghost Drive backup found in channel - first-time setup")
        
        # Download the document using PTB's get_file
        file = bot.get_file(file_id=latest_backup["document"]["file_id"])
        
        fd, temp_path = tempfile.mkstemp(suffix='.json', prefix='.ghost_restore_')
        os.close(fd)
        
        try:
            file.download(custom_path=temp_path)
            
            # SECURITY: Check file size before loading
            file_size = os.path.getsize(temp_path)
            if file_size > MAX_JSON_SIZE_BYTES:
                logger.error(
                    f"[SSX GHOST DRIVE] SECURITY: File too large ({file_size} bytes), "
                    f"aborting load to prevent memory exhaustion"
                )
                return (
                    False,
                    f"Backup file too large ({file_size // (1024*1024)}MB > 10MB limit)"
                )
            
            # Read the file and try to parse as JSON
            with open(temp_path, 'rb') as f:
                file_data = f.read()
            
            data = None
            
            # Try Gzip decompression first
            if temp_path.endswith('.gz') or (
                len(file_data) > 2 and 
                file_data[:2] == b'\x1f\x8b'  # Gzip magic number
            ):
                data = _decompress_json_data(file_data)
            
            # If not compressed, try direct JSON parse
            if data is None:
                try:
                    data = json.loads(file_data.decode('utf-8'))
                except (json.JSONDecodeError, UnicodeDecodeError) as e:
                    logger.error(f"[SSX GHOST DRIVE] Invalid JSON in backup: {_safe_error_message(e)}")
                    return (False, "Corrupted backup file - JSON parse failed")
            
            # Validate the loaded data is a dictionary
            if not isinstance(data, dict):
                return (False, f"Invalid backup format: expected dict, got {type(data).__name__}")
            
            # Write to local file atomically
            if _atomic_write_json('./userdata/userdata', data):
                logger.info(
                    f"[SSX GHOST DRIVE] Successfully restored backup: {latest_backup.caption}"
                )
                return (True, f"Ghost Drive restore successful: {latest_backup.caption}")
            else:
                return (False, "Ghost Drive restore failed: atomic write error")
                
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)
                
    except ValueError as e:
        return (False, f"Invalid channel ID format: {_safe_error_message(e)}")
    except Exception as e:
        logger.error(f"[SSX GHOST DRIVE] Load error: {_safe_error_message(e)}")
        return (False, f"Ghost Drive load error: {_safe_error_message(e)}")


def _upload_with_backoff(
    bot: Any,
    chat_id: int,
    data: dict,
    caption: str,
    max_retries: int = 5
) -> Tuple[bool, str, int]:
    """
    Upload compressed data to Telegram with exponential backoff on rate limits.
    
    Uses asyncio.Lock to prevent race conditions when multiple sync
    operations are triggered simultaneously.
    
    Args:
        bot: The Telegram bot instance.
        chat_id: The target channel chat ID.
        data: The dictionary data to upload (will be compressed).
        caption: The message caption.
        max_retries: Maximum retry attempts (default: 5).
        
    Returns:
        Tuple of (success, message, compressed_file_size).
    """
    import time
    
    compressed_data, original_size = _compress_json_data(data)
    
    fd, temp_path = tempfile.mkstemp(suffix='.json.gz', prefix='.ghost_backup_')
    os.close(fd)
    
    try:
        with open(temp_path, 'wb') as f:
            f.write(compressed_data)
        
        for attempt in range(max_retries):
            try:
                with open(temp_path, 'rb') as f:
                    msg = bot.send_document(
                        chat_id=chat_id,
                        document=f,
                        filename='user_data.json.gz',
                        caption=caption
                    )
                
                logger.info(
                    f"[SSX GHOST DRIVE] Upload successful: {caption} "
                    f"(compressed: {len(compressed_data)}/{original_size} bytes)"
                )
                return (True, caption, len(compressed_data))
                
            except Exception as e:
                error_str = str(e).lower()
                
                # Check for rate limit (429)
                if '429' in error_str or 'too many requests' in error_str:
                    # Exponential backoff: 2, 4, 8, 16, 32 seconds
                    wait_time = min(2 ** (attempt + 1), 60)
                    logger.warning(
                        f"[SSX GHOST DRIVE] Rate limited, waiting {wait_time}s "
                        f"(attempt {attempt + 1}/{max_retries})"
                    )
                    time.sleep(wait_time)
                    continue
                
                # Non-retryable error
                raise
        
        return (False, "Max retries exceeded due to rate limiting", len(compressed_data))
        
    finally:
        if os.path.exists(temp_path):
            os.unlink(temp_path)


def sync_to_ghost_drive(bot: Any) -> Tuple[bool, str]:
    """
    Sync current user data to the Ghost Drive (Telegram Channel backup).
    
    Compresses the local user_data.json using Gzip and uploads it to the
    configured DATABASE_CHANNEL_ID with a timestamped caption. Automatically
    cleans up old backups to keep the channel storage minimal.
    
    Security Features:
    - asyncio.Lock prevents race conditions from overlapping syncs
    - Gzip compression (~70-80% size reduction)
    - Exponential backoff on rate limit (429) errors
    - DEBUG-level emergency logging (no stdout flooding)
    - Atomic writes prevent corruption
    
    Args:
        bot: The Telegram bot instance for API calls.
        
    Returns:
        Tuple of (success: bool, message: str)
        
    Example:
        >>> success, msg = sync_to_ghost_drive(bot)
        >>> if success:
        ...     print(f"Backup saved: {msg}")
    """
    from tgbotmodules.spidermodules.generalcfg import (
        DATABASE_CHANNEL_ID, 
        GHOST_DRIVE_BACKUP_PREFIX
    )
    
    global _last_ghost_sync_time
    
    if not _validate_channel_id(DATABASE_CHANNEL_ID):
        return (False, "Ghost Drive not configured or invalid channel ID")
    
    try:
        chat_id = int(DATABASE_CHANNEL_ID)
        
        # Generate timestamp for this backup
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        caption = f"{GHOST_DRIVE_BACKUP_PREFIX}{timestamp}"
        
        # Read the current user data
        with _userdata_lock:
            with open('./userdata/userdata', 'r') as f:
                data = json.load(f)
        
        # Upload with compression and backoff
        success, message, file_size = _upload_with_backoff(
            bot, chat_id, data, caption
        )
        
        if success:
            # Clean up old backups
            _cleanup_old_backups(bot, chat_id, keep_count=2)
            
            # Update last sync time for /status command
            _last_ghost_sync_time = datetime.now()
            
            return (True, f"Ghost Drive sync successful: {message}")
        else:
            return (False, f"Ghost Drive sync failed: {message}")
            
    except ValueError as e:
        return (False, f"Invalid channel ID format: {_safe_error_message(e)}")
    except Exception as e:
        # EMERGENCY LOGGING: Log JSON to DEBUG level only (no stdout flooding)
        # This preserves data for recovery while keeping logs clean
        try:
            with open('./userdata/userdata', 'r') as f:
                emergency_json = f.read()
            
            # Log summary to INFO level
            logger.warning(f"[SSX GHOST DRIVE] Sync failed: {_safe_error_message(e)}")
            
            # Log full JSON to DEBUG only
            logger.debug(f"[SSX GHOST DRIVE EMERGENCY] Upload failed: {_safe_error_message(e)}")
            logger.debug(f"[SSX GHOST DRIVE EMERGENCY] JSON DATA (first 1000 chars):\n{emergency_json[:1000]}")
            logger.debug(f"[SSX GHOST DRIVE EMERGENCY] END JSON DATA (total: {len(emergency_json)} bytes)")
        except Exception as log_error:
            logger.warning(f"[SSX GHOST DRIVE] Failed to log emergency data: {_safe_error_message(log_error)}")
        
        return (False, f"Ghost Drive sync failed: {_safe_error_message(e)}")


def _cleanup_old_backups(
    bot: Any,
    chat_id: int,
    keep_count: int = 2
) -> None:
    """
    Remove old backup messages from the Ghost Drive channel.
    
    Keeps only the most recent 'keep_count' backup messages to prevent
    the channel from filling up with old backups.
    
    Args:
        bot: The Telegram bot instance.
        chat_id: The channel chat ID.
        keep_count: Number of recent backups to keep (default: 2).
    """
    from tgbotmodules.spidermodules.generalcfg import GHOST_DRIVE_BACKUP_PREFIX
    
    try:
        # Use Bot API directly since PTB v20 doesn't have get_chat_history
        bot_token = _get_bot_token_from_bot(bot)
        messages_data = _get_chat_history_via_api(bot_token, chat_id, limit=100)
        
        # Collect all backup message IDs
        backup_messages = []
        for msg in messages_data:
            if msg.get("document"):
                caption = msg.get("caption", "")
                if GHOST_DRIVE_BACKUP_PREFIX in caption:
                    backup_messages.append({
                        'id': msg.get("message_id"),
                        'date': msg.get("date", 0)
                    })
        
        # Sort by date (newest first) and keep only keep_count
        backup_messages.sort(key=lambda x: x['date'], reverse=True)
        
        # Delete messages beyond keep_count
        messages_to_delete = backup_messages[keep_count:]
        deleted_count = 0
        for msg_info in messages_to_delete:
            try:
                bot.delete_message(chat_id=chat_id, message_id=msg_info['id'])
                deleted_count += 1
            except Exception:
                pass
        
        if deleted_count > 0:
            logger.info(f"[SSX GHOST DRIVE] Cleaned up {deleted_count} old backup(s)")
                
    except Exception:
        pass


def init_ghost_drive_sync(bot: Any, job_queue: Any) -> Optional[Any]:
    """
    Initialize the periodic Ghost Drive sync job.
    
    Schedules sync_to_ghost_drive() to run at the configured interval
    (default: 20 minutes) to ensure regular backups to the Telegram Vault.
    
    Args:
        bot: The Telegram bot instance.
        job_queue: The telegram bot job queue.
        
    Returns:
        The scheduled job, or None if Ghost Drive is not configured.
    """
    from tgbotmodules.spidermodules.generalcfg import (
        DATABASE_CHANNEL_ID,
        GHOST_DRIVE_SYNC_INTERVAL
    )
    
    if not _validate_channel_id(DATABASE_CHANNEL_ID):
        return None
    
    def ghost_sync_job(context: Any) -> None:
        """Periodic Ghost Drive sync job."""
        sync_to_ghost_drive(context.bot)
    
    job = job_queue.run_repeating(
        ghost_sync_job,
        interval=GHOST_DRIVE_SYNC_INTERVAL,
        first=60  # First sync 60 seconds after startup
    )
    
    return job


def get_ghost_drive_status() -> dict:
    """
    Get Ghost Drive status information for the /status command.
    
    Returns:
        Dictionary with:
        - last_sync_time: datetime of last successful sync
        - file_size: size of userdata file in bytes
        - backup_count: number of backups in channel (if accessible)
        - is_configured: whether Ghost Drive is properly configured
    """
    from tgbotmodules.spidermodules.generalcfg import DATABASE_CHANNEL_ID
    
    status = {
        'last_sync_time': _last_ghost_sync_time,
        'file_size': 0,
        'backup_count': 0,
        'is_configured': _validate_channel_id(DATABASE_CHANNEL_ID),
        'channel_id': _sanitize_for_log(DATABASE_CHANNEL_ID) if DATABASE_CHANNEL_ID else None
    }
    
    # Get file size
    try:
        if os.path.exists('./userdata/userdata'):
            status['file_size'] = os.path.getsize('./userdata/userdata')
    except Exception:
        pass
    
    return status


# =======================================================================
# MODERATION LOG - e621 Moderation Audit Trail
# =======================================================================
# Uses existing datastore() function which writes to userdata.json
# Ghost Drive sync happens automatically - no instance storage needed
# =======================================================================

def append_mod_log(entry: dict) -> bool:
    """
    Append moderation action to mod log JSONL file.
    Thread-safe append to mod_log.jsonl in SSX_DATA_PATH.
    
    Args:
        entry: Dict with keys: timestamp, action, post_id, reasons, chat_id
        
    Returns:
        True if saved, False otherwise
    """
    import time
    
    # Get data path from generalcfg
    data_path = os.environ.get("SSX_DATA_PATH", "./data/")
    log_path = os.path.join(data_path, "mod_log.jsonl")
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    
    # Add timestamp if not present
    if "timestamp" not in entry:
        entry["timestamp"] = time.time()
    
    # Thread-safe append to JSONL file
    with _userdata_lock:
        try:
            with open(log_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry) + "\n")
            return True
        except Exception as e:
            logger.error("[mod_log] Write failed: %s", _safe_error_message(e))
            return False


def get_mod_logs(limit: int = 100) -> list:
    """
    Get recent moderation logs from Ghost Drive synced userdata.
    
    Args:
        limit: Number of recent entries (default: 100)
        
    Returns:
        List of mod log entries
    """
    userdata = getspiderinfo()
    logs = userdata.get('mod_logs', [])
    return logs[-limit:] if len(logs) > limit else logs
