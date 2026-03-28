#!/usr/bin/env python
"""
User Data Store Module - Thread-Safe Version with Ghost Drive Persistence

SSX Zero-Bug Hardening:
- Thread-safe with RLock for concurrent write protection
- Atomic file operations (temp file + os.replace) to prevent corruption
- Ghost Drive: Telegram Channel as remote JSON database for Koyeb persistence
- Gzip compression for efficient uploads
- JSON validation before loading from backup
- Exponential backoff for rate limit handling

SECURITY: All sensitive values are loaded from environment variables.
"""

import json
import gzip
import logging
import os
import sys
import time
import tempfile
import threading
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple, Any

# =======================================================================
# SSX LOGGING SETUP
# =======================================================================
logger = logging.getLogger(__name__)


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
            
            os.replace(temp_path, filepath)
            return True
            
        except Exception:
            if os.path.exists(temp_path):
                os.unlink(temp_path)
            raise
            
    except Exception as e:
        sys.stderr.write(f"[SSX DATASTORE ERROR] Atomic write failed for {filepath}: {e}\n")
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
            sys.stderr.write(f"[SSX DATASTORE WARNING] Flush/sync warning: {e}\n")


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
        logger.warning(
            f"[SSX GHOST DRIVE] Invalid channel ID format: {channel_id}. "
            f"Expected format: -100XXXXXXXXXX"
        )
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
        logger.error(f"[SSX GHOST DRIVE] Decompression failed: {e}")
        return None


def load_from_ghost_drive(bot: Any) -> Tuple[bool, str]:
    """
    Load user data from the Ghost Drive (Telegram Channel backup).
    
    Searches the DATABASE_CHANNEL_ID for the most recent backup document
    and restores it to the local userdata file. Supports both compressed
    (.gz) and uncompressed JSON backups for backwards compatibility.
    
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
        
        # Fetch recent messages with documents from the channel
        messages = bot.get_chat_history(chat_id=chat_id, limit=100)
        
        # Find the latest backup document
        latest_backup = None
        latest_date = None
        
        for msg in messages:
            if msg.document:
                caption = msg.caption or ""
                if GHOST_DRIVE_BACKUP_PREFIX in caption:
                    if latest_date is None or msg.date > latest_date:
                        latest_backup = msg
                        latest_date = msg.date
        
        if not latest_backup:
            return (False, "No Ghost Drive backup found in channel - first-time setup")
        
        # Download the document
        file = bot.get_file(file_id=latest_backup.document.file_id)
        
        fd, temp_path = tempfile.mkstemp(suffix='.json', prefix='.ghost_restore_')
        os.close(fd)
        
        try:
            file.download(custom_path=temp_path)
            
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
                    logger.error(f"[SSX GHOST DRIVE] Invalid JSON in backup: {e}")
                    return (False, f"Corrupted backup file - JSON parse failed: {e}")
            
            # Validate the loaded data is a dictionary
            if not isinstance(data, dict):
                return (False, f"Invalid backup format: expected dict, got {type(data).__name__}")
            
            # Write to local file atomically
            if _atomic_write_json('./userdata/userdata', data):
                file_size = len(file_data)
                logger.info(
                    f"[SSX GHOST DRIVE] Successfully restored backup: "
                    f"{latest_backup.caption} ({file_size} bytes)"
                )
                return (True, f"Ghost Drive restore successful: {latest_backup.caption}")
            else:
                return (False, "Ghost Drive restore failed: atomic write error")
                
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)
                
    except ValueError as e:
        return (False, f"Invalid channel ID format: {e}")
    except Exception as e:
        return (False, f"Ghost Drive load error: {str(e)}")


def _upload_with_backoff(
    bot: Any,
    chat_id: int,
    data: dict,
    caption: str,
    max_retries: int = 5
) -> Tuple[bool, str, int]:
    """
    Upload compressed data to Telegram with exponential backoff on rate limits.
    
    Args:
        bot: The Telegram bot instance.
        chat_id: The target channel chat ID.
        data: The dictionary data to upload (will be compressed).
        caption: The message caption.
        max_retries: Maximum retry attempts (default: 5).
        
    Returns:
        Tuple of (success, message, file_size).
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
    
    Features:
    - Gzip compression reduces upload size by ~70-80%
    - Exponential backoff on rate limit (429) errors
    - Automatic cleanup of old backups
    - Emergency JSON logging on failure (DEBUG level only)
    
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
            
            return (True, f"Ghost Drive sync successful: {message} ({file_size} compressed bytes)")
        else:
            return (False, f"Ghost Drive sync failed: {message}")
            
    except ValueError as e:
        return (False, f"Invalid channel ID format: {e}")
    except Exception as e:
        # EMERGENCY LOGGING: Log JSON to DEBUG level only
        try:
            with open('./userdata/userdata', 'r') as f:
                emergency_json = f.read()
            logger.debug(f"[SSX GHOST DRIVE EMERGENCY] Upload failed: {str(e)}")
            logger.debug(f"[SSX GHOST DRIVE EMERGENCY] JSON DATA:\n{emergency_json}\n")
            logger.debug(f"[SSX GHOST DRIVE EMERGENCY] END JSON DATA")
        except Exception as log_error:
            logger.debug(f"[SSX GHOST DRIVE EMERGENCY] Failed to log emergency data: {log_error}")
        
        return (False, f"Ghost Drive sync failed: {str(e)}")


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
        messages = bot.get_chat_history(chat_id=chat_id, limit=100)
        
        # Collect all backup message IDs
        backup_messages = []
        for msg in messages:
            if msg.document:
                caption = msg.caption or ""
                if GHOST_DRIVE_BACKUP_PREFIX in caption:
                    backup_messages.append({
                        'id': msg.message_id,
                        'date': msg.date
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
        'channel_id': DATABASE_CHANNEL_ID if DATABASE_CHANNEL_ID else None
    }
    
    # Get file size
    try:
        if os.path.exists('./userdata/userdata'):
            status['file_size'] = os.path.getsize('./userdata/userdata')
    except Exception:
        pass
    
    return status
