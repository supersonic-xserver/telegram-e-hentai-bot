#!/usr/bin/python3
"""
General Configuration Module - SSX Ghost Protocol Edition

SSX Zero-Bug Hardening & Production-Ready Configuration:
- Thread-safe proxy index tracking with RLock
- Session-bound User-Agent storage
- Environment variable support via python-dotenv
- SSX "Ghost Protocol" stealth settings (enabled by default)
- Safety toggles for content filtering
- Persistent volume path mapping for Koyeb

SECURITY NOTE: All sensitive values (tokens, IDs) are loaded from environment
variables. Never hardcode secrets in this file. Use .env for local dev.
"""

import os
import threading
import atexit
from dotenv import load_dotenv

# =======================================================================
# SSX ENVIRONMENT LOADING - Load .env file for local development
# =======================================================================
# Load environment variables from .env file if it exists.
# In production (Koyeb), these are injected via platform config.
# =======================================================================
load_dotenv()


# =======================================================================
# SSX THREAD SAFETY - Lock for Shared State Protection
# =======================================================================
# The proxy index and session UA must be protected by locks when
# modified/read by multiple threads concurrently.
# =======================================================================

_proxy_lock = threading.RLock()
_session_ua_lock = threading.RLock()

# Current proxy index - MUST be accessed via get/set functions
_proxy_index = 0

# Session-bound User-Agents: Maps session_id -> UA string
# This ensures UA stays consistent within a single gallery download session
_session_user_agents = {}

# Shutdown flag for graceful exit
_shutdown_requested = False
_shutdown_lock = threading.Lock()


# =======================================================================
# PROXY INDEX FUNCTIONS - Thread-Safe Access
# =======================================================================

def get_proxy_index():
    """Get the current proxy index (thread-safe)."""
    with _proxy_lock:
        return _proxy_index


def set_proxy_index(index: int):
    """Set the proxy index (thread-safe)."""
    global _proxy_index
    with _proxy_lock:
        _proxy_index = index


def increment_proxy_index(max_proxies: int):
    """
    Atomically increment and wrap the proxy index.
    
    Args:
        max_proxies: Total number of available proxies.
        
    Returns:
        The new proxy index (wrapped).
    """
    with _proxy_lock:
        global _proxy_index
        if max_proxies > 0:
            _proxy_index = (_proxy_index + 1) % max_proxies
        return _proxy_index


# =======================================================================
# SESSION USER-AGENT FUNCTIONS - Thread-Safe UA/Cookie Sync
# =======================================================================

def get_session_ua(session_id: str) -> str:
    """
    Get the User-Agent for a specific session.
    If no UA is assigned, returns None (caller should pick one).
    
    Args:
        session_id: Unique identifier for the session.
        
    Returns:
        The UA string or None if not yet assigned.
    """
    with _session_ua_lock:
        return _session_user_agents.get(session_id)


def set_session_ua(session_id: str, user_agent: str):
    """
    Set the User-Agent for a specific session.
    Once set, it should NOT change for the lifetime of that session.
    
    Args:
        session_id: Unique identifier for the session.
        user_agent: The User-Agent string to assign.
    """
    with _session_ua_lock:
        _session_user_agents[session_id] = user_agent


def clear_session_ua(session_id: str):
    """
    Clear the User-Agent for a session (called when session ends).
    
    Args:
        session_id: Unique identifier for the session.
    """
    with _session_ua_lock:
        _session_user_agents.pop(session_id, None)


def clear_all_session_uas():
    """
    Clear all session User-Agents (used during shutdown).
    """
    with _session_ua_lock:
        _session_user_agents.clear()


# =======================================================================
# SHUTDOWN FLAG - Graceful Exit Support
# =======================================================================

def is_shutdown_requested() -> bool:
    """Check if shutdown has been requested."""
    with _shutdown_lock:
        return _shutdown_requested


def request_shutdown():
    """Request graceful shutdown."""
    with _shutdown_lock:
        global _shutdown_requested
        _shutdown_requested = True


def reset_shutdown():
    """Reset shutdown flag (for testing)."""
    with _shutdown_lock:
        global _shutdown_requested
        _shutdown_requested = False


# =======================================================================
# CLEANUP FUNCTION - Called on exit
# =======================================================================

def _cleanup_on_exit():
    """
    Cleanup function called on normal interpreter exit.
    Clears session data and ensures graceful state.
    """
    clear_all_session_uas()
    request_shutdown()


# Register cleanup
atexit.register(_cleanup_on_exit)


# =======================================================================
# ENVIRONMENT & SECRET MANAGEMENT
# SSX Ghost Protocol - All sensitive values via environment variables
# =======================================================================

# The token of the Telegram bot (SECURE: from environment)
token = os.getenv("TG_TOKEN", "")

# The passcode to verify user and allow them to use this service
passcode = os.getenv("TG_PASSCODE", "This is a test passcode.")

# Admin ID - cast to int to avoid Telegram API type errors
# SECURE: from environment, defaults to 0 if not set
_admin_id_raw = os.getenv("TG_ADMIN_ID", "")
adminID = int(_admin_id_raw) if _admin_id_raw else 0

# Public channel ID for broadcasting results
# SECURE: from environment, defaults to empty string if not set
pubChannelID = os.getenv("TG_PUB_CHANNEL_ID", "")

# =======================================================================
# SSX GHOST DRIVE - Telegram Channel as Remote Database
# =======================================================================
# A private Telegram channel acts as a remote JSON database for persistence.
# Bypasses Koyeb's ephemeral filesystem by storing user_data.json in a channel.
# Set DATABASE_CHANNEL_ID to a private channel's ID where the bot is an admin.
# =======================================================================
DATABASE_CHANNEL_ID = os.getenv("TG_DATABASE_CHANNEL_ID", "")

# Ghost Drive sync interval in seconds (default: 20 minutes)
GHOST_DRIVE_SYNC_INTERVAL = int(os.getenv("SSX_GHOST_SYNC_INTERVAL", "1200"))

# Ghost Drive backup filename prefix
GHOST_DRIVE_BACKUP_PREFIX = "SSX_BACKUP_"


# =======================================================================
# STEALTH & NETWORK CONSTANTS (Ghost Agent)
# SSX Ghost Protocol - Proxy and Jitter system parameters
# =======================================================================

# Enable stealth mode (default: True for production security)
ENABLE_STEALTH_MODE = True

# Proxy timeout in seconds - prevents hanging on slow/dead proxies
PROXY_TIMEOUT = 10

# Human jitter settings - random delay between requests to avoid bot detection
JITTER_MIN = 0.5  # Minimum delay in seconds
JITTER_MAX = 2.0  # Maximum delay in seconds

# Proxy URL from environment - constructs proxy dictionary only if detected
# Supports HTTP/HTTPS/SOCKS5 proxy formats
_proxy_url = os.getenv("PROXY_URL", "")

if _proxy_url:
    # Normalize proxy URL to list format for compatibility
    if not _proxy_url.startswith(("http://", "https://", "socks5://", "socks4://")):
        _proxy_url = "http://" + _proxy_url
    proxy = [_proxy_url]
else:
    proxy = []

# Alternative single proxy variable (legacy support)
TG_PROXY = os.getenv("TG_PROXY", "")


# =======================================================================
# BUCKET 3: PATH & VOLUME MAPPING (SRE Agent)
# SSX Ghost Protocol - Persistent volume support for Koyeb deployment
# =======================================================================

# SSX Data Path - Persistent storage location
# Default: ./data/ for local development
# Override: /app/data/ for Koyeb volume mount
SSX_DATA_PATH = os.getenv("SSX_DATA_PATH", "./data/")

# Ensure the data directory exists (create if not present)
# This handles both local dev and Koyeb volume scenarios
os.makedirs(SSX_DATA_PATH, exist_ok=True)

# User data JSON file path
USER_DATA_JSON = os.path.join(SSX_DATA_PATH, "user_data.json")

# Downloads folder path
DOWNLOADS_PATH = os.path.join(SSX_DATA_PATH, "downloads")

# Ensure downloads directory exists
os.makedirs(DOWNLOADS_PATH, exist_ok=True)


# =======================================================================
# SAFETY TOGGLE LOGIC (Policy Agent)
# SSX "Dungeon Rules" - Global content filtering switches
# =======================================================================

# Content safety toggles - block specific content types
# Set via environment variables (default: True for safety)
BLOCK_LOLI = os.getenv("SSX_BLOCK_LOLI", "true").lower() in ("true", "1", "yes")
BLOCK_REAL_WORLD = os.getenv("SSX_BLOCK_REAL_WORLD", "true").lower() in ("true", "1", "yes")
BLOCK_AI = os.getenv("SSX_BLOCK_AI", "true").lower() in ("true", "1", "yes")

# Search interval - prevents aggressive scraping (default: 1800 seconds = 30 minutes)
search_interval = int(os.getenv("SSX_SEARCH_INTERVAL", "1800"))


# =======================================================================
# E621 MODERATION CONFIG (SSX Dungeon Channel)
# =======================================================================
# Channel IDs for e621 link monitoring and mod notifications
E621_WATCH_CHANNEL_ID = os.getenv("E621_WATCH_CHANNEL_ID", "")
E621_MOD_LOG_CHANNEL_ID = os.getenv("E621_MOD_LOG_CHANNEL_ID", "")

# e621 API configuration
# User-Agent required by e621 TOS - do not change without reading TOS
E621_USER_AGENT = os.getenv(
    "E621_USER_AGENT",
    "ssX-modbot/1.0 (supersonic-xserver; contact via GitHub)"
)

# Worker counts - keep low for memory efficiency (512MB RAM)
E621_FETCHER_WORKERS = int(os.getenv("E621_FETCHER_WORKERS", "2"))
E621_EVALUATOR_WORKERS = int(os.getenv("E621_EVALUATOR_WORKERS", "2"))


# =======================================================================
# DO NOT REMOVE ANY VARIABLE BELOW OR THE PROGRAM WILL CRASH!!
# =======================================================================
# Below this line are the original configuration variables.
# Keep them exactly as they were - they are referenced by other modules.
# =======================================================================

#-----------------------bot config session----------------------

maxiumprofile = 3   #The maximun profiles number every actual user could own. 

interval = (3600*6)  #The interval (second) between every run of spiderfunction.

timeoutRetry = 5 #Retry times to deal with the timeout issue while sending users' searching result.

forceCookiesEH = True # This variable would try to use user's cookies to access e-hentai after these cookies failed to access exhentai.

rest = "3-8" #The interval of every page of the exh/eh for every user

searchInterval = '60-90'  #The search interval between every user
                          # Example 60 or 60-90

userPageLimit = 5 # The pages limitation (integret number) for every user's searching process at every searching cycle.
                  # Since e-hentai/exhentai would not update a huge amount of galleries in the preset time period
                  # (six hours), setting it to a large number is useless and would add some pressure to
                  # e-hentai/exhentai's server.  

#------------------------spider session-------------------------
# SSX STEALTH MODE - Rotating User-Agents for anti-ban protection
ROTATING_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/120.0.0.0",
]

headers = [{"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36",}]  
# This list contains some headers for the requests module.
# Bot admin do not need to alter this variable in ordinary situation.

langkeys = ['gay', 'Gay', 'RUS', 'Indonesian', 'Osomatsu-san', 'English', 'Korean', 'korean', 'Thai', 'Spanish', 'Vietnamese', 'Russian', 'French', 'Italian', 'Portuguese', 'German'] 
#This list contains ALL the UNWANTED keywords the bot admin wants to rule out based on the titles in the index pages of e-hentai/exhentai.
#Since the history reason, it keeps to be called langkeys.


noEngOnlyGallery = True  # Discard all the gerallies only containing English title and not suitable to Eastern users.

dlFullPreviewImage = True # This variable would determin whether the bot downloads the first image in the gallery as 
                          # the preview image sending to channels.
                          # While it would provide a better image quality, it would also consume e-hentai image quota.

useEngTitle = False       # If true, the bot would return the galleries' English titles to user. Else it would return 
                          # Japanese titles (if have).

#------------------------stealth session-------------------------
dlRetry = 3  # This variable determins the retry times while downloading preview images.

dlThreadLimit = 3  # This variable determins the maximum thread amount while downloading preview images.
                   # While exploiting too many threads to DL images, e-h would ban the ip.
                   # If efficiency is not a major concern, please set this variable to 1.

unwantedfemale = ['ssbbw']
unwantedmale = ['ssbbw'] 
unwantedmisc = []

wantedfemale = []
wantedmale = []
wantedmisc = []

#Rule out the unwanted tags and keep the wanted tags
#If the gallery's tags contain both unwanted and wanted tags simultaneously, the program would keep the gallery 
#Please use underline to replace space
