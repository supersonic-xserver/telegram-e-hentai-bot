#!/usr/bin/python3
"""
SSX Shared Constants Module - NO IMPORTS

This module contains ONLY static configuration constants.
It must NOT import any other SSX modules to break circular dependencies.

This file is a "leaf" module in the dependency graph - it has no dependencies.
"""

import os
from dotenv import load_dotenv

# Load .env for local development
load_dotenv()

# =======================================================================
# BASIC CONFIGURATION (No SSX module imports allowed here!)
# =======================================================================

# Bot Configuration
token = os.getenv("TG_TOKEN", "")
passcode = os.getenv("TG_PASSCODE", "This is a test passcode.")
maxiumprofile = 3
adminID = int(os.getenv("TG_ADMIN_ID", "0") or 0)
pubChannelID = os.getenv("TG_PUB_CHANNEL_ID", "")

# Ghost Drive Configuration
DATABASE_CHANNEL_ID = os.getenv("TG_DATABASE_CHANNEL_ID", "")
GHOST_DRIVE_SYNC_INTERVAL = int(os.getenv("SSX_GHOST_SYNC_INTERVAL", "1200"))
GHOST_DRIVE_BACKUP_PREFIX = "SSX_BACKUP_"

# Network Configuration
_proxy_url = os.getenv("PROXY_URL", "")
if _proxy_url:
    if not _proxy_url.startswith(("http://", "https://", "socks5://", "socks4://")):
        _proxy_url = "http://" + _proxy_url
    proxy = [_proxy_url]
else:
    proxy = []

TG_PROXY = os.getenv("TG_PROXY", "")

# Search Configuration
interval = (3600 * 6)
timeoutRetry = 5
forceCookiesEH = True
rest = "3-8"
searchInterval = '60-90'
userPageLimit = 5

# Stealth Configuration
ENABLE_STEALTH_MODE = True
PROXY_TIMEOUT = 10
JITTER_MIN = 0.5
JITTER_MAX = 2.0

headers = [{"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36",}]

# Content Filtering
langkeys = ['gay', 'Gay', 'RUS', 'Indonesian', 'Osomatsu-san', 'English', 'Korean', 'korean', 'Thai', 'Spanish', 'Vietnamese', 'Russian', 'French', 'Italian', 'Portuguese', 'German']
noEngOnlyGallery = True
dlFullPreviewImage = True
useEngTitle = False

# DL Configuration
dlRetry = 3
dlThreadLimit = 3

# Tags
unwantedfemale = ['ssbbw']
unwantedmale = ['ssbbw']
unwantedmisc = []
wantedfemale = []
wantedmale = []
wantedmisc = []

# Safety Toggles
BLOCK_LOLI = os.getenv("SSX_BLOCK_LOLI", "true").lower() in ("true", "1", "yes")
BLOCK_REAL_WORLD = os.getenv("SSX_BLOCK_REAL_WORLD", "true").lower() in ("true", "1", "yes")
BLOCK_AI = os.getenv("SSX_BLOCK_AI", "true").lower() in ("true", "1", "yes")
search_interval = int(os.getenv("SSX_SEARCH_INTERVAL", "1800"))

# Path Configuration
SSX_DATA_PATH = os.getenv("SSX_DATA_PATH", "./data/")
os.makedirs(SSX_DATA_PATH, exist_ok=True)
USER_DATA_JSON = os.path.join(SSX_DATA_PATH, "user_data.json")
DOWNLOADS_PATH = os.path.join(SSX_DATA_PATH, "downloads")
os.makedirs(DOWNLOADS_PATH, exist_ok=True)

# Rotating User Agents
ROTATING_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/120.0.0.0",
]
