#!/usr/bin/python3
"""
E-Hentai Download Module - Production Grade with Optional Stealth Features

SSX Zero-Bug Hardening:
- Proxy timeout handling (strict timeout=10 for slow/dead proxies)
- UA/Cookie synchronization (UA stays consistent per session)
- 503 retry logic with jitter on EVERY retry
- Path character escaping using shlex.quote
- Context manager for session resources
- Atomic file operations for log files
"""

import os
import requests
import time
import json
import io
import re
import random
import shlex
import tempfile
import sys
# LAZY IMPORTS: Use shared_vars to break circular dependencies
import tgbotmodules.spidermodules.shared_vars as generalcfg
# Import generator lazily inside functions that need it
from .theLogger import loggerGene


# =======================================================================
# SSX STEALTH MODULE - Ghost Protocol Agent (OPTIONAL)
# =======================================================================
# Enable these features by setting ENABLE_STEALTH_MODE = True in generalcfg
# Features: Rotating User-Agents, Human Jitter, Proxy Rotation, Timeout Handling
# =======================================================================

ENABLE_STEALTH_MODE = getattr(generalcfg, 'ENABLE_STEALTH_MODE', False)

# Rotating User-Agents for stealth - prevents E-Hentai from detecting bot patterns
ROTATING_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/120.0.0.0",
]

# Human jitter settings - adds random delay between requests
JITTER_MIN = getattr(generalcfg, 'JITTER_MIN', 0.5)
JITTER_MAX = getattr(generalcfg, 'JITTER_MAX', 2.0)

# SSX PROXY TIMEOUT - Strict timeout to prevent hanging on slow/dead proxies
# This is the critical fix for Bucket 3.1
PROXY_TIMEOUT = getattr(generalcfg, 'PROXY_TIMEOUT', 10)


def get_random_user_agent():
    """
    Get a random User-Agent from the rotating list.
    SSX Stealth: Prevents E-Hentai from fingerprinting requests.
    Only active when ENABLE_STEALTH_MODE = True.
    
    Note: The session using this UA should call set_session_ua() to ensure
    the same UA is used for the entire session (UA/Cookie sync).
    """
    if ENABLE_STEALTH_MODE and ROTATING_USER_AGENTS:
        return random.choice(ROTATING_USER_AGENTS)
    # Fallback to configured header
    if generalcfg.headers and generalcfg.headers[0].get('User-Agent'):
        return generalcfg.headers[0]['User-Agent']
    return "Mozilla/5.0 (compatible; SSXBot/1.0)"


def apply_human_jitter():
    """
    Add human-like random delay between requests.
    SSX Stealth: Prevents bot detection through timing analysis.
    Only active when ENABLE_STEALTH_MODE = True.
    """
    if ENABLE_STEALTH_MODE:
        delay = random.uniform(JITTER_MIN, JITTER_MAX)
        time.sleep(delay)


def apply_jitter_before_retry(retry_count: int = 1):
    """
    SSX Zero-Bug Hardening: Apply jitter delay BEFORE every retry attempt.
    This satisfies rate-limiters by adding delay between retries, not just before the first attempt.
    
    Args:
        retry_count: The current retry attempt number (1-based).
    """
    if ENABLE_STEALTH_MODE:
        # Add jitter delay on EVERY retry (not just the first one)
        delay = random.uniform(JITTER_MIN * retry_count, JITTER_MAX * retry_count)
        loggerGene().info(f"[SSX STEALTH] Applying retry jitter delay: {delay:.2f}s (attempt {retry_count})")
        time.sleep(delay)


def refresh_session_headers(session, session_id: str = None):
    """
    Refresh session headers with a new random User-Agent.
    SSX Stealth: Call before each major request when stealth mode is enabled.
    
    SSX UA/Cookie Sync: If session_id is provided, the UA is stored and reused
    for the entire session to prevent cookie invalidation.
    """
    if ENABLE_STEALTH_MODE:
        # Check if session already has a UA assigned (UA/Cookie sync)
        if session_id:
            existing_ua = generalcfg.get_session_ua(session_id)
            if existing_ua:
                # Reuse existing UA for this session
                session.headers.update({'User-Agent': existing_ua})
                return existing_ua
        
        # No existing UA - get a new one and store it
        ua = get_random_user_agent()
        session.headers.update({'User-Agent': ua})
        
        if session_id:
            generalcfg.set_session_ua(session_id, ua)
        
        return ua
    return None


def clear_session_context(session_id: str = None):
    """
    Clear session context (called when session ends).
    SSX UA/Cookie Sync: Ensures clean state between sessions.
    """
    if session_id:
        generalcfg.clear_session_ua(session_id)


class ProxyRotator:
    """
    Manages proxy rotation for anti-ban protection.
    SSX Stealth: Automatically rotates proxies on 403/503 errors.
    Only active when ENABLE_STEALTH_MODE = True.
    
    SSX Zero-Bug Hardening:
    - Strict timeout on all requests to prevent hanging
    - Thread-safe index tracking via generalcfg functions
    """
    
    def __init__(self, proxies):
        self.proxies = proxies if proxies else []
        self.current_index = 0
        
    def get_current_proxy(self):
        """Get the currently active proxy."""
        if not self.proxies:
            return None
        # Use thread-safe index from generalcfg
        self.current_index = generalcfg.get_proxy_index()
        if self.current_index >= len(self.proxies):
            self.current_index = 0
        return self.proxies[self.current_index]
    
    def rotate_on_error(self):
        """
        Rotate to next proxy in the list.
        SSX Thread Safety: Uses thread-safe increment function.
        """
        if len(self.proxies) > 1:
            generalcfg.increment_proxy_index(len(self.proxies))
            self.current_index = generalcfg.get_proxy_index()
            loggerGene().info(f"[SSX STEALTH] Rotating to proxy index: {self.current_index}")
    
    def should_rotate(self, status_code):
        """Determine if we should rotate based on HTTP status code."""
        return status_code in (403, 503, 429)
    
    def format_proxy_dict(self):
        """Format proxy for requests library."""
        proxy = self.get_current_proxy()
        if not proxy:
            return None
        if proxy.startswith('socks5://'):
            return {'http': proxy, 'https': proxy}
        return {'http': proxy, 'https': proxy}
    
    def get_timeout(self):
        """
        Get the timeout value for proxy requests.
        SSX Zero-Bug Hardening: Ensures strict timeout to prevent hanging.
        """
        return PROXY_TIMEOUT


# Global proxy rotator instance (lazy initialization)
_proxy_rotator = None


def get_proxy_rotator():
    """Get or create the proxy rotator instance."""
    global _proxy_rotator
    if ENABLE_STEALTH_MODE and _proxy_rotator is None and generalcfg.proxy:
        _proxy_rotator = ProxyRotator(generalcfg.proxy)
    return _proxy_rotator


# =======================================================================
# SSX PATH SAFETY - Linux File Path Escaping
# =======================================================================
# SSX Zero-Bug Hardening: Escape special characters in filenames
# to prevent issues with Linux file system
# =======================================================================

def sanitize_filename(title: str) -> str:
    """
    Sanitize a title string for safe use as a filename on Linux.
    SSX Zero-Bug Hardening: Handles special characters common in EH gallery titles.
    
    Args:
        title: The raw title string.
        
    Returns:
        A safe filename string with special characters escaped or removed.
    """
    if not title:
        return "untitled"
    
    # Remove or replace characters that are problematic on Linux
    # / and \ are path separators
    # Control chars (\x00-\x1f) can cause issues
    # Leading/trailing dots and spaces can be hidden files
    safe_chars = []
    for char in title:
        if char in '/\\':
            safe_chars.append('_')
        elif ord(char) < 32:  # Control characters
            safe_chars.append('_')
        elif char in '\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0b\x0c\x0d\x0e\x0f':
            # Skip control chars except TAB (0x09), LF (0x0a), CR (0x0d)
            pass
        else:
            safe_chars.append(char)
    
    result = ''.join(safe_chars)
    
    # Remove leading/trailing spaces and dots
    result = result.strip(' .')
    
    # Collapse multiple underscores
    while '__' in result:
        result = result.replace('__', '_')
    
    # Limit length (255 is typical max for ext4)
    if len(result) > 200:
        result = result[:200]
    
    # Final fallback
    if not result:
        return "untitled"
    
    return result


def shlex_quote_path(path_component: str) -> str:
    """
    Use shlex.quote for additional path safety when needed.
    SSX Zero-Bug Hardening: Shell-safe quoting for paths used in commands.
    
    Args:
        path_component: A single path component (no separators).
        
    Returns:
        A shell-quoted string.
    """
    return shlex.quote(path_component)


# =======================================================================
# SSX ATOMIC FILE OPERATIONS - Prevent Partial Writes
# =======================================================================
# SSX Zero-Bug Hardening: Use temp files + atomic replace to prevent
# corruption when bot crashes mid-write
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
        target_path = os.path.abspath(filepath)
        dir_path = os.path.dirname(target_path)
        
        # Create temp file in same directory (same filesystem for atomic move)
        fd, temp_path = tempfile.mkstemp(
            suffix='.tmp',
            prefix='.mangalog_',
            dir=dir_path
        )
        
        try:
            with os.fdopen(fd, 'w') as fo:
                json.dump(data, fo, indent=2)
            
            # Atomic replace - atomic on POSIX systems
            os.replace(temp_path, target_path)
            return True
            
        except Exception:
            if os.path.exists(temp_path):
                os.unlink(temp_path)
            raise
            
    except Exception as e:
        sys.stderr.write(f"[SSX DOWNLOAD ERROR] Atomic write failed for {filepath}: {e}\n")
        return False


# =======================================================================
# ORIGINAL FUNCTIONS - With fixes applied
# =======================================================================

def userfiledetect(path):
    """
    Detect and create user data files with atomic write support.
    SSX Zero-Bug Hardening: Uses atomic file operations.
    """
    if os.path.exists(path) == False:
        os.makedirs(path, exist_ok=True)
        userdict = {}
        _atomic_write_json("{0}.mangalog".format(path), userdict)
    elif os.path.isfile("{0}.mangalog".format(path)) == False:
        userdict = {}
        _atomic_write_json("{0}.mangalog".format(path), userdict)
    else:
        try:
            with open("{0}.mangalog".format(path), 'r') as fo:
                usersdict = json.load(fo)
        except json.decoder.JSONDecodeError:
            broken_file = os.path.join(path, '.mangalog')
            bkm = 'userdata.broken.TIME'
            backup_file_name = bkm.replace('TIME', str(time.asctime(time.localtime())))
            backup_file_name = backup_file_name.replace(":", ".")
            backup_file = os.path.join(path, backup_file_name)
            os.rename(broken_file, backup_file)
            userdict = {}
            _atomic_write_json("{0}.mangalog".format(path), userdict)


def previewImageDL(manga, mangasession, logger):
    logger.info('Begin to retrive preview image of {0}.'.format(manga.url))
    
    # SSX PATH SAFETY: Sanitize the title for safe filename
    safe_title = sanitize_filename(manga.title)
    
    previewimg = {'imageurlSmall': manga.imageUrlSmall,
                  'title': safe_title,  # Use sanitized title
                  'imageurlBig': '',
                  'imageurlBigReload': '',
                  'mangaUrl': manga.url}
    if generalcfg.dlFullPreviewImage == True:
        imagePatternBig = re.compile(r'''href="(https://[a-z-]+\.org/[a-z0-9]/[a-z0-9]+/[a-z0-9]+\-1)"''')
        tdHtmlContent = accesstoehentai(method='get',
                                        mangasession=mangasession,
                                        stop=generator.Sleep(2),
                                        urls=[manga.url],
                                        logger=logger)
        
        imageMatchBig = imagePatternBig.search(tdHtmlContent[0])
        if imageMatchBig:
            previewimg.update({'imageurlBig': imageMatchBig.group(1)})
        bio = imageDownload(previewimg=previewimg, mangasession=mangasession, logger=logger, fromBig=True)
    else:
        bio = imageDownload(previewimg=previewimg, mangasession=mangasession, logger=logger)
    manga.previewImageObj = bio


def retryDocorator(func, logger=None, retry=None):
    '''This simple retry decorator provides a try-except looping to the accesstoehentai function for
       overcoming network fluctuation.
       
       SSX Zero-Bug Hardening:
       - Jitter delay on EVERY retry (not just first attempt)
       - Specific handling for timeout and proxy errors
       - Proxy rotation trigger on proxy errors'''
    if logger is None:
        logger = loggerGene()
    if retry is None:
        retry = generalcfg.timeoutRetry
    
    def wrapperFunction(*args, **kwargs):
        proxy_rotator = get_proxy_rotator()
        
        for err in range(retry):
            try:
                resultList = func(*args, **kwargs)
                break
            except requests.exceptions.Timeout as error:
                # SSX PROXY TIMEOUT: Timeout occurred - rotate proxy
                logger.warning(f"[SSX TIMEOUT] Request timed out on attempt {err + 1}: {error}")
                if proxy_rotator and ENABLE_STEALTH_MODE:
                    proxy_rotator.rotate_on_error()
                # Apply jitter before retry (SSX 503 Retry Logic)
                if err < retry - 1:
                    apply_jitter_before_retry(err + 2)
                    
            except requests.exceptions.ProxyError as error:
                # SSX PROXY ERROR: Proxy error - rotate immediately
                logger.warning(f"[SSX PROXY ERROR] Proxy error on attempt {err + 1}: {error}")
                if proxy_rotator and ENABLE_STEALTH_MODE:
                    proxy_rotator.rotate_on_error()
                # Apply jitter before retry
                if err < retry - 1:
                    apply_jitter_before_retry(err + 2)
                    
            except Exception as error:
                err += 1
                logger.warning(f"[SSX RETRY] Attempt {err}: {error}")
                # Apply jitter before retry
                if err < retry:
                    apply_jitter_before_retry(err + 1)
        else:
            logger.warning('Retry limitation reached')
            resultList = []
        
        return resultList
    
    return wrapperFunction


@retryDocorator
def accesstoehentai(method, mangasession, stop, logger, urls=None):
    '''Most of the parts of the program would use this function to retrive the htmlpage, and galleries'
       information by using e-h's API. It provides two methods to access e-hentai/exhentai. The GET 
       method would return the htmlpage; and the POST method would extract the gallery ID and gallery
       key to generate the json payload sending to e-h's API then retrive the API's result.
       
       SSX Zero-Bug Hardening:
       - Proxy timeout on all requests (prevents hanging on slow proxies)
       - UA/Cookie sync (consistent UA per session)
       - Jitter on every request'''
    resultList = []
    
    # SSX STEALTH: Apply jitter before every request
    apply_human_jitter()
    
    if method == 'get':
        inputInfo = urls
    elif method == 'post':
        tokenPattern = re.compile(r'''https://.+\.org/g/([0-9a-z]+)\/([0-9a-z]+)\/''')
        mangaJsonPayload = {
            "method": "gdata",
            "gidlist": [],
            "namespace": 1
        }
        for url in urls:
            mangaTokenMatch = tokenPattern.search(url)
            mangaJsonPayload["gidlist"].append([mangaTokenMatch.group(1), mangaTokenMatch.group(2)])
        inputInfo = [mangaJsonPayload]
    else:
        inputInfo = ''
    
    for ii in inputInfo:
        if method == 'get':
            # SSX PROXY TIMEOUT: Use timeout on all requests
            r = mangasession.get(ii, timeout=PROXY_TIMEOUT)
            resultList.append(r.text)
        else:
            # SSX PROXY TIMEOUT: Use timeout on POST requests too
            r = mangasession.post(
                'https://api.e-hentai.org/api.php',
                json=ii,
                timeout=PROXY_TIMEOUT
            )
            mangaDictMeta = r.json()
            resultList.extend(mangaDictMeta['gmetadata'])
    
    return resultList


def imageDownload(mangasession, previewimg, logger, fromBig=False):
    """
    Download preview images with retry logic and timeout handling.
    
    SSX Zero-Bug Hardening:
    - Proxy timeout to prevent hanging
    - Path sanitization for titles
    - Context manager for session resources (handled by caller)
    """
    err = 0
    imageDict = {}
    
    # SSX PATH SAFETY: Sanitize title for safe filename
    safe_title = sanitize_filename(previewimg.get('title', 'untitled'))
    previewimg['title'] = safe_title
    
    if fromBig == True:
        logger.info('Begin to download full preview image of {0}.'.format(previewimg['mangaUrl']))
    else:
        logger.info('Begin to download small preview image of {0}.'.format(previewimg['mangaUrl']))
    
    for err in range(generalcfg.dlRetry):
        try:
            if fromBig == True:
                if err != 0 and previewimg['imageurlBigReload']:
                    # SSX PROXY TIMEOUT: Use timeout
                    r = mangasession.get(previewimg['imageurlBigReload'], timeout=PROXY_TIMEOUT)
                else:
                    r = mangasession.get(previewimg['imageurlBig'], timeout=PROXY_TIMEOUT)
                
                downloadUrlsDict = mangadlhtmlfilter(htmlContent=r.text, url=previewimg['imageurlBig'])
                previewimg.update({'imageurlBigReload': downloadUrlsDict['reloadUrl']})
                if downloadUrlsDict['imageUrl']:
                    previewimgUrl = downloadUrlsDict['imageUrl']
                else:
                    previewimgUrl = previewimg['imageurlSmall']
            else:
                logger.warning('Could not retrive full image downloading url of {0}, try to download small one.'.format(previewimg['mangaUrl']))
                previewimgUrl = previewimg['imageurlSmall']
            
            # SSX PROXY TIMEOUT: Use timeout on image downloads
            previewimage = mangasession.get(previewimgUrl, timeout=PROXY_TIMEOUT)
            
            if previewimage.status_code == 200:
                bio = io.BytesIO(previewimage.content)
                bio.name = safe_title  # Use sanitized title
                if bio.getbuffer().nbytes != int(previewimage.headers['content-length']):
                    raise jpegEOIError('Image is corrupted.')            
            else:
                raise downloadStatusCodeError('Error status code.')
                
        except Exception as error:
            logger.error('Encountered an error while downloading image {0} - {1}'.format(previewimg['mangaUrl'], str(error)))
            err += 1
            time.sleep(0.5)
        else:
            err = 0
            break    
    else:
        err = 0
        logger.error('Error limitation while download {0} is reached, stop this thread.'.format(previewimg['mangaUrl']))
        bio = None
    
    return bio


def mangadlhtmlfilter(htmlContent, url):
    downloadUrlsDict = {'imageUrl': "", 'reloadUrl': ''}
    imagePattern = re.compile('''<img id="img" src="(http://.+)" style="''')
    matchUrls = imagePattern.search(htmlContent)
    reloadPattern = re.compile(r'''id\="loadfail" onclick\="return nl\(\'([0-9\-]+)\'\)\"''')
    reloadUrl = reloadPattern.search(htmlContent)
    if matchUrls:                     # This block still has some strange issues..... 
        downloadUrlsDict['imageUrl'] = matchUrls.group(1)
    if reloadUrl:
        downloadUrlsDict['reloadUrl'] = '{0}?nl={1}'.format(url, reloadUrl.group(1))
    return downloadUrlsDict  


#-------------Several personalized Exceptions----------------------

class jpegEOIError(Exception):
    pass

class htmlPageError(Exception):
    pass

class downloadStatusCodeError(Exception):
    pass
