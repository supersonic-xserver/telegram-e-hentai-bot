#!/usr/bin/python3
import os
import sys
import re
from typing import List, Tuple, Optional
from urllib.parse import urlparse

# =============================================================================
# SSX DUNGEON RULES - SAFETY FILTER
# =============================================================================
# Nuclear Tags (Illegal/Exploitative Content)
# Real-World Content Tags (Photos/Videos - Drawn Art Only Policy)
# AI-Generated Content Block
# Domain Firewall (Real-World Porn Site Blocklist)
# =============================================================================
# The Nuclear domain bucket - Top 50 real-world porn domains
# These domains are NEVER touched by this bot - DRAWN ART ONLY POLICY
# Includes all major tube sites, pay sites, cam sites, and social platforms
# =============================================================================

BLOCKED_DOMAINS = [
    # Major Tube Sites
    "pornhub.com",
    "xvideos.com",
    "xnxx.com",
    "xhamster.com",
    "spankbang.com",
    "youporn.com",
    "redtube.com",
    "tube8.com",
    "tubeum.com",
    "eporner.com",
    "hqporner.com",
    "thumbzilla.com",
    "sunporno.com",
    "porntube.com",
    "xtube.com",
    "empflix.com",
    "freeporn.com",
    "porn.com",
    "pornone.com",
    "porndroids.com",
    "fapality.com",
    "keezmovies.com",
    "sleazyneasy.com",
    "slutload.com",
    "stileproject.com",
    "xvideo.cc",
    "pornhd.com",
    "gotporn.com",
    "anyporn.com",
    "hellporno.com",
    
    # Pay Sites / Studios
    "onlyfans.com",
    "brazzers.com",
    "bangbros.com",
    "naughtyamerica.com",
    "digitaldesire.com",
    "realitykings.com",
    "mofos.com",
    "bignaturals.com",
    "bigbuttslikebigdicks.com",
    "bigboobsalerts.com",
    "ddfnetwork.com",
    "povd.com",
    "tonightscl.com",
    "fakehub.com",
    "kink.com",
    "evilangel.com",
    "passporthealth.com",
    "proporn.com",
    "porndoe.com",
    
    # Live Cam / Chat Roulette Sites
    "chaturbate.com",
    "livejasmin.com",
    "bongacams.com",
    "myfreecams.com",
    "streamate.com",
    "imlive.com",
    "flirt4free.com",
    "camsoda.com",
    "cam4.com",
    "camwithher.com",
    "jalife.com",
    "xnview.com",
    "roulette.com",
    "omegle.com",
    "chatroulette.com",
    "coomeet.com",
    "chatspin.com",
    "dirtyroulette.com",
    "tinychat.com",
    "paltalk.com",
    
    # Social/Link-In-Bio Platforms Known for Porn
    "linktr.ee",
    "beacons.ai",
    "linkpop.com",
    "lnk.bio",
    "bio.link",
    
    # Misc Real-World Adult Platforms
    "ashemaletube.com",
    "shemalelist.com",
    "trannytube.tv",
    "shef4hetero.com",
    "faptime.com",
    "pornmd.com",
    "aeronca.com",
    "sex.com",
    "xxx.com",
    "yuvutu.com",
    "xozk.com",
]

# =======================================================================
# PERFORMANCE OPTIMIZER - Regex Engine
# =======================================================================
# Pre-compiled patterns and optimized domain lookup for O(1) performance.
# SSX Policy: DRAWN ART ONLY - No real-world porn sites allowed.
# =======================================================================

# Create sets for O(1) lookups
_BLOCKED_DOMAIN_SET = set(BLOCKED_DOMAINS)

# Pre-build suffix set for O(1) subdomain matching
# Store all possible suffixes: "com", "pornhub.com", "rt.pornhub.com" patterns
_BLOCKED_DOMAIN_SUFFIXES = set()
for domain in _BLOCKED_DOMAIN_SET:
    # Add the domain itself
    _BLOCKED_DOMAIN_SUFFIXES.add(domain)
    # Add common subdomain patterns
    parts = domain.split('.')
    if len(parts) == 2:
        _BLOCKED_DOMAIN_SUFFIXES.add('.' + domain)
        _BLOCKED_DOMAIN_SUFFIXES.add('www.' + domain)


def extract_domain_from_url(url: str) -> Optional[str]:
    """
    Extract the root domain from a URL for blocking purposes.
    Handles subdomains by extracting the registered domain.
    
    SSX Policy: This function is part of the Domain Firewall that
    enforces the "Drawn Art Only" rule by blocking real-world porn sites.
    
    Args:
        url: The URL to parse.
        
    Returns:
        The root domain (e.g., 'pornhub.com' from 'rt.pornhub.com').
    """
    if not url:
        return None
    
    try:
        # Add scheme if missing
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        
        # Remove port if present
        if ':' in domain:
            domain = domain.split(':')[0]
        
        # Remove 'www.' prefix for consistent matching
        if domain.startswith('www.'):
            domain = domain[4:]
        
        return domain
    except Exception:
        return None


def get_root_domain(domain: str) -> str:
    """
    Extract root domain from potentially subdomain-containing domain.
    e.g., 'rt.pornhub.com' -> 'pornhub.com'
    
    SSX Policy: Used by the Domain Firewall to match subdomains
    against blocked parent domains.
    
    Args:
        domain: The domain string.
        
    Returns:
        The root domain.
    """
    parts = domain.split('.')
    if len(parts) >= 2:
        # Return last two parts as root domain
        return '.'.join(parts[-2:])
    return domain


def is_domain_blocked(url_or_domain: str) -> Tuple[bool, Optional[str]]:
    """
    Check if a URL or domain is in the blocked list.
    Uses optimized O(1) set lookup with suffix matching for subdomains.
    
    SSX Policy: This is the Domain Firewall Agent that enforces the
    "Drawn Art Only" rule by blocking all real-world porn domains.
    
    Performance Optimizations:
    - Fast-fail on URLs under 10 characters
    - O(1) direct set lookup
    - O(1) endswith suffix matching
    
    Args:
        url_or_domain: URL or domain string to check.
        
    Returns:
        Tuple of (is_blocked, blocked_domain).
    """
    # =======================================================================
    # FAST-FAIL - Early exit for short strings
    # =======================================================================
    # Skip expensive parsing for obviously invalid inputs
    if not url_or_domain or len(url_or_domain) < 10:
        return False, None
    
    domain = extract_domain_from_url(url_or_domain)
    if not domain:
        return False, None
    
    # =======================================================================
    # O(1) DIRECT MATCH CHECK
    # =======================================================================
    if domain in _BLOCKED_DOMAIN_SET:
        return True, domain
    
    # =======================================================================
    # O(1) SUFFIX MATCH - Uses Python's optimized endswith()
    # =======================================================================
    # Check if domain ends with any blocked domain (catches subdomains)
    # Python's endswith() is highly optimized in C
    for blocked in _BLOCKED_DOMAIN_SET:
        if domain.endswith('.' + blocked):
            return True, blocked
    
    return False, None


# Default blocked tags - SSX Dungeon Rules Enforcement
SSX_BLOCKED_TAGS_DEFAULT = [
    # Nuclear Tags - Illegal/Exploitative
    "loli",
    "shota",
    "cub",
    "feral",
    "toddlercon",
    "underage",
    "rape",
    "snuff",
    "guro",
    "nonconsensual",
    "mind break",
    "incest",
    "bestiality",
    "animal on girl",
    "animal on boy",
    "animal on furry",
    
    # Real-World Content - Drawn Art Only Policy
    "cosplay",
    "asian porn",
    "western porn",
    "real",
    "realistic",
    
    # AI-Generated Content Block
    "ai-generated",
    "ai_art",
]

# High-risk tags that warrant admin notification
SSX_HIGH_RISK_TAGS = [
    "loli",
    "shota",
    "cub",
    "toddlercon",
    "underage",
    "rape",
    "snuff",
    "guro",
    "bestiality",
]

# =======================================================================
# REGEX BENCHMARKER - Optimized Patterns
# =======================================================================
# Optimized for performance:
# - Non-capturing groups (?:...) to save memory
# - Word boundaries \b for precise matching
# - Atomic patterns that can't cause catastrophic backtracking
# =======================================================================

# Regex patterns for heuristic content scanning
# Catches variations, leetspeak, and encoded attempts
SSX_HEURISTIC_PATTERNS = [
    # Illegal content variations (using non-capturing groups)
    r'\bloli\b',
    r'\bshota\b',
    r'\bcub\b',
    r'\btoddler\b',
    r'\bunderage\b',
    r'\brape\b',
    r'\bsnuff\b',
    r'\bguro\b',
    r'\bincest\b',
    r'\bbeastial(?:ity|ism)\b',
    r'\banimal\s*(?:on\s*)?(?:girl|boy|furry)\b',
    
    # Real content indicators (using non-capturing groups)
    r'\breal\s*(?:life|photo|porn)?\b',
    r'\bcosplay\s*(?:porn)?\b',
    r'\basian\s*porn\b',
    r'\bwestern\s*porn\b',
    r'\bphot(?:o|ograph(?:y|ic))\b',
    
    # AI content (using non-capturing groups)
    r'\bai[\s_-]*(?:generat|art|assisted)\b',
    r'\bstable\s*diffusion\b',
    r'\bmidjourney\b',
    r'\bnovelai\b',
    r'\bcomfyui\b',
    
    # =======================================================================
    # REAL-WORLD KEYWORDS - Content Auditor
    # =======================================================================
    # These keywords indicate real-world/真人 content that violates
    # the "Drawn Art Only" policy. All use word boundaries to prevent
    # false positives (e.g., "irl" won't match "girl" or "world").
    # =======================================================================
    
    # Live Cam / Webcam indicators
    r'\blive\s*(?:cam|show|stream|porn)?\b',
    r'\bwebcam\b',
    r'\bweb\s*cam\b',
    r'\b(?:cam|show|model)\s*(?:girl|boy|woman|man|babe|shoot)\b',
    r'\b(?:private|public)\s*show\b',
    
    # Amateur / Homemade indicators
    r'\bamateur\b',
    r'\bhomemade\b',
    r'\bhome\s*(?:made|made)?\b',
    r'\bmy\s*(?:wife|girlfriend|boyfriend|husband)\b',
    
    # IRL / Real-World indicators - WORD BOUNDARIES CRITICAL
    # These prevent false positives like "girl" triggering on "irl"
    r'\birl\b',
    r'\bin\s*(?:real|the)\s*(?:life|world)\b',
    r'\b真人\b',
    r'\b实拍\b',
    
    # Premium / Pay content indicators
    r'\b(?:premium|paid|vip|exclusive)\s*(?:content|access|model)?\b',
    r'\b(?:onlyfans|fansly|only)\s*(?:only)?\b',
    r'\blink\s*(?:in\s*)?bio\b',
    r'\bsubscrib(?:e|tion|er)\b',
    r'\bppv\b',
    r'\b(?:tip|tipped)\s*(?:me)?\b',
    
    # Verified Creator / Professional indicators
    r'\bverified\s*(?:creator|model|account)?\b',
    r'\b(?:professional|pro)\s*(?:model|photographer|shoot)?\b',
    r'\b(?:studio|studios)\s*(?:production)?\b',
    
    # Social media porn indicators
    r'\b(?:twitter|instagram|tiktok)\s*(?:model|porn|leak)?\b',
    r'\b(?:tweet|gram)\s*(?:nudes|leaks)?\b',
    r'\bleak(?:ed|s)?\b',
    r'\b(?:sex|xxx)\s*tape\b',
    
    # Real model names (common search terms for real people)
    r'\b(?:porn\s*)?star\b',
    r'\b(?:adult\s*)?actor\b',
    r'\bactress\b',
]

# Compile patterns for performance
_compiled_patterns = [re.compile(p, re.IGNORECASE) for p in SSX_HEURISTIC_PATTERNS]


def _get_blocked_tags() -> List[str]:
    """
    Get the list of blocked tags from environment variable or use default SSX rules.
    
    Returns:
        List of blocked tag strings (lowercase).
    """
    blocked_tags_env = os.getenv("BLOCKED_TAGS", "")
    
    if blocked_tags_env:
        # Use environment variable if set
        return [tag.strip().lower() for tag in blocked_tags_env.split(',') if tag.strip()]
    else:
        # Use default SSX Dungeon Rules
        return [tag.lower() for tag in SSX_BLOCKED_TAGS_DEFAULT]


def check_heuristic(title: str, description: str = "") -> Tuple[bool, List[str]]:
    """
    Secondary check using regex patterns on title/description.
    Catches 'leaked' or 'laundered' content that might bypass tag matching.
    
    Args:
        title: The gallery title to scan.
        description: Optional gallery description to scan.
        
    Returns:
        Tuple of (is_blocked, list of matched patterns).
    """
    if not title and not description:
        return False, []
    
    scan_text = f"{title} {description}".lower()
    matched_patterns = []
    
    for pattern in _compiled_patterns:
        if pattern.search(scan_text):
            matched_patterns.append(pattern.pattern)
    
    return len(matched_patterns) > 0, matched_patterns


def get_offending_tags(tag_list: List[str]) -> List[str]:
    """
    Get the list of tags that would be blocked.
    
    Args:
        tag_list: List of tags to check.
        
    Returns:
        List of offending tags that match the blocklist.
    """
    blocked_tags = _get_blocked_tags()
    offending = []
    
    if tag_list:
        tag_list_lower = [tag.lower() for tag in tag_list]
    else:
        tag_list_lower = []
    
    for blocked in blocked_tags:
        if blocked.lower() in tag_list_lower:
            offending.append(blocked)
    
    return offending


def is_high_risk(tags: List[str]) -> bool:
    """
    Check if any of the tags are high-risk (warranting admin notification).
    
    Args:
        tags: List of tags to check.
        
    Returns:
        True if any tag is high-risk.
    """
    if not tags:
        return False
    
    tags_lower = [t.lower() for t in tags]
    
    for hr_tag in SSX_HIGH_RISK_TAGS:
        if hr_tag in tags_lower:
            return True
    
    return False


def is_safe(tag_list: List[str], title: str = "", gallery_id: str = "", url: str = "") -> Tuple[bool, Optional[str]]:
    """
    Check if the given tag list is safe based on the SSX Dungeon Rules.
    
    SSX Policy: DRAWN ART ONLY - This function enforces the Jesterman's Creed
    by blocking illegal/exploitative content, real-world porn, and AI-generated
    content. The Domain Firewall prevents any interaction with real-world
    porn sites.
    
    Primary check: Tag matching against blocklist.
    Secondary check: Heuristic regex scan of title/description.
    Tertiary check: Domain firewall scan for real-world porn sites.
    
    Args:
        tag_list: A list of tags to check against the blocklist.
        title: Gallery title for heuristic scanning (optional).
        gallery_id: Gallery ID for logging (optional).
        url: Gallery source URL for domain firewall check (optional).
        
    Returns:
        Tuple of (is_safe, blocked_reason).
        - (True, None) if the gallery is safe.
        - (False, reason_string) if blocked, with reason describing why.
    """
    # =======================================================================
    # FAIL-SAFE - Black-Box Exception Wrapper
    # =======================================================================
    # If ANY error occurs, fail closed (block content) for security.
    # This prevents malformed input from bypassing the safety filter.
    # =======================================================================
    try:
        return _is_safe_internal(tag_list, title, gallery_id, url)
    except Exception as e:
        # Fail-closed: block on any error to maintain security
        err_msg = f"FILTER_ERROR_FAIL_CLOSED: {str(e)[:50]}"
        log_id = gallery_id if gallery_id else "UNKNOWN"
        sys.stdout.write(f"[SSX SAFETY - ERROR] Filter crashed on {log_id}, blocking by default: {err_msg}\n")
        sys.stdout.flush()
        return False, err_msg


def _is_safe_internal(tag_list: List[str], title: str = "", gallery_id: str = "", url: str = "") -> Tuple[bool, Optional[str]]:
    """
    Internal implementation of is_safe() - wrapped by exception handler.
    
    SSX Policy: DRAWN ART ONLY - Enforces Jesterman's Creed through
    tag blocking, heuristic scanning, and domain firewall checks.
    """
    blocked_tags = _get_blocked_tags()
    blocked_reason = None
    offending_tags = []
    log_id = gallery_id if gallery_id else "UNKNOWN"
    
    # =======================================================================
    # BUCKET 1: DOMAIN FIREWALL CHECK
    # =======================================================================
    # This is the first line of defense against real-world porn sites.
    # If the source URL is from a blocked domain, nuke it immediately.
    # =======================================================================
    if url:
        domain_blocked, blocked_domain = is_domain_blocked(url)
        if domain_blocked:
            # This is a REAL_WORLD_DOMAIN block - the most severe violation
            blocked_reason = "REAL_WORLD_DOMAIN"
            log_msg = f"[SSX FIREWALL] Blocking unethical real-world domain: {blocked_domain} | Gallery: {log_id}"
            sys.stdout.write(f"{log_msg}\n")
            sys.stdout.flush()
            
            # Log the block event with proper categorization
            log_block_event(
                gallery_id=log_id,
                offending_tags=[blocked_domain],
                block_type="REAL_WORLD_PORN"
            )
            
            return False, blocked_reason
    
    # =======================================================================
    # PRIMARY CHECK: Tag Matching
    # =======================================================================
    if tag_list:
        tag_list_lower = [tag.lower() for tag in tag_list]
    else:
        tag_list_lower = []
    
    for blocked in blocked_tags:
        if blocked.lower() in tag_list_lower:
            offending_tags.append(blocked)
    
    if offending_tags:
        blocked_reason = f"BLOCKED_TAGS: {', '.join(offending_tags)}"
    
    # =======================================================================
    # SECONDARY CHECK: Heuristic Title/Description Scan
    # =======================================================================
    if title:
        heuristic_blocked, matched_patterns = check_heuristic(title)
        if heuristic_blocked:
            # Check if any matched patterns are real-world indicators
            real_world_indicators = [
                'live', 'webcam', 'amateur', 'homemade', 'irl',
                'verified', 'premium', 'ppv', 'amateur', 'cosplay'
            ]
            is_real_world_heuristic = any(
                any(ind in p.lower() for ind in real_world_indicators)
                for p in matched_patterns
            )
            
            if is_real_world_heuristic:
                # This is a REAL_WORLD_HEURISTIC block
                if blocked_reason:
                    blocked_reason += f" | HEURISTIC_REAL_WORLD: {matched_patterns}"
                else:
                    blocked_reason = f"HEURISTIC_REAL_WORLD: {matched_patterns}"
                
                log_msg = f"[SSX FIREWALL] Blocking real-world content via heuristic: {matched_patterns} | Gallery: {log_id}"
                sys.stdout.write(f"{log_msg}\n")
                sys.stdout.flush()
                
                log_block_event(
                    gallery_id=log_id,
                    offending_tags=matched_patterns,
                    block_type="REAL_WORLD_HEURISTIC"
                )
            else:
                # Regular heuristic block (illegal content, AI, etc.)
                if blocked_reason:
                    blocked_reason += f" | HEURISTIC: {matched_patterns}"
                else:
                    blocked_reason = f"HEURISTIC: {matched_patterns}"
    
    # =======================================================================
    # LOGGING & NOTIFICATIONS
    # =======================================================================
    if blocked_reason:
        log_msg = f"[SSX SAFETY] Blocked gallery {log_id} for: {blocked_reason}"
        sys.stdout.write(f"{log_msg}\n")
        sys.stdout.flush()
        
        # Check if this is a high-risk block requiring admin notification
        if is_high_risk(offending_tags):
            admin_msg = f"[SSX SAFETY - HIGH RISK] Gallery {log_id} blocked. Tags: {', '.join(offending_tags)}"
            sys.stdout.write(f"{admin_msg}\n")
            sys.stdout.flush()
        
        return False, blocked_reason
    
    return True, None


def log_block_event(gallery_id: str, offending_tags: List[str], block_type: str = "TAG_MATCH"):
    """
    Log a block event for auditing purposes.
    
    SSX Policy: All blocks are logged for the SSX Dungeon audit trail.
    This allows monitoring if the system is too aggressive or too lenient.
    
    Block Types:
    - TAG_MATCH: Regular tag-based block
    - HEURISTIC: Pattern-based heuristic block
    - REAL_WORLD_PORN: Domain firewall block (real-world porn site)
    - REAL_WORLD_HEURISTIC: Heuristic block specifically for real-world content
    - DOMAIN_LINK: Gallery contains link to blocked domain
    - FILTER_ERROR_FAIL_CLOSED: Safety filter crashed, content blocked by default
    
    Args:
        gallery_id: The blocked gallery ID.
        offending_tags: List of tags that caused the block.
        block_type: Type of block (TAG_MATCH, HEURISTIC, REAL_WORLD_PORN, etc.)
    """
    # =======================================================================
    # LOG SANITIZATION - Handle None Types Safely
    # =======================================================================
    # Sanitize inputs to prevent TypeError crashes
    safe_gallery_id = str(gallery_id) if gallery_id else "UNKNOWN"
    safe_tags = offending_tags if offending_tags else []
    safe_block_type = str(block_type) if block_type else "UNKNOWN"
    
    # Build safe tag string
    try:
        safe_tags_str = ', '.join(str(t) for t in safe_tags)
    except Exception:
        safe_tags_str = "ERROR_SERIALIZING_TAGS"
    
    # Special formatting for real-world porn blocks
    if safe_block_type in ("REAL_WORLD_PORN", "REAL_WORLD_HEURISTIC", "DOMAIN_LINK", "FILTER_ERROR_FAIL_CLOSED"):
        log_entry = f"[SSX FIREWALL AUDIT] ID={safe_gallery_id} [TYPE: {safe_block_type}] BLOCKED=[{safe_tags_str}]"
    else:
        log_entry = f"[SSX SAFETY AUDIT] ID={safe_gallery_id} TYPE={safe_block_type} TAGS=[{safe_tags_str}]"
    
    sys.stdout.write(f"{log_entry}\n")
    sys.stdout.flush()


def check_gallery_links_for_blocked_domains(links: List[str]) -> Tuple[bool, List[str]]:
    """
    Check a list of URLs/links for blocked domains.
    Used for galleries that may have embedded links to external sites.
    
    Args:
        links: List of URLs or domain strings to check.
        
    Returns:
        Tuple of (has_blocked_links, list of blocked domains found).
    """
    if not links:
        return False, []
    
    blocked_domains_found = []
    for link in links:
        is_blocked, blocked_domain = is_domain_blocked(link)
        if is_blocked and blocked_domain not in blocked_domains_found:
            blocked_domains_found.append(blocked_domain)
    
    return len(blocked_domains_found) > 0, blocked_domains_found


def get_filter_stats_summary() -> dict:
    """
    Get a summary of the current filter configuration.
    
    Returns:
        Dictionary with filter configuration info.
    """
    blocked_tags = _get_blocked_tags()
    
    return {
        "total_blocked_tags": len(blocked_tags),
        "nuclear_tags_count": len([t for t in blocked_tags if t in SSX_BLOCKED_TAGS_DEFAULT]),
        "high_risk_tags_count": len([t for t in blocked_tags if t in SSX_HIGH_RISK_TAGS]),
        "heuristic_patterns_count": len(_compiled_patterns),
        "blocked_domains_count": len(BLOCKED_DOMAINS),
        "using_default_rules": not os.getenv("BLOCKED_TAGS"),
    }


def get_blocked_domains_list() -> List[str]:
    """
    Get the list of all blocked domains.
    
    Returns:
        List of blocked domain strings.
    """
    return list(BLOCKED_DOMAINS)


# Backwards compatibility - simple boolean function
def is_safe_simple(tag_list: List[str]) -> bool:
    """
    Simple safety check (backwards compatible).
    
    Args:
        tag_list: A list of tags to check.
        
    Returns:
        True if safe, False if blocked.
    """
    safe, _ = is_safe(tag_list)
    return safe
