#!/usr/bin/env python3
"""
e621 Ruleset Module — SSX Dungeon Channel Moderation

Single source of truth for e621 moderation rules.
Imports illegal tags from safety_filter (core SSX blacklists).
Adds e621-specific rules (AI art, media type, community standard).

SSX Dungeon Rules:
- Illegal/Exploitative: from safety_filter.py (nuclear tags)
- Hard delete: Community standard (real-person content)
- AI art: ai_generated alone = DELETE, ai_assisted = KEEP
- Media type: Block video files (IRL content risk)
- Allowed: LGBTQ+ drawn, consensual BDSM, furry/anthro, ai_assisted
"""

# =======================================================================
# IMPORTS - Core SSX blacklists from safety_filter
# =======================================================================
from tgbotmodules.safety_filter import SSX_BLOCKED_TAGS_DEFAULT

# Create set for fast O(1) lookups
BLACKLIST_ILLEGAL: set[str] = {
    tag.lower() for tag in SSX_BLOCKED_TAGS_DEFAULT
}

# =======================================================================
# HARD DELETE: Community Standard (e621 specific)
# =======================================================================
# Real-person / photo content — funds exploitation
# These are NOT in safety_filter.py (which is for EHentai galleries)
# e621 has different tag taxonomy for real-world content
BLACKLIST_COMMUNITY: set[str] = {
    "real_person", "photo", "cosplay_nude", "onlyfans", "cam_girl",
    "photograph", "irl",
}

# =======================================================================
# AI ART RULES (e621 specific)
# =======================================================================
# ai_generated alone       -> DELETE (no human edit, lazy AI dump)
# ai_generated + ai_assisted -> KEEP (human collaborated)
# ai_assisted alone       -> KEEP (explicitly allowed by ssX rules)
TAG_AI_GENERATED = "ai_generated"
TAG_AI_ASSISTED = "ai_assisted"
