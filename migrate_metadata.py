#!/usr/bin/env python3
"""
Migration Script: Isolate system metadata from user_data namespace

This script migrates existing userdata files that have system metadata
fields (timestamp, version, ghost_drive_init, init, etc.) at the top
level to the new _metadata sub-dict format.

Usage:
    python migrate_metadata.py                    # Migrate default location
    python migrate_metadata.py --path ./data/    # Migrate specific path
    python migrate_metadata.py --dry-run         # Preview changes without applying
"""

import argparse
import json
import os
import sys

# Add the project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from tgbotmodules import userdatastore


def migrate_file(filepath: str, dry_run: bool = False) -> bool:
    """
    Migrate a single userdata file to the new metadata format.
    
    Args:
        filepath: Path to the userdata JSON file
        dry_run: If True, only preview changes without applying
        
    Returns:
        True if migration was needed/applied, False otherwise
    """
    if not os.path.exists(filepath):
        print(f"  ❌ File not found: {filepath}")
        return False
    
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        print(f"  ❌ Invalid JSON: {e}")
        return False
    
    # Check for legacy garbage keys
    legacy_keys = {'ssx_active', 'timestamp', 'version', 'ghost_drive_init', 'init',
                   'last_sync_timestamp', 'sync_version'}
    
    keys_to_migrate = [key for key in legacy_keys if key in data]
    
    if not keys_to_migrate:
        print(f"  ✓ No legacy metadata found, migration not needed")
        return False
    
    print(f"  Found {len(keys_to_migrate)} legacy keys to migrate: {keys_to_migrate}")
    
    if dry_run:
        print(f"  [DRY RUN] Would migrate these keys to _metadata:")
        for key in keys_to_migrate:
            value = data[key]
            if isinstance(value, str) and len(value) > 50:
                value = value[:50] + "..."
            elif isinstance(value, float):
                from datetime import datetime
                value = f"{datetime.fromtimestamp(value)}"
            print(f"    {key}: {value}")
        return True
    
    # Perform migration
    result = userdatastore.migrate_legacy_userdata(filepath)
    
    if result:
        print(f"  ✓ Successfully migrated {len(keys_to_migrate)} keys to _metadata")
        return True
    else:
        print(f"  ❌ Migration failed")
        return False


def main():
    parser = argparse.ArgumentParser(
        description='Migrate userdata files to isolate metadata in _metadata sub-dict'
    )
    parser.add_argument(
        '--path', '-p',
        default='./userdata/userdata',
        help='Path to userdata file (default: ./userdata/userdata)'
    )
    parser.add_argument(
        '--dry-run', '-d',
        action='store_true',
        help='Preview changes without applying them'
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Show verbose output'
    )
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("Metadata Isolation Migration Script")
    print("=" * 60)
    
    if args.dry_run:
        print("\n⚠️  DRY RUN MODE - No changes will be applied\n")
    
    filepath = os.path.abspath(args.path)
    print(f"\nProcessing: {filepath}\n")
    
    # Check if it's a directory
    if os.path.isdir(filepath):
        print("Directory mode - checking all JSON files in directory")
        migrated_count = 0
        for filename in os.listdir(filepath):
            if filename.endswith('.json'):
                full_path = os.path.join(filepath, filename)
                if migrate_file(full_path, args.dry_run):
                    migrated_count += 1
        print(f"\n{'Would migrate' if args.dry_run else 'Migrated'} {migrated_count} file(s)")
    else:
        migrate_file(filepath, args.dry_run)
    
    print("\n" + "=" * 60)
    print("Migration complete!")
    print("=" * 60)
    
    if args.dry_run:
        print("\nTo apply these changes, run without --dry-run:")
        print(f"  python {sys.argv[0]} --path {args.path}")


if __name__ == '__main__':
    main()
