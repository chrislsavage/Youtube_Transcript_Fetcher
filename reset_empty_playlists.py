#!/usr/bin/env python3
"""
Reset the playlists that showed "No videos found" so they can be re-processed
with the new playlist scraping logic
"""

import json

# Playlists that showed "No videos found" in the logs
empty_playlists = [
    "Renovaré Institute Series",
    "For Such a Time As This", 
    "Hearing God in Real Life",
    '"Hey Dallas!" Answers to pertinent questions.',
    "Solo Sermons",
    "Authentic Leader Series",
    "Kingdom Living", 
    "The Divine Conspiracy",
    "Denver Seminary Spiritual Formation 611 - 17 Parts",
    "Short Clips",
    "Philosophy and Apologetics",
    "Good Friday",
    "The Disappearance of Moral Knowledge",
    "Healing the Heart and Life by Walking with Jesus Daily"
]

def reset_empty_playlists():
    # Load current progress
    with open('processing_progress.json', 'r', encoding='utf-8') as f:
        progress = json.load(f)
    
    print(f"📋 Current completed playlists: {len(progress['completed_playlists'])}")
    
    # Remove empty playlists from completed list
    original_completed = progress['completed_playlists'].copy()
    progress['completed_playlists'] = [
        playlist for playlist in progress['completed_playlists'] 
        if playlist not in empty_playlists
    ]
    
    removed_count = len(original_completed) - len(progress['completed_playlists'])
    print(f"🔄 Removed {removed_count} empty playlists from completed list")
    print(f"📋 New completed playlists: {len(progress['completed_playlists'])}")
    
    # Reset stats for the removed playlists
    progress['stats']['total_processed'] = 0  # Will be recalculated
    progress['stats']['total_failed'] = 0
    
    # Save updated progress
    with open('processing_progress.json', 'w', encoding='utf-8') as f:
        json.dump(progress, f, indent=2)
    
    print(f"✅ Progress file updated. Ready to re-process empty playlists with new scraping logic.")
    
    # Show which playlists will be re-processed
    print(f"\n🔄 Playlists to be re-processed:")
    for playlist in empty_playlists:
        print(f"   - {playlist}")

if __name__ == "__main__":
    reset_empty_playlists()