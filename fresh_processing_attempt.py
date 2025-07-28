"""
Fresh Processing Attempt with New IP
Resets progress tracking and attempts to process all videos again with enhanced rate limiting
"""

import os
import json
import shutil
from datetime import datetime
from batch_processor import BatchTranscriptProcessor

def backup_current_progress():
    """Backup current progress files."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_dir = f"backup_{timestamp}"
    os.makedirs(backup_dir, exist_ok=True)
    
    files_to_backup = [
        'processing_progress.json',
        'knowledge_base_index.json'
    ]
    
    for file in files_to_backup:
        if os.path.exists(file):
            shutil.copy2(file, f"{backup_dir}/{file}")
            print(f"✅ Backed up {file}")
    
    return backup_dir

def reset_progress_for_retry():
    """Reset progress tracking to allow reprocessing."""
    if os.path.exists('processing_progress.json'):
        os.remove('processing_progress.json')
        print("🔄 Reset processing progress")
    
    # Keep existing transcripts but allow reprocessing of playlists
    print("📁 Keeping existing transcripts, but will attempt all playlists again")

def start_fresh_processing():
    """Start fresh processing with enhanced rate limiting."""
    print("🚀 Starting fresh processing attempt with new IP and enhanced rate limiting")
    print("📝 Settings: 3 videos per batch, 8 requests/minute, exponential backoff")
    
    # Use ultra-conservative settings for new IP
    processor = BatchTranscriptProcessor(batch_size=2, rate_limit=6)
    
    # Load playlists data
    if not os.path.exists('dallas_willard_playlists.json'):
        print("❌ No playlist data found!")
        return
    
    with open('dallas_willard_playlists.json', 'r', encoding='utf-8') as f:
        playlists_data = json.load(f)
    
    playlists = playlists_data.get('playlists', [])
    print(f"📋 Found {len(playlists)} playlists to process")
    
    # Start processing
    try:
        if len(playlists) > 0:
            print(f"🎯 Starting with first playlist to test new IP...")
            processor.process_playlists_batch(playlists[:1])  # Test with just one playlist first
        else:
            print("❌ No playlists found in data file")
    except Exception as e:
        print(f"💥 Error during processing: {e}")

def main():
    print("🔄 FRESH PROCESSING ATTEMPT WITH NEW IP")
    print("=" * 50)
    
    # Backup current progress
    backup_dir = backup_current_progress()
    print(f"📦 Progress backed up to: {backup_dir}")
    
    # Reset progress tracking
    reset_progress_for_retry()
    
    # Start fresh processing
    start_fresh_processing()

if __name__ == "__main__":
    main()