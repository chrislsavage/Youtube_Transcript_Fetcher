"""
Direct Retry Script for Missing Dallas Willard Transcripts
Scans all playlists and retries videos that don't have transcript files
"""

import os
import json
import glob
from enhanced_transcript_processor import DallasWillardTranscriptProcessor

def load_playlists():
    """Load playlist data."""
    with open('dallas_willard_playlists.json', 'r', encoding='utf-8') as f:
        return json.load(f)

def get_existing_video_ids():
    """Get list of video IDs that already have transcripts."""
    transcript_files = glob.glob('dallas_willard_transcripts/**/*.txt', recursive=True)
    existing_ids = set()
    
    for file_path in transcript_files:
        filename = os.path.basename(file_path)
        # Extract video ID from filename (format: Title_VideoID.txt)
        if '_' in filename:
            video_id = filename.split('_')[-1].replace('.txt', '')
            existing_ids.add(video_id)
    
    return existing_ids

def find_missing_videos():
    """Find videos that don't have transcript files."""
    playlists_data = load_playlists()
    existing_ids = get_existing_video_ids()
    missing_videos = []
    
    print(f"📋 Found {len(existing_ids)} existing transcripts")
    
    for playlist in playlists_data.get('playlists', []):
        playlist_title = playlist.get('title', 'Unknown')
        videos = playlist.get('videos', [])
        
        for video in videos:
            video_id = video.get('video_id', '')
            if video_id and video_id not in existing_ids:
                missing_videos.append({
                    'video_id': video_id,
                    'title': video.get('title', 'Unknown'),
                    'playlist': playlist_title,
                    'url': f"https://www.youtube.com/watch?v={video_id}"
                })
    
    print(f"❌ Found {len(missing_videos)} missing transcripts")
    return missing_videos

def retry_missing_transcripts(max_retries: int = None):
    """Retry processing missing video transcripts."""
    missing_videos = find_missing_videos()
    
    if not missing_videos:
        print("✅ All videos already have transcripts!")
        return
    
    if max_retries:
        missing_videos = missing_videos[:max_retries]
        print(f"🎯 Processing first {max_retries} missing videos")
    
    # Ultra-conservative processor for new IP
    processor = DallasWillardTranscriptProcessor(rate_limit_per_minute=5)
    
    print(f"🚀 Starting retry with enhanced rate limiting (5 requests/minute)")
    print("📝 Features: Exponential backoff + Session caching + Random jitter\n")
    
    success_count = 0
    fail_count = 0
    
    for i, video_info in enumerate(missing_videos, 1):
        video_id = video_info['video_id']
        video_url = video_info['url']
        
        print(f"[{i}/{len(missing_videos)}] Retrying: {video_id}")
        print(f"📂 Playlist: {video_info['playlist']}")
        print(f"🎥 Title: {video_info['title'][:60]}...")
        
        try:
            result = processor.process_single_video(video_url)
            
            if result and result.get('success'):
                print(f"✅ SUCCESS: Transcript saved!")
                success_count += 1
            else:
                print(f"❌ FAILED: No transcript available")
                fail_count += 1
                
        except Exception as e:
            print(f"💥 ERROR: {str(e)}")
            fail_count += 1
        
        print(f"📊 Progress: {success_count} success, {fail_count} failed\n")
        
        # Save progress every 10 videos
        if i % 10 == 0:
            print(f"💾 Checkpoint: {success_count}/{i} successful so far")
    
    print(f"\n🎉 RETRY COMPLETE!")
    print(f"✅ Successful: {success_count}")
    print(f"❌ Failed: {fail_count}")
    print(f"📊 Success rate: {(success_count / len(missing_videos) * 100):.1f}%")

def main():
    import sys
    
    if len(sys.argv) > 1:
        try:
            max_retries = int(sys.argv[1])
        except ValueError:
            max_retries = None
    else:
        max_retries = None
    
    retry_missing_transcripts(max_retries)

if __name__ == "__main__":
    main()