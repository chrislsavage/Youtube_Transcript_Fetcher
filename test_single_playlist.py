#!/usr/bin/env python3
"""
Test processing a single playlist to verify it works
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from proxy_enhanced_processor import ProxyEnhancedTranscriptProcessor

def test_single_playlist():
    # Use direct connection
    processor = ProxyEnhancedTranscriptProcessor(
        rate_limit_per_minute=10,
        use_proxies=False
    )
    
    # Test with Renovaré Institute Series which we know has 21 videos
    test_url = "https://www.youtube.com/playlist?list=PLQ7tmLfYg7S9_mm8Aeq_r4bKRpLH8Jn_3"
    playlist_title = "Renovaré Institute Series"
    
    print(f"🧪 Testing single playlist: {playlist_title}")
    print(f"📍 URL: {test_url}")
    
    # Step 1: Test playlist scraping
    print(f"\n1️⃣ Testing playlist scraping...")
    videos = processor.scrape_playlist_videos(test_url)
    
    if not videos:
        print(f"❌ No videos found in playlist scraping")
        return False
    
    print(f"✅ Found {len(videos)} videos")
    
    # Step 2: Test processing one video
    if len(videos) > 0:
        print(f"\n2️⃣ Testing transcript processing for first video...")
        first_video = videos[0]
        video_id = first_video['video_id']
        video_title = first_video['title']
        video_url = f"https://www.youtube.com/watch?v={video_id}"
        
        print(f"📹 Processing: {video_title[:50]}...")
        print(f"🔗 URL: {video_url}")
        
        try:
            result = processor.process_single_video(video_url, playlist_title)
            
            if result and result.get('success'):
                print(f"✅ Successfully processed video!")
                print(f"💾 Saved to: {result.get('transcript_file', 'unknown')}")
                print(f"🏷️ Subjects: {result.get('subjects', [])}")
                return True
            else:
                print(f"❌ Failed to process video: {result.get('error', 'unknown error')}")
                return False
                
        except Exception as e:
            print(f"💥 Error processing video: {str(e)}")
            return False
    
    return False

if __name__ == "__main__":
    success = test_single_playlist()
    if success:
        print(f"\n🎉 Single playlist test PASSED!")
    else:
        print(f"\n❌ Single playlist test FAILED!")