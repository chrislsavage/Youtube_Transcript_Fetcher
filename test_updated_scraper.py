#!/usr/bin/env python3
"""
Test the updated scraping logic from the enhanced processor
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from proxy_enhanced_processor import ProxyEnhancedTranscriptProcessor

def test_playlist_scraping():
    # Test without proxies first
    processor = ProxyEnhancedTranscriptProcessor(
        rate_limit_per_minute=10,
        use_proxies=False
    )
    
    test_url = "https://www.youtube.com/playlist?list=PLQ7tmLfYg7S9_mm8Aeq_r4bKRpLH8Jn_3"
    print(f"Testing playlist scraping: {test_url}")
    
    videos = processor.scrape_playlist_videos(test_url)
    print(f"Result: Found {len(videos)} videos")
    
    if videos:
        print("✅ Playlist scraping works!")
        return True
    else:
        print("❌ No videos found")
        return False

if __name__ == "__main__":
    test_playlist_scraping()