#!/usr/bin/env python3
"""
Simple test script for playlist scraping without proxies
"""

import requests
from bs4 import BeautifulSoup
import json
import re

def scrape_playlist_simple(playlist_url):
    """Simple playlist scraper for testing"""
    try:
        print(f"🔍 Testing playlist scraping: {playlist_url}")
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        
        response = requests.get(playlist_url, headers=headers, timeout=30)
        response.raise_for_status()
        
        print(f"✅ Got response, status: {response.status_code}")
        print(f"📄 Content length: {len(response.text)} characters")
        
        # Look for video data in the response
        if 'var ytInitialData' in response.text:
            print("✅ Found ytInitialData in response")
            
            # Extract the ytInitialData JSON
            try:
                start_marker = 'var ytInitialData = '
                start_idx = response.text.find(start_marker)
                if start_idx != -1:
                    start_idx += len(start_marker)
                    end_idx = response.text.find(';</script>', start_idx)
                    if end_idx == -1:
                        end_idx = response.text.find('}};', start_idx) + 2
                    
                    json_str = response.text[start_idx:end_idx]
                    data = json.loads(json_str)
                    
                    print("✅ Successfully parsed ytInitialData JSON")
                    
                    # Find video data - let's look for any videoRenderer
                    def find_videos_recursive(obj, videos_found):
                        if isinstance(obj, dict):
                            # Look for playlist video renderer
                            if 'playlistVideoRenderer' in obj:
                                video_data = obj['playlistVideoRenderer']
                                video_id = video_data.get('videoId')
                                title_runs = video_data.get('title', {}).get('runs', [])
                                title = title_runs[0].get('text', 'Unknown') if title_runs else 'Unknown'
                                
                                if video_id:
                                    videos_found.append({'id': video_id, 'title': title})
                                    
                            # Recursively search all dict values
                            for value in obj.values():
                                find_videos_recursive(value, videos_found)
                                
                        elif isinstance(obj, list):
                            # Recursively search all list items
                            for item in obj:
                                find_videos_recursive(item, videos_found)
                    
                    videos_found = []
                    find_videos_recursive(data, videos_found)
                    
                    print(f"🎯 Found {len(videos_found)} videos via JSON parsing")
                    for i, video in enumerate(videos_found[:3]):
                        print(f"   📹 {video['id']} - {video['title'][:50]}...")
                    
                    if len(videos_found) > 3:
                        print(f"   ... and {len(videos_found) - 3} more")
                    
                    return len(videos_found) > 0
                    
            except Exception as e:
                print(f"⚠️ Failed to parse JSON: {e}")
        
        else:
            print("❌ No ytInitialData found")
        
        # Parse with BeautifulSoup as fallback
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Look for video links as fallback
        video_links = soup.find_all('a', href=True)
        video_count = 0
        
        for link in video_links:
            href = link.get('href', '')
            if '/watch?v=' in href and '&list=' in href:
                video_count += 1
                if video_count <= 3:  # Show first 3
                    video_id_match = re.search(r'v=([a-zA-Z0-9_-]{11})', href)
                    if video_id_match:
                        video_id = video_id_match.group(1)
                        title = link.get('title', link.text.strip())
                        print(f"   📹 Found video: {video_id} - {title[:50]}...")
        
        print(f"🎯 Total video links found: {video_count}")
        return video_count > 0
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    # Test with one of the playlists
    test_url = "https://www.youtube.com/playlist?list=PLQ7tmLfYg7S9_mm8Aeq_r4bKRpLH8Jn_3"
    scrape_playlist_simple(test_url)