"""
YouTube Playlist Extractor for Dallas Willard Knowledge Base
Extracts all playlists from a YouTube channel using the YouTube Data API v3
"""

import os
import sys
import json
from typing import List, Dict, Optional
import requests
from urllib.parse import urlparse, parse_qs

class YouTubePlaylistExtractor:
    def __init__(self, api_key: str):
        """
        Initialize with YouTube Data API key.
        Get your free API key at: https://console.developers.google.com/
        """
        self.api_key = api_key
        self.base_url = "https://www.googleapis.com/youtube/v3"
    
    def get_channel_id_from_username(self, username: str) -> Optional[str]:
        """
        Get channel ID from username/handle (e.g., @dallaswillard)
        """
        # Remove @ if present
        username = username.lstrip('@')
        
        url = f"{self.base_url}/channels"
        params = {
            'part': 'id,snippet',
            'forHandle': username,
            'key': self.api_key
        }
        
        response = requests.get(url, params=params)
        
        if response.status_code == 200:
            data = response.json()
            if 'items' in data and len(data['items']) > 0:
                return data['items'][0]['id']
        
        # Fallback: try with forUsername
        params['forUsername'] = username
        del params['forHandle']
        
        response = requests.get(url, params=params)
        
        if response.status_code == 200:
            data = response.json()
            if 'items' in data and len(data['items']) > 0:
                return data['items'][0]['id']
        
        print(f"Could not find channel ID for username: {username}")
        return None
    
    def get_channel_playlists(self, channel_id: str) -> List[Dict]:
        """
        Get all playlists from a channel.
        """
        playlists = []
        next_page_token = None
        
        while True:
            url = f"{self.base_url}/playlists"
            params = {
                'part': 'id,snippet,status',
                'channelId': channel_id,
                'maxResults': 50,
                'key': self.api_key
            }
            
            if next_page_token:
                params['pageToken'] = next_page_token
            
            response = requests.get(url, params=params)
            
            if response.status_code != 200:
                print(f"Error fetching playlists: {response.status_code}")
                print(response.text)
                break
            
            data = response.json()
            
            for item in data.get('items', []):
                playlist_info = {
                    'playlist_id': item['id'],
                    'title': item['snippet']['title'],
                    'description': item['snippet'].get('description', ''),
                    'url': f"https://www.youtube.com/playlist?list={item['id']}",
                    'published_at': item['snippet']['publishedAt'],
                    'video_count': item.get('contentDetails', {}).get('itemCount', 'Unknown'),
                    'privacy_status': item.get('status', {}).get('privacyStatus', 'Unknown')
                }
                playlists.append(playlist_info)
            
            next_page_token = data.get('nextPageToken')
            if not next_page_token:
                break
        
        return playlists
    
    def get_channel_info(self, channel_id: str) -> Dict:
        """
        Get basic channel information.
        """
        url = f"{self.base_url}/channels"
        params = {
            'part': 'snippet,statistics',
            'id': channel_id,
            'key': self.api_key
        }
        
        response = requests.get(url, params=params)
        
        if response.status_code == 200:
            data = response.json()
            if 'items' in data and len(data['items']) > 0:
                item = data['items'][0]
                return {
                    'channel_id': channel_id,
                    'title': item['snippet']['title'],
                    'description': item['snippet']['description'],
                    'subscriber_count': item['statistics'].get('subscriberCount', 'Hidden'),
                    'video_count': item['statistics'].get('videoCount', '0'),
                    'view_count': item['statistics'].get('viewCount', '0'),
                    'published_at': item['snippet']['publishedAt']
                }
        
        return {'channel_id': channel_id, 'title': 'Unknown Channel'}
    
    def extract_all_playlists(self, channel_identifier: str, output_file: str = 'dallas_willard_playlists.json') -> Dict:
        """
        Main method to extract all playlists from a channel.
        channel_identifier can be: channel_id, username, or @handle
        """
        # Determine if it's a channel ID or username
        if channel_identifier.startswith('@') or not channel_identifier.startswith('UC'):
            print(f"Getting channel ID for: {channel_identifier}")
            channel_id = self.get_channel_id_from_username(channel_identifier)
            if not channel_id:
                return {}
        else:
            channel_id = channel_identifier
        
        print(f"Using channel ID: {channel_id}")
        
        # Get channel info
        channel_info = self.get_channel_info(channel_id)
        print(f"Channel: {channel_info['title']}")
        
        # Get playlists
        print("Fetching playlists...")
        playlists = self.get_channel_playlists(channel_id)
        
        result = {
            'channel_info': channel_info,
            'playlists': playlists,
            'total_playlists': len(playlists),
            'extraction_date': '2025-01-23'  # Current date
        }
        
        # Save to file
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        
        print(f"\\nExtracted {len(playlists)} playlists")
        print(f"Results saved to: {output_file}")
        
        # Print playlist URLs for easy copying
        print("\\n=== PLAYLIST URLS ===")
        for playlist in playlists:
            print(f"{playlist['title']}: {playlist['url']}")
        
        return result

def main():
    """
    Usage: python playlist_extractor.py <API_KEY> [channel_identifier]
    
    Get your free YouTube Data API key at:
    https://console.developers.google.com/
    
    Examples:
    python playlist_extractor.py YOUR_API_KEY @dallaswillard
    python playlist_extractor.py YOUR_API_KEY dallaswillard  
    python playlist_extractor.py YOUR_API_KEY UC1234567890abcdef
    """
    
    if len(sys.argv) < 2:
        print("Usage: python playlist_extractor.py <API_KEY> [channel_identifier]")
        print("\\nGet your free YouTube Data API key at:")
        print("https://console.developers.google.com/")
        print("\\nChannel identifier can be:")
        print("  @dallaswillard")
        print("  dallaswillard")
        print("  UC1234567890abcdef (channel ID)")
        return
    
    api_key = sys.argv[1]
    channel_identifier = sys.argv[2] if len(sys.argv) > 2 else "@dallaswillard"
    
    extractor = YouTubePlaylistExtractor(api_key)
    result = extractor.extract_all_playlists(channel_identifier)
    
    if result:
        print(f"\\nSuccess! Found {result['total_playlists']} playlists")
        print("\\nNext steps:")
        print("1. Review the playlist URLs above")
        print("2. Run the enhanced transcript fetcher with these playlists")

if __name__ == "__main__":
    main()