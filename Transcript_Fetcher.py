import sys
from youtube_transcript_api import YouTubeTranscriptApi
from typing import Optional, List, Dict, Generator
import re
import json
from datetime import datetime
import os
import requests
import time
from urllib.parse import parse_qs, urlparse

class YouTubeTranscriptFetcher:
    def __init__(self, rate_limit_per_minute: int = 60):
        self.rate_limit = rate_limit_per_minute
        self.last_request_time = 0
        self.speaker_patterns = [
            r'^\[([^\]]+)\]:(.+)$',  # [Speaker]: Text
            r'^([^:]+):(.+)$',        # Speaker: Text
            r'^\(([^\)]+)\):(.+)$',   # (Speaker): Text
            r'<([^>]+)>:(.+)$'        # <Speaker>: Text
        ]

    def _sanitize_filename(self, name: str) -> str:
        """Convert a string into a valid filename/directory name."""
        # Remove invalid chars
        name = re.sub(r'[<>:"/\\|?*]', '', name)
        # Replace spaces and multiple dashes with single dash
        name = re.sub(r'[-\s]+', '-', name.strip())
        return name

    def _extract_video_id(self, url: str) -> Optional[str]:
        """Extract video ID from various forms of YouTube URLs."""
        patterns = [
            r'(?:v=|\/)([0-9A-Za-z_-]{11}).*',
            r'^([0-9A-Za-z_-]{11})$'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                return match.group(1)
        return None

    def _extract_playlist_id(self, url: str) -> Optional[str]:
        """Extract playlist ID from YouTube URL."""
        # Handle different playlist URL formats
        parsed_url = urlparse(url)
        
        # Format: https://www.youtube.com/playlist?list=PLAYLIST_ID
        if 'list' in parse_qs(parsed_url.query):
            return parse_qs(parsed_url.query)['list'][0]
            
        return None

    def _rate_limit_wait(self):
        """Implement rate limiting to avoid overloading the API."""
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time
        wait_time = (60 / self.rate_limit) - time_since_last_request
        
        if wait_time > 0:
            time.sleep(wait_time)
        
        self.last_request_time = time.time()

    def _format_timestamp(self, seconds: float) -> str:
        """Convert seconds to HH:MM:SS format."""
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        seconds = int(seconds % 60)
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

    def _extract_speaker_and_text(self, text: str) -> tuple:
        """
        Try to extract speaker and text from a transcript line.
        Returns tuple of (speaker, text). If no speaker found, returns (None, original_text)
        """
        for pattern in self.speaker_patterns:
            match = re.match(pattern, text.strip())
            if match:
                speaker, content = match.groups()
                return speaker.strip(), content.strip()
        
        return None, text.strip()

    def _check_for_speakers(self, transcript: List[Dict]) -> bool:
        """Check if the transcript contains any speaker labels."""
        for entry in transcript:
            speaker, _ = self._extract_speaker_and_text(entry['text'])
            if speaker:
                return True
        return False

    def _get_channel_info(self, video_id: str) -> Dict:
        """Get channel information from YouTube oEmbed API."""
        try:
            oembed_url = f"https://www.youtube.com/oembed?url=https://www.youtube.com/watch?v={video_id}&format=json"
            response = requests.get(oembed_url)
            if response.status_code == 200:
                data = response.json()
                # Extract channel URL from author_url if available
                channel_url = data.get('author_url', '')
                channel_id = channel_url.split('/')[-1] if channel_url else None
                
                return {
                    'channel_name': data.get('author_name', 'Unknown Channel'),
                    'channel_id': channel_id,
                    'channel_url': channel_url
                }
        except Exception as e:
            print(f"Error fetching channel info: {e}")
        
        return {
            'channel_name': 'Unknown Channel',
            'channel_id': None,
            'channel_url': None
        }

    def _get_playlist_info(self, playlist_id: str) -> Dict:
        """Get playlist metadata using YouTube's oEmbed API."""
        try:
            # Get first video from playlist to get channel info
            playlist_url = f"https://www.youtube.com/playlist?list={playlist_id}"
            response = requests.get(playlist_url)
            
            if response.status_code == 200:
                # Extract playlist title from HTML (basic extraction)
                title_match = re.search(r'<title>([^<]+)</title>', response.text)
                playlist_title = title_match.group(1).replace('- YouTube', '').strip() if title_match else 'Unknown Playlist'
                
                return {
                    'playlist_id': playlist_id,
                    'title': playlist_title,
                    'url': playlist_url
                }
        except Exception as e:
            print(f"Error fetching playlist info: {e}")
        
        return {
            'playlist_id': playlist_id,
            'title': 'Unknown Playlist',
            'url': f"https://www.youtube.com/playlist?list={playlist_id}"
        }

    def _get_playlist_videos(self, playlist_id: str) -> Generator[str, None, None]:
        """
        Get video IDs from a playlist using requests and basic HTML parsing.
        This is a simple implementation - for production, consider using youtube-dl or an official API.
        """
        try:
            playlist_url = f"https://www.youtube.com/playlist?list={playlist_id}"
            response = requests.get(playlist_url)
            
            if response.status_code == 200:
                # Basic regex to find video IDs
                video_ids = re.findall(r'watch\?v=([a-zA-Z0-9_-]{11})', response.text)
                seen = set()
                
                for video_id in video_ids:
                    if video_id not in seen:
                        seen.add(video_id)
                        yield video_id
                        
        except Exception as e:
            print(f"Error fetching playlist videos: {e}")

    def _get_video_metadata(self, video_id: str, transcript: List[Dict]) -> Dict:
        """
        Get basic video metadata using oEmbed and check for speaker labels.
        """
        try:
            oembed_url = f"https://www.youtube.com/oembed?url=https://www.youtube.com/watch?v={video_id}&format=json"
            response = requests.get(oembed_url)
            
            if response.status_code == 200:
                data = response.json()
                channel_info = self._get_channel_info(video_id)
                
                return {
                    'title': data.get('title', 'Unknown Title'),
                    'video_id': video_id,
                    'url': f'https://www.youtube.com/watch?v={video_id}',
                    'has_speaker_labels': self._check_for_speakers(transcript),
                    'channel': channel_info
                }
        except Exception as e:
            print(f"Error fetching metadata: {e}")
        
        return {
            'title': 'Unknown Title',
            'video_id': video_id,
            'url': f'https://www.youtube.com/watch?v={video_id}',
            'has_speaker_labels': self._check_for_speakers(transcript),
            'channel': self._get_channel_info(video_id)
        }

    def get_transcript(self, video_url: str) -> Optional[List[Dict]]:
        """
        Fetch transcript for a given YouTube video URL.
        Returns None if transcript is unavailable.
        """
        try:
            video_id = self._extract_video_id(video_url)
            if not video_id:
                print(f"Error: Could not extract video ID from URL: {video_url}")
                return None

            self._rate_limit_wait()
            transcript = YouTubeTranscriptApi.get_transcript(video_id)
            return transcript

        except Exception as e:
            print(f"Error fetching transcript for {video_url}: {str(e)}")
            return None

    def save_transcript_with_timestamps(self, video_url: str, base_dir: str = "transcripts") -> Optional[str]:
        """
        Save transcript with timestamps and metadata to a file within channel-specific directory.
        Attempts to detect and preserve speaker labels if present.
        Returns the path to the saved file if successful, None otherwise.
        """
        video_id = self._extract_video_id(video_url)
        if not video_id:
            return None

        transcript = self.get_transcript(video_url)
        if not transcript:
            return None

        metadata = self._get_video_metadata(video_id, transcript)
        
        # Create base directory if it doesn't exist
        os.makedirs(base_dir, exist_ok=True)
        
        # Create channel-specific directory
        channel_name = self._sanitize_filename(metadata['channel']['channel_name'])
        channel_dir = os.path.join(base_dir, channel_name)
        os.makedirs(channel_dir, exist_ok=True)
        
        # Create filename from video title (sanitized)
        safe_title = self._sanitize_filename(metadata['title'])
        filename = f"{safe_title}_{video_id}.txt"
        filepath = os.path.join(channel_dir, filename)

        with open(filepath, 'w', encoding='utf-8') as f:
            # Write metadata header
            f.write(f"Title: {metadata['title']}\n")
            f.write(f"Video URL: {metadata['url']}\n")
            f.write(f"Channel Name: {metadata['channel']['channel_name']}\n")
            f.write(f"Channel URL: {metadata['channel']['channel_url']}\n")
            f.write(f"Channel ID: {metadata['channel']['channel_id']}\n")
            f.write(f"Has Speaker Labels: {metadata['has_speaker_labels']}\n")
            f.write(f"Downloaded: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("\n" + "="*50 + "\n\n")

            # Process transcript entries
            for entry in transcript:
                timestamp = self._format_timestamp(entry['start'])
                speaker, text = self._extract_speaker_and_text(entry['text'])
                
                if speaker:
                    f.write(f"[{timestamp}] {speaker}: {text}\n")
                else:
                    f.write(f"[{timestamp}] {text}\n")

        print(f"Transcript saved to: {filepath}")
        
        # Also save metadata to a JSON file in the same channel directory
        metadata_file = os.path.join(channel_dir, f"{safe_title}_{video_id}_metadata.json")
        with open(metadata_file, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2)
        print(f"Metadata saved to: {metadata_file}")
        
        return filepath

    def save_playlist_transcripts(self, playlist_url: str, base_dir: str = "transcripts") -> List[str]:
        """
        Save transcripts for all videos in a playlist.
        Returns list of saved file paths.
        """
        playlist_id = self._extract_playlist_id(playlist_url)
        if not playlist_id:
            print("Error: Could not extract playlist ID from URL")
            return []

        playlist_info = self._get_playlist_info(playlist_id)
        saved_files = []
        
        # Get first video to determine channel info
        first_video_id = next(self._get_playlist_videos(playlist_id), None)
        if not first_video_id:
            print("Error: Could not find any videos in playlist")
            return []

        # Get channel info from first video
        channel_info = self._get_channel_info(first_video_id)
        
        # Create directory structure
        channel_name = self._sanitize_filename(channel_info['channel_name'])
        playlist_name = self._sanitize_filename(playlist_info['title'])
        playlist_dir = os.path.join(base_dir, channel_name, playlist_name)
        os.makedirs(playlist_dir, exist_ok=True)

        # Save playlist metadata
        playlist_metadata = {
            'playlist_id': playlist_id,
            'title': playlist_info['title'],
            'url': playlist_info['url'],
            'channel': channel_info
        }
        
        playlist_metadata_file = os.path.join(playlist_dir, 'playlist_metadata.json')
        with open(playlist_metadata_file, 'w', encoding='utf-8') as f:
            json.dump(playlist_metadata, f, indent=2)

        # Process each video in the playlist
        for video_id in self._get_playlist_videos(playlist_id):
            video_url = f"https://www.youtube.com/watch?v={video_id}"
            try:
                filepath = self.save_transcript_with_timestamps(
                    video_url, 
                    base_dir=playlist_dir  # Save in playlist directory
                )
                if filepath:
                    saved_files.append(filepath)
                    print(f"Processed video: {video_id}")
                else:
                    print(f"Failed to process video: {video_id}")
                
                # Respect rate limit
                self._rate_limit_wait()
                
            except Exception as e:
                print(f"Error processing video {video_id}: {e}")
                continue

        return saved_files

def main():
    if len(sys.argv) != 2:
        print("Usage: python script.py <youtube_url>")
        print("Supported URLs:")
        print("  - Single video: https://www.youtube.com/watch?v=VIDEO_ID")
        print("  - Playlist: https://www.youtube.com/playlist?list=PLAYLIST_ID")
        return

    url = sys.argv[1]
    fetcher = YouTubeTranscriptFetcher(rate_limit_per_minute=30)

    if 'playlist' in url:
        saved_files = fetcher.save_playlist_transcripts(url)
        if saved_files:
            print(f"\nSuccessfully saved {len(saved_files)} transcripts!")
        else:
            print("\nNo transcripts were saved.")
    else:
        filepath = fetcher.save_transcript_with_timestamps(url)
        if filepath:
            print("Transcript saved successfully!")
        else:
            print("Failed to save transcript.")

if __name__ == "__main__":
    main()