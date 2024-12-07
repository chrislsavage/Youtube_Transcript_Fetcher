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
    def update_root_transcripts_json(self, transcript_path: str, metadata: dict, base_dir: str = "transcripts") -> None:
        """
        Update the JSON file that tracks transcripts in the root directory.
        
        Args:
            transcript_path: Path to the transcript file
            metadata: Video and channel metadata
            base_dir: Base directory for transcripts
        """
        root_json_path = os.path.join(base_dir, "root_transcripts.json")
        
        try:
            # Load existing root JSON if it exists
            if os.path.exists(root_json_path):
                with open(root_json_path, 'r', encoding='utf-8') as f:
                    root_data = json.load(f)
            else:
                root_data = {
                    'last_updated': '',
                    'total_transcripts': 0,
                    'channels': {}
                }
            
            channel_name = metadata['channel']['channel_name']
            
            # Initialize channel if it doesn't exist
            if channel_name not in root_data['channels']:
                root_data['channels'][channel_name] = {
                    'channel_id': metadata['channel']['channel_id'],
                    'channel_url': metadata['channel']['channel_url'],
                    'videos': []
                }
            
            # Add video information
            video_info = {
                'title': metadata['title'],
                'video_id': metadata['video_id'],
                'url': metadata['url'],
                'transcript_path': os.path.relpath(transcript_path, base_dir),
                'date_added': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            # Check if video already exists (avoid duplicates)
            video_ids = [v['video_id'] for v in root_data['channels'][channel_name]['videos']]
            if video_info['video_id'] not in video_ids:
                root_data['channels'][channel_name]['videos'].append(video_info)
            
            # Update metadata
            root_data['last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            root_data['total_transcripts'] = sum(
                len(channel_data['videos'])
                for channel_data in root_data['channels'].values()
            )
            
            # Save updated root JSON
            with open(root_json_path, 'w', encoding='utf-8') as f:
                json.dump(root_data, f, indent=2)
                
        except Exception as e:
            print(f"Error updating root transcripts JSON: {e}")


    def update_master_json(self, transcript_path: str, metadata: dict, base_dir: str = "transcripts") -> None:
        """
        Update the master JSON file with information about a new transcript.
        """
        master_json_path = os.path.join(base_dir, "master_transcript_index.json")
        
        try:
            # Load existing master JSON if it exists
            if os.path.exists(master_json_path):
                with open(master_json_path, 'r', encoding='utf-8') as f:
                    master_data = json.load(f)
            else:
                master_data = {
                    'last_updated': '',
                    'total_transcripts': 0,
                    'channels': {}
                }
            
            channel_name = metadata['channel']['channel_name']
            
            # Get relative path from base_dir to transcript
            rel_path = os.path.relpath(transcript_path, base_dir)
            
            # Initialize channel if it doesn't exist
            if channel_name not in master_data['channels']:
                master_data['channels'][channel_name] = {
                    'channel_id': metadata['channel']['channel_id'],
                    'channel_url': metadata['channel']['channel_url'],
                    'playlists': {},
                    'videos': []
                }
            
            # Get playlist name from path if it exists
            path_parts = rel_path.split(os.sep)
            if len(path_parts) > 2:  # If in playlist subfolder
                playlist_name = path_parts[1]
                if playlist_name not in master_data['channels'][channel_name]['playlists']:
                    master_data['channels'][channel_name]['playlists'][playlist_name] = {
                        'videos': []
                    }
                
                # Add video to playlist
                video_info = {
                    'title': metadata['title'],
                    'video_id': metadata['video_id'],
                    'url': metadata['url'],
                    'transcript_path': rel_path,
                    'date_added': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                master_data['channels'][channel_name]['playlists'][playlist_name]['videos'].append(video_info)
            else:
                # Add video to channel's direct videos list
                video_info = {
                    'title': metadata['title'],
                    'video_id': metadata['video_id'],
                    'url': metadata['url'],
                    'transcript_path': rel_path,
                    'date_added': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                master_data['channels'][channel_name]['videos'].append(video_info)
            
            # Update master data metadata
            master_data['last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            master_data['total_transcripts'] = sum(
                len(channel_data['videos']) +
                sum(len(playlist['videos']) for playlist in channel_data['playlists'].values())
                for channel_data in master_data['channels'].values()
            )
            
            # Save updated master JSON
            with open(master_json_path, 'w', encoding='utf-8') as f:
                json.dump(master_data, f, indent=2)
            
        except Exception as e:
            print(f"Error updating master JSON: {e}")

    def update_root_folder_json(self, original_path: str, root_path: str, metadata: dict, channel_dir: str) -> None:
        """
        Update the JSON file tracking transcripts in the root account folder.
        """
        root_json_path = os.path.join(channel_dir, "root_transcripts.json")
        
        try:
            # Load existing JSON if it exists
            if os.path.exists(root_json_path):
                with open(root_json_path, 'r', encoding='utf-8') as f:
                    root_data = json.load(f)
            else:
                root_data = {
                    'last_updated': '',
                    'total_transcripts': 0,
                    'videos': []
                }
            
            # Add video information
            video_info = {
                'title': metadata['title'],
                'video_id': metadata['video_id'],
                'url': metadata['url'],
                'original_path': original_path,
                'root_path': root_path,
                'date_added': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            root_data['videos'].append(video_info)
            
            # Update metadata
            root_data['last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            root_data['total_transcripts'] = len(root_data['videos'])
            
            # Save updated JSON
            with open(root_json_path, 'w', encoding='utf-8') as f:
                json.dump(root_data, f, indent=2)
            
        except Exception as e:
            print(f"Error updating root folder JSON: {e}")

    def save_transcript_with_timestamps(self, video_url: str, base_dir: str = "transcripts") -> Optional[tuple]:
        """
        Save transcript with timestamps to both playlist location and root directory.
        Returns tuple of (filepath, metadata) if successful, None otherwise.
        """
        video_id = self._extract_video_id(video_url)
        if not video_id:
            return None

        transcript = self.get_transcript(video_url)
        if not transcript:
            return None

        metadata = self._get_video_metadata(video_id, transcript)
        
        # Create filename from video title (sanitized)
        safe_title = self._sanitize_filename(metadata['title'])
        filename = f"{safe_title}_{video_id}.txt"
        
        # Setup paths for both playlist and root locations
        playlist_filepath = os.path.join(base_dir, filename)
        root_dir = os.path.dirname(os.path.dirname(base_dir))  # Go up to main transcripts dir
        root_filepath = os.path.join(root_dir, filename)

        # Create directories if needed
        os.makedirs(base_dir, exist_ok=True)
        os.makedirs(root_dir, exist_ok=True)

        # Create transcript content
        transcript_content = f"Title: {metadata['title']}\n"
        transcript_content += f"Video URL: {metadata['url']}\n"
        transcript_content += f"Channel Name: {metadata['channel']['channel_name']}\n"
        transcript_content += f"Channel URL: {metadata['channel']['channel_url']}\n"
        transcript_content += f"Channel ID: {metadata['channel']['channel_id']}\n"
        transcript_content += f"Has Speaker Labels: {metadata['has_speaker_labels']}\n"
        transcript_content += f"Downloaded: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        transcript_content += "\n" + "="*50 + "\n\n"

        for entry in transcript:
            timestamp = self._format_timestamp(entry['start'])
            speaker, text = self._extract_speaker_and_text(entry['text'])
            
            if speaker:
                transcript_content += f"[{timestamp}] {speaker}: {text}\n"
            else:
                transcript_content += f"[{timestamp}] {text}\n"

        # Save to playlist location
        with open(playlist_filepath, 'w', encoding='utf-8') as f:
            f.write(transcript_content)
        print(f"Transcript saved to playlist folder: {playlist_filepath}")

        # Save to root location
        with open(root_filepath, 'w', encoding='utf-8') as f:
            f.write(transcript_content)
        print(f"Transcript saved to root folder: {root_filepath}")

        # Update tracking JSONs
        self.update_master_json(playlist_filepath, metadata, base_dir=root_dir)
        self.update_root_transcripts_json(root_filepath, metadata, base_dir=root_dir)
        
        return playlist_filepath, metadata

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

        # Initialize playlist metadata
        playlist_metadata = {
            'playlist_info': {
                'playlist_id': playlist_id,
                'title': playlist_info['title'],
                'url': playlist_info['url'],
                'channel': channel_info
            },
            'videos': []
        }
        
        # Process each video in the playlist
        for video_id in self._get_playlist_videos(playlist_id):
            video_url = f"https://www.youtube.com/watch?v={video_id}"
            try:
                result = self.save_transcript_with_timestamps(
                    video_url, 
                    base_dir=playlist_dir
                )
                if result:
                    filepath, metadata = result
                    saved_files.append(filepath)
                    playlist_metadata['videos'].append(metadata)
                    print(f"Processed video: {video_id}")
                else:
                    print(f"Failed to process video: {video_id}")
                
                # Update playlist metadata file
                metadata_file = os.path.join(playlist_dir, "playlist_metadata.json")
                with open(metadata_file, 'w', encoding='utf-8') as f:
                    json.dump(playlist_metadata, f, indent=2)
                
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