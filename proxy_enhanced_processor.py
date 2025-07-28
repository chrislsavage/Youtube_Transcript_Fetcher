"""
Enhanced YouTube Transcript Processor with Rotating Residential Proxies
Uses Webshare rotating residential proxies to avoid IP blocking
"""

import os
import sys
import json
import re
import time
import random
from datetime import datetime
from typing import List, Dict, Optional, Set, Tuple
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api.formatters import TextFormatter
import requests
from urllib.parse import parse_qs, urlparse

# Check if proxy support is available
try:
    from youtube_transcript_api.proxies import WebshareProxyConfig
    PROXY_SUPPORT = True
except ImportError:
    PROXY_SUPPORT = False
    print("⚠️ Proxy support not available. Install latest youtube-transcript-api version.")

class ProxyEnhancedTranscriptProcessor:
    def __init__(self, 
                 rate_limit_per_minute: int = 20,
                 proxy_username: str = None,
                 proxy_password: str = None,
                 use_proxies: bool = True):
        self.rate_limit = rate_limit_per_minute
        self.last_request_time = 0
        self.consecutive_failures = 0
        self.success_count = 0
        self.failure_count = 0
        self.session_start = time.time()
        
        # Initialize YouTube API with or without proxies
        if use_proxies and PROXY_SUPPORT and proxy_username and proxy_password:
            print(f"🔄 Initializing with rotating residential proxies...")
            self.proxy_config = WebshareProxyConfig(
                proxy_username=proxy_username,
                proxy_password=proxy_password,
                # Optional: filter to specific countries for lower latency
                # filter_ip_locations=["us", "ca", "gb"]
            )
            self.api = YouTubeTranscriptApi(proxy_config=self.proxy_config)
            self.using_proxies = True
            print(f"✅ Proxy configuration active")
        else:
            print(f"📡 Using direct connection (no proxies)")
            self.api = YouTubeTranscriptApi()
            self.using_proxies = False
        
        # Enhanced speaker patterns for Dallas Willard content
        self.speaker_patterns = [
            r'^\[([^\]]+)\]:(.+)$',
            r'^([^:]+):(.+)$',
            r'^\(([^\)]+)\):(.+)$',
            r'<([^>]+)>:(.+)$',
            r'^(Dallas|Dr\.?\s*Willard|Dallas\s*Willard):(.+)$',
            r'^(John|John\s*Ortberg):(.+)$',
            r'^(Interviewer|Host|Moderator):(.+)$'
        ]
        
        # Key subject areas for Dallas Willard teachings
        self.subject_keywords = {
            'discipleship': ['discipleship', 'disciple', 'apprentice', 'apprenticeship', 'following jesus'],
            'spiritual_formation': ['spiritual formation', 'formation', 'spiritual discipline', 'discipline'],
            'kingdom_of_god': ['kingdom of god', 'kingdom of heaven', 'kingdom', 'divine conspiracy'],
            'prayer': ['prayer', 'praying', 'pray', 'communion with god'],
            'hearing_god': ['hearing god', 'gods voice', 'divine guidance', 'listening to god'],
            'spiritual_disciplines': ['meditation', 'solitude', 'silence', 'fasting', 'study', 'worship'],
            'philosophy': ['philosophy', 'philosophical', 'epistemology', 'knowledge', 'truth'],
            'soul': ['soul', 'inner life', 'heart', 'spirit', 'character', 'transformation'],
            'will_of_god': ['will of god', 'gods will', 'divine will', 'purpose', 'calling'],
            'righteousness': ['righteousness', 'righteous', 'holiness', 'holy living', 'virtue'],
            'scripture': ['bible', 'scripture', 'biblical', 'gods word', 'word of god'],
            'church': ['church', 'christian community', 'fellowship', 'community'],
            'apologetics': ['apologetics', 'defense of faith', 'rational faith', 'evidence'],
            'teaching': ['teaching', 'teacher', 'education', 'knowledge', 'learning']
        }

    def _rate_limit_wait(self, is_retry: bool = False):
        """Enhanced rate limiting with exponential backoff and jitter."""
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time
        
        # Base wait time (more conservative when using proxies)
        base_wait = (60 / self.rate_limit) - time_since_last_request
        
        # Reduced exponential backoff when using proxies (proxies handle IP rotation)
        if is_retry and self.consecutive_failures > 0:
            if self.using_proxies:
                # Shorter backoff with proxies since IP rotation should help
                backoff_multiplier = min(2 ** self.consecutive_failures, 4)  # Cap at 4x
                base_wait = max(base_wait, backoff_multiplier * 30)  # Max 2 minutes
            else:
                # Longer backoff without proxies
                backoff_multiplier = min(2 ** self.consecutive_failures, 16)
                base_wait = max(base_wait, backoff_multiplier * 60)
            
            proxy_status = "with proxy rotation" if self.using_proxies else "without proxies"
            print(f"⏳ Exponential backoff: waiting {base_wait/60:.1f} minutes (attempt {self.consecutive_failures + 1}, {proxy_status})")
        
        # Add random jitter
        jitter = random.uniform(0.8, 1.2)
        final_wait = max(base_wait * jitter, 3)  # Minimum 3 seconds
        
        if final_wait > 60:
            print(f"⏳ Waiting {final_wait/60:.1f} minutes before next request...")
        elif final_wait > 10:
            print(f"⏳ Waiting {final_wait:.1f} seconds...")
        
        if final_wait > 0:
            time.sleep(final_wait)
        
        self.last_request_time = time.time()

    def get_transcript_with_retry(self, video_id: str, max_retries: int = 3):
        """Fetch transcript with intelligent retry logic and proxy support."""
        for attempt in range(max_retries + 1):
            try:
                self._rate_limit_wait(is_retry=(attempt > 0))
                
                # Fetch transcript using the configured API (with or without proxies)
                if self.using_proxies:
                    transcript_list = self.api.list(video_id)
                    transcript = transcript_list.find_transcript(['en']).fetch()
                else:
                    transcript_list = YouTubeTranscriptApi().list(video_id)
                    transcript = transcript_list.find_transcript(['en']).fetch()
                
                # Reset failure counter on success
                self.consecutive_failures = 0
                self.success_count += 1
                
                # Log success rate periodically
                if (self.success_count + self.failure_count) % 10 == 0:
                    total_requests = self.success_count + self.failure_count
                    success_rate = (self.success_count / total_requests) * 100
                    proxy_status = "with proxies" if self.using_proxies else "direct"
                    print(f"📊 Success rate: {success_rate:.1f}% ({self.success_count}/{total_requests}) {proxy_status}")
                
                return transcript
                
            except Exception as e:
                error_msg = str(e).lower()
                self.failure_count += 1
                
                # Check if it's a rate limiting/blocking error
                if any(keyword in error_msg for keyword in ['blocked', 'rate', 'quota', 'too many']):
                    self.consecutive_failures += 1
                    
                    if self.using_proxies:
                        print(f"🔄 Proxy request blocked, rotating IP (attempt {attempt + 1}/{max_retries + 1})")
                    else:
                        print(f"🚫 IP blocked (attempt {attempt + 1}/{max_retries + 1})")
                    
                    if attempt < max_retries:
                        # Shorter wait with proxies since IP should rotate
                        wait_time = 60 if self.using_proxies else min(2 ** (attempt + 1) * 60, 900)
                        if wait_time > 60:
                            print(f"⏳ Waiting {wait_time/60:.1f} minutes before retry...")
                        time.sleep(wait_time)
                        continue
                    else:
                        print(f"❌ Max retries exceeded for video {video_id}")
                        raise
                else:
                    # Non-rate-limit error, don't retry
                    print(f"❌ Non-blocking error for {video_id}: {str(e)[:100]}...")
                    raise
        
        return None

    def extract_subjects(self, text: str) -> Set[str]:
        """Extract key subject areas from text."""
        text_lower = text.lower()
        found_subjects = set()
        
        for subject, keywords in self.subject_keywords.items():
            for keyword in keywords:
                if keyword in text_lower:
                    found_subjects.add(subject)
        
        return found_subjects

    def _extract_video_id(self, url: str) -> Optional[str]:
        """Extract video ID from YouTube URL."""
        patterns = [
            r'(?:v=|\/)([0-9A-Za-z_-]{11}).*',
            r'(?:embed\/)([0-9A-Za-z_-]{11})',
            r'(?:v\/)([0-9A-Za-z_-]{11})'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                return match.group(1)
        return None

    def process_single_video(self, video_url: str, playlist_title: str = "Single-Video-Processing"):
        """Process a single video with proxy support."""
        try:
            video_id = self._extract_video_id(video_url)
            if not video_id:
                print(f"❌ Could not extract video ID from URL: {video_url}")
                return None
            
            print(f"🎥 Processing video {video_id} {' (with proxies)' if self.using_proxies else '(direct)'}")
            
            # Get transcript with retry logic
            transcript = self.get_transcript_with_retry(video_id)
            
            if not transcript:
                return {'success': False, 'error': 'No transcript available'}
            
            # Format transcript
            formatter = TextFormatter()
            formatted_text = formatter.format_transcript(transcript)
            
            # Extract subjects
            subjects = self.extract_subjects(formatted_text)
            
            # Create output directory
            safe_title = re.sub(r'[^\w\s-]', '', playlist_title).strip()
            safe_title = re.sub(r'[-\s]+', '-', safe_title)
            output_dir = f"dallas_willard_transcripts/{safe_title}"
            os.makedirs(output_dir, exist_ok=True)
            
            # Save transcript
            output_file = f"{output_dir}/video_{video_id}.txt"
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(formatted_text)
            
            print(f"✅ Success: Transcript saved to {output_file}")
            print(f"🏷️ Subjects found: {', '.join(subjects) if subjects else 'None'}")
            
            return {
                'success': True,
                'transcript_file': output_file,
                'subjects': list(subjects),
                'video_id': video_id
            }
            
        except Exception as e:
            print(f"❌ Error processing {video_url}: {str(e)}")
            return {'success': False, 'error': str(e)}

    def print_session_stats(self):
        """Print session statistics."""
        total_requests = self.success_count + self.failure_count
        if total_requests > 0:
            success_rate = (self.success_count / total_requests) * 100
            session_time = (time.time() - self.session_start) / 60
            proxy_status = "with rotating proxies" if self.using_proxies else "direct connection"
            
            print(f"\n📊 SESSION STATISTICS ({proxy_status}):")
            print(f"   ✅ Successful requests: {self.success_count}")
            print(f"   ❌ Failed requests: {self.failure_count}")
            print(f"   📈 Success rate: {success_rate:.1f}%")
            print(f"   ⏱️ Session duration: {session_time:.1f} minutes")
            print(f"   🚀 Requests per minute: {total_requests/session_time:.1f}")

def main():
    """Main function with proxy configuration."""
    if len(sys.argv) < 2:
        print("Usage: python proxy_enhanced_processor.py <command> [options]")
        print("Commands:")
        print("  single <video_url>                    - Process single video")
        print("  test-proxy <username> <password>      - Test proxy configuration")
        print("  test-direct                           - Test direct connection")
        return
    
    command = sys.argv[1]
    
    if command == "single":
        if len(sys.argv) < 3:
            print("Usage: python proxy_enhanced_processor.py single <video_url>")
            return
        
        video_url = sys.argv[2]
        
        # Try to get proxy credentials from environment or command line
        proxy_username = os.getenv('WEBSHARE_USERNAME')
        proxy_password = os.getenv('WEBSHARE_PASSWORD')
        
        if len(sys.argv) >= 5:
            proxy_username = sys.argv[3]
            proxy_password = sys.argv[4]
        
        use_proxies = bool(proxy_username and proxy_password)
        
        if use_proxies:
            print(f"🔄 Using rotating residential proxies")
        else:
            print(f"📡 Using direct connection (set WEBSHARE_USERNAME/WEBSHARE_PASSWORD for proxies)")
        
        processor = ProxyEnhancedTranscriptProcessor(
            rate_limit_per_minute=30,
            proxy_username=proxy_username,
            proxy_password=proxy_password,
            use_proxies=use_proxies
        )
        
        result = processor.process_single_video(video_url)
        processor.print_session_stats()
        
    elif command == "test-proxy":
        if len(sys.argv) < 4:
            print("Usage: python proxy_enhanced_processor.py test-proxy <username> <password>")
            return
        
        username = sys.argv[2]
        password = sys.argv[3]
        
        processor = ProxyEnhancedTranscriptProcessor(
            rate_limit_per_minute=10,
            proxy_username=username,
            proxy_password=password,
            use_proxies=True
        )
        
        # Test with a known video
        test_url = "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
        print(f"🧪 Testing proxy configuration with test video...")
        result = processor.process_single_video(test_url)
        processor.print_session_stats()
        
    elif command == "test-direct":
        processor = ProxyEnhancedTranscriptProcessor(
            rate_limit_per_minute=10,
            use_proxies=False
        )
        
        test_url = "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
        print(f"🧪 Testing direct connection with test video...")
        result = processor.process_single_video(test_url)
        processor.print_session_stats()
    
    else:
        print(f"Unknown command: {command}")

if __name__ == "__main__":
    main()