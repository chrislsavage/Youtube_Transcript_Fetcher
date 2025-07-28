"""
Optimized Recovery Processor for Failed Dallas Willard Videos
Uses enhanced rate limiting with exponential backoff to recover previously failed transcripts
"""

import os
import json
import time
from datetime import datetime
from enhanced_transcript_processor import DallasWillardTranscriptProcessor

class FailedVideoRecoveryProcessor:
    def __init__(self):
        self.processor = DallasWillardTranscriptProcessor(rate_limit_per_minute=6)
        self.progress_file = "processing_progress.json"
        self.recovery_log = "recovery_log.json"
        
    def load_failed_videos(self):
        """Load list of previously failed videos from progress file."""
        if not os.path.exists(self.progress_file):
            print("❌ No processing progress file found")
            return []
            
        with open(self.progress_file, 'r', encoding='utf-8') as f:
            progress_data = json.load(f)
        
        failed_videos = progress_data.get('failed_videos', [])
        print(f"📋 Found {len(failed_videos)} failed videos to retry")
        return failed_videos
    
    def load_playlist_data(self):
        """Load playlist data to get video context."""
        playlist_file = "dallas_willard_playlists.json"
        if not os.path.exists(playlist_file):
            print("❌ Playlist data file not found")
            return {}
            
        with open(playlist_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def find_video_context(self, video_id: str, playlists_data: dict):
        """Find which playlist and title a video belongs to."""
        for playlist in playlists_data.get('playlists', []):
            for video in playlist.get('videos', []):
                if video.get('video_id') == video_id:
                    return {
                        'playlist_title': playlist.get('title', 'Unknown'),
                        'video_title': video.get('title', 'Unknown'),
                        'video_url': f"https://www.youtube.com/watch?v={video_id}"
                    }
        return None
    
    def save_recovery_log(self, recovery_data: dict):
        """Save recovery progress."""
        with open(self.recovery_log, 'w', encoding='utf-8') as f:
            json.dump(recovery_data, f, indent=2)
    
    def recover_failed_videos(self, max_videos: int = None):
        """Attempt to recover failed video transcripts with enhanced rate limiting."""
        failed_videos = self.load_failed_videos()
        if not failed_videos:
            print("✅ No failed videos to recover!")
            return
        
        playlists_data = self.load_playlist_data()
        
        # Limit processing if specified
        if max_videos:
            failed_videos = failed_videos[:max_videos]
            print(f"🎯 Processing first {max_videos} failed videos")
        
        recovery_stats = {
            'start_time': datetime.now().isoformat(),
            'total_attempted': len(failed_videos),
            'recovered': 0,
            'still_failed': 0,
            'recovered_videos': [],
            'still_failing_videos': []
        }
        
        print(f"🚀 Starting recovery of {len(failed_videos)} videos with enhanced rate limiting...")
        print("📝 Strategy: 6 requests/minute + exponential backoff + session caching + jitter")
        
        for i, video_id in enumerate(failed_videos, 1):
            print(f"\n[{i}/{len(failed_videos)}] Attempting recovery: {video_id}")
            
            # Get video context
            context = self.find_video_context(video_id, playlists_data)
            if context:
                print(f"📂 Playlist: {context['playlist_title']}")
                print(f"🎥 Title: {context['video_title']}")
                video_url = context['video_url']
            else:
                video_url = f"https://www.youtube.com/watch?v={video_id}"
                print(f"⚠️  Context not found, using URL: {video_url}")
            
            try:
                # Attempt to process the video
                result = self.processor.process_single_video(video_url)
                
                if result and result.get('success'):
                    print(f"✅ RECOVERED: {video_id}")
                    recovery_stats['recovered'] += 1
                    recovery_stats['recovered_videos'].append({
                        'video_id': video_id,
                        'recovered_at': datetime.now().isoformat(),
                        'transcript_file': result.get('transcript_file'),
                        'metadata_file': result.get('metadata_file')
                    })
                else:
                    print(f"❌ Still failed: {video_id}")
                    recovery_stats['still_failed'] += 1
                    recovery_stats['still_failing_videos'].append(video_id)
                    
            except Exception as e:
                print(f"💥 Error processing {video_id}: {e}")
                recovery_stats['still_failed'] += 1
                recovery_stats['still_failing_videos'].append(video_id)
            
            # Save progress every 5 videos
            if i % 5 == 0:
                recovery_stats['end_time'] = datetime.now().isoformat()
                self.save_recovery_log(recovery_stats)
                print(f"💾 Progress saved: {recovery_stats['recovered']} recovered so far")
        
        # Final save
        recovery_stats['end_time'] = datetime.now().isoformat()
        self.save_recovery_log(recovery_stats)
        
        # Print final results
        print(f"\n🎉 RECOVERY COMPLETE!")
        print(f"✅ Recovered: {recovery_stats['recovered']}")
        print(f"❌ Still failed: {recovery_stats['still_failed']}")
        print(f"📊 Success rate: {(recovery_stats['recovered'] / len(failed_videos) * 100):.1f}%")
        
        if recovery_stats['recovered'] > 0:
            print(f"📁 Check recovered transcripts in: dallas_willard_transcripts/")
        
        return recovery_stats

def main():
    import sys
    
    processor = FailedVideoRecoveryProcessor()
    
    if len(sys.argv) > 1:
        try:
            max_videos = int(sys.argv[1])
            print(f"🎯 Recovery limited to {max_videos} videos")
        except ValueError:
            print("❌ Invalid number argument, processing all failed videos")
            max_videos = None
    else:
        max_videos = None
    
    processor.recover_failed_videos(max_videos)

if __name__ == "__main__":
    main()