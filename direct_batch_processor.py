#!/usr/bin/env python3
"""
Direct connection batch processor for remaining playlists
Uses playlist scraping without proxies to process remaining videos
"""

import os
import sys
import json
import time
from datetime import datetime
from proxy_enhanced_processor import ProxyEnhancedTranscriptProcessor

class DirectBatchProcessor:
    def __init__(self, batch_size: int = 3, rate_limit: int = 15):
        self.processor = ProxyEnhancedTranscriptProcessor(
            rate_limit_per_minute=rate_limit,
            use_proxies=False  # Use direct connection
        )
        self.batch_size = batch_size
        self.progress_file = "processing_progress.json"
        
    def load_progress(self) -> dict:
        """Load existing progress data."""
        if os.path.exists(self.progress_file):
            with open(self.progress_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {
            'completed_playlists': [],
            'completed_videos': [],
            'failed_videos': [],
            'current_playlist_index': 0,
            'start_time': datetime.now().isoformat(),
            'stats': {
                'total_processed': 0,
                'total_failed': 0,
                'series_created': [],
                'subjects_found': []
            }
        }
    
    def save_progress(self, progress_data: dict):
        """Save processing progress."""
        with open(self.progress_file, 'w', encoding='utf-8') as f:
            json.dump(progress_data, f, indent=2)
    
    def load_playlists(self):
        """Load playlist data."""
        with open('dallas_willard_playlists.json', 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def process_playlist_with_direct_connection(self, playlist_info: dict):
        """Process a single playlist using direct connection."""
        playlist_title = playlist_info.get('title', 'Unknown')
        playlist_id = playlist_info.get('playlist_id', '')
        playlist_url = playlist_info.get('url', f"https://www.youtube.com/playlist?list={playlist_id}")
        
        print(f"\n{'='*60}")
        print(f"🔄 Processing with direct connection: {playlist_title}")
        print(f"{'='*60}")
        
        # Use playlist scraping to get videos
        print(f"🔍 Scraping videos from playlist URL: {playlist_url}")
        videos = self.processor.scrape_playlist_videos(playlist_url)
        
        if not videos:
            print(f"⚠️ No videos found in playlist: {playlist_title}")
            return {'processed': 0, 'failed': 0}
        
        processed = 0
        failed = 0
        
        print(f"Found {len(videos)} videos to process")
        
        for i, video in enumerate(videos, 1):
            video_id = video.get('video_id', '')
            video_title = video.get('title', 'Unknown')
            
            if not video_id:
                print(f"[{i}/{len(videos)}] ❌ No video ID found")
                failed += 1
                continue
            
            video_url = f"https://www.youtube.com/watch?v={video_id}"
            
            print(f"\n[{i}/{len(videos)}] Processing: {video_id}")
            print(f"📝 Title: {video_title[:60]}...")
            
            try:
                result = self.processor.process_single_video(video_url, playlist_title)
                
                if result and result.get('success'):
                    print(f"✅ Success: {video_title}")
                    processed += 1
                else:
                    print(f"❌ Failed: {video_title}")
                    failed += 1
                    
            except Exception as e:
                print(f"💥 Error processing {video_id}: {str(e)}")
                failed += 1
            
            # Progress update
            if i % 5 == 0:
                success_rate = (processed / i) * 100 if i > 0 else 0
                print(f"📊 Progress: {i}/{len(videos)} ({success_rate:.1f}% success rate)")
        
        print(f"\n✅ Completed playlist: {playlist_title}")
        print(f"   Processed: {processed}")
        print(f"   Failed: {failed}")
        if len(videos) > 0:
            print(f"   Success rate: {processed}/{len(videos)} ({(processed/len(videos)*100):.1f}%)")
        
        return {'processed': processed, 'failed': failed}
    
    def resume_processing_direct(self):
        """Resume processing using direct connection."""
        print("🔄 DIRECT BATCH PROCESSING: Starting with direct connection")
        print("=" * 80)
        
        # Load progress and playlists
        progress_data = self.load_progress()
        playlists_data = self.load_playlists()
        playlists = playlists_data.get('playlists', [])
        
        completed_playlists = set(progress_data.get('completed_playlists', []))
        
        print(f"📋 Total playlists: {len(playlists)}")
        print(f"✅ Already completed: {len(completed_playlists)}")
        print(f"⏳ Remaining: {len(playlists) - len(completed_playlists)}")
        
        total_processed = 0
        total_failed = 0
        
        for i, playlist in enumerate(playlists):
            playlist_title = playlist.get('title', f'Playlist_{i}')
            
            # Skip completed playlists
            if playlist_title in completed_playlists:
                print(f"Skipping already completed: {playlist_title}")
                continue
            
            try:
                # Process playlist with direct connection
                result = self.process_playlist_with_direct_connection(playlist)
                
                total_processed += result['processed']
                total_failed += result['failed']
                
                # Mark playlist as completed
                progress_data['completed_playlists'].append(playlist_title)
                progress_data['stats']['total_processed'] = total_processed
                progress_data['stats']['total_failed'] = total_failed
                
                # Save progress
                self.save_progress(progress_data)
                
                print(f"💾 Progress saved: {len(progress_data['completed_playlists'])}/{len(playlists)} playlists complete")
                
            except Exception as e:
                print(f"💥 Error processing playlist {playlist_title}: {e}")
                continue
        
        # Print final statistics
        self.processor.print_session_stats()
        
        print(f"\n🎉 DIRECT BATCH PROCESSING COMPLETE!")
        print(f"📊 Final Results:")
        print(f"   ✅ Total processed: {total_processed}")
        print(f"   ❌ Total failed: {total_failed}")
        if (total_processed + total_failed) > 0:
            print(f"   📈 Overall success rate: {(total_processed/(total_processed+total_failed)*100):.1f}%")
        else:
            print(f"   📈 Overall success rate: N/A (no videos processed in this session)")

def main():
    if len(sys.argv) < 2:
        print("Usage: python direct_batch_processor.py <command>")
        print("Commands:")
        print("  resume   - Resume processing with direct connection")
        return
    
    command = sys.argv[1]
    
    if command == "resume":
        processor = DirectBatchProcessor(
            batch_size=2,  # Conservative batch size
            rate_limit=10  # Conservative rate limiting for direct connection
        )
        processor.resume_processing_direct()
    else:
        print(f"Unknown command: {command}")

if __name__ == "__main__":
    main()