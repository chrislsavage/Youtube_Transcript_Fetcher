"""
Batch Processor for Dallas Willard Transcripts
Processes playlists in smaller batches with progress tracking and resume capability
"""

import os
import sys
import json
import time
from datetime import datetime
from enhanced_transcript_processor import DallasWillardTranscriptProcessor

class BatchTranscriptProcessor:
    def __init__(self, batch_size: int = 3, rate_limit: int = 8):
        self.processor = DallasWillardTranscriptProcessor(rate_limit_per_minute=rate_limit)
        self.batch_size = batch_size
        self.progress_file = "processing_progress.json"
        
    def save_progress(self, progress_data: dict):
        """Save processing progress to resume later if needed."""
        with open(self.progress_file, 'w', encoding='utf-8') as f:
            json.dump(progress_data, f, indent=2)
    
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
    
    def process_playlist_batch(self, playlist_info: dict, start_video: int = 0, max_videos: int = None):
        """Process a single playlist in batches."""
        playlist_id = playlist_info['playlist_id']
        playlist_title = playlist_info['title']
        
        print(f"\n{'='*60}")
        print(f"Processing: {playlist_title}")
        print(f"{'='*60}")
        
        # Get video IDs
        video_ids = list(self.processor._get_playlist_videos(playlist_id))
        total_videos = len(video_ids)
        
        if max_videos:
            video_ids = video_ids[:max_videos]
            print(f"Limited to first {max_videos} videos")
        
        if start_video > 0:
            video_ids = video_ids[start_video:]
            print(f"Starting from video {start_video + 1}")
        
        print(f"Found {len(video_ids)} videos to process")
        
        results = {
            'playlist_title': playlist_title,
            'processed': 0,
            'failed': 0,
            'videos': []
        }
        
        for i, video_id in enumerate(video_ids):
            current_video = start_video + i + 1
            video_url = f"https://www.youtube.com/watch?v={video_id}"
            
            print(f"\n[{current_video}/{total_videos}] Processing: {video_id}")
            
            try:
                metadata = self.processor.save_enhanced_transcript(video_url, playlist_info)
                if metadata:
                    results['processed'] += 1
                    results['videos'].append({
                        'video_id': video_id,
                        'title': metadata['title'],
                        'status': 'success',
                        'series': metadata['teaching_context']['series_name'],
                        'subjects': metadata['teaching_context']['subjects']
                    })
                    print(f"✓ Success: {metadata['title']}")
                else:
                    results['failed'] += 1
                    results['videos'].append({
                        'video_id': video_id,
                        'status': 'failed',
                        'reason': 'No transcript available'
                    })
                    print(f"✗ Failed: No transcript available")
            
            except Exception as e:
                results['failed'] += 1
                results['videos'].append({
                    'video_id': video_id,
                    'status': 'error',
                    'reason': str(e)
                })
                print(f"✗ Error: {str(e)}")
            
            # Show progress
            if (i + 1) % 5 == 0:
                print(f"\nProgress: {i + 1}/{len(video_ids)} videos")
                print(f"Success rate: {results['processed']}/{i + 1} ({100 * results['processed'] / (i + 1):.1f}%)")
        
        return results
    
    def process_selected_playlists(self, playlist_names: list = None, max_videos_per_playlist: int = None):
        """Process selected playlists or all playlists."""
        
        # Load playlist data
        with open('dallas_willard_playlists.json', 'r', encoding='utf-8') as f:
            playlist_data = json.load(f)
        
        playlists = playlist_data['playlists']
        
        # Filter playlists if specified
        if playlist_names:
            playlists = [p for p in playlists if p['title'] in playlist_names]
            print(f"Processing {len(playlists)} selected playlists")
        else:
            print(f"Processing all {len(playlists)} playlists")
        
        progress = self.load_progress()
        
        for i, playlist in enumerate(playlists):
            if playlist['title'] in progress['completed_playlists']:
                print(f"Skipping already completed: {playlist['title']}")
                continue
            
            try:
                results = self.process_playlist_batch(
                    playlist, 
                    max_videos=max_videos_per_playlist
                )
                
                # Update progress
                progress['completed_playlists'].append(playlist['title'])
                progress['stats']['total_processed'] += results['processed']
                progress['stats']['total_failed'] += results['failed']
                progress['current_playlist_index'] = i + 1
                
                # Collect unique series and subjects
                for video in results['videos']:
                    if video['status'] == 'success':
                        series = video.get('series')
                        if series and series not in progress['stats']['series_created']:
                            progress['stats']['series_created'].append(series)
                        
                        subjects = video.get('subjects', [])
                        for subject in subjects:
                            if subject not in progress['stats']['subjects_found']:
                                progress['stats']['subjects_found'].append(subject)
                
                self.save_progress(progress)
                
                print(f"\n✓ Completed playlist: {playlist['title']}")
                print(f"  Processed: {results['processed']}")
                print(f"  Failed: {results['failed']}")
                
            except Exception as e:
                print(f"✗ Error processing playlist {playlist['title']}: {e}")
                progress['failed_videos'].append({
                    'playlist': playlist['title'],
                    'error': str(e)
                })
                self.save_progress(progress)
        
        # Final summary
        print(f"\n{'='*60}")
        print("BATCH PROCESSING COMPLETE!")
        print(f"{'='*60}")
        print(f"Total processed: {progress['stats']['total_processed']}")
        print(f"Total failed: {progress['stats']['total_failed']}")
        print(f"Series created: {len(progress['stats']['series_created'])}")
        print(f"Subjects found: {len(progress['stats']['subjects_found'])}")
        
        return progress

def main():
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python batch_processor.py sample     # Process first 2 videos from first 3 playlists")
        print("  python batch_processor.py small      # Process first 5 videos from first 5 playlists")
        print("  python batch_processor.py full       # Process all playlists")
        print("  python batch_processor.py resume     # Resume previous processing")
        return
    
    mode = sys.argv[1]
    processor = BatchTranscriptProcessor()
    
    if mode == "sample":
        # Quick test - first 2 videos from first 3 playlists
        print("🧪 SAMPLE MODE: Processing first 2 videos from first 3 playlists")
        
        with open('dallas_willard_playlists.json', 'r', encoding='utf-8') as f:
            playlist_data = json.load(f)
        
        sample_playlists = [p['title'] for p in playlist_data['playlists'][:3]]
        processor.process_selected_playlists(sample_playlists, max_videos_per_playlist=2)
    
    elif mode == "small":
        # Small batch - first 5 videos from first 5 playlists
        print("📦 SMALL MODE: Processing first 5 videos from first 5 playlists")
        
        with open('dallas_willard_playlists.json', 'r', encoding='utf-8') as f:
            playlist_data = json.load(f)
        
        small_playlists = [p['title'] for p in playlist_data['playlists'][:5]]
        processor.process_selected_playlists(small_playlists, max_videos_per_playlist=5)
    
    elif mode == "full":
        # Process everything
        print("🚀 FULL MODE: Processing all playlists")
        processor.process_selected_playlists()
    
    elif mode == "resume":
        # Resume previous processing
        print("⏯️  RESUME MODE: Continuing previous processing")
        processor.process_selected_playlists()
    
    else:
        print(f"Unknown mode: {mode}")

if __name__ == "__main__":
    main()