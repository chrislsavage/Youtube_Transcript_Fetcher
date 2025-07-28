"""
Progress Monitor for Dallas Willard Transcript Processing
Monitors processing progress and provides real-time updates
"""

import time
import json
import os
from datetime import datetime

def get_current_status():
    """Get current processing status."""
    try:
        # Check transcript count
        transcript_count = len([f for f in os.listdir('dallas_willard_transcripts') 
                               if os.path.isdir(os.path.join('dallas_willard_transcripts', f))])
        
        # Count actual transcript files
        total_files = 0
        for root, dirs, files in os.walk('dallas_willard_transcripts'):
            total_files += len([f for f in files if f.endswith('.txt')])
        
        # Check progress file
        progress_data = {}
        if os.path.exists('processing_progress.json'):
            with open('processing_progress.json', 'r') as f:
                progress_data = json.load(f)
        
        # Check knowledge base index
        index_data = {}
        if os.path.exists('knowledge_base_index.json'):
            with open('knowledge_base_index.json', 'r') as f:
                index_data = json.load(f)
        
        return {
            'timestamp': datetime.now().strftime('%H:%M:%S'),
            'transcript_files': total_files,
            'series_folders': transcript_count,
            'completed_playlists': len(progress_data.get('completed_playlists', [])),
            'total_processed': progress_data.get('stats', {}).get('total_processed', 0),
            'total_failed': progress_data.get('stats', {}).get('total_failed', 0),
            'series_created': len(progress_data.get('stats', {}).get('series_created', [])),
            'subjects_found': len(progress_data.get('stats', {}).get('subjects_found', [])),
            'content_chunks': index_data.get('summary', {}).get('total_chunks', 0)
        }
    except Exception as e:
        return {'error': str(e), 'timestamp': datetime.now().strftime('%H:%M:%S')}

def monitor_progress(duration_minutes=30, interval_seconds=60):
    """Monitor progress for specified duration."""
    print(f"🔍 MONITORING PROGRESS FOR {duration_minutes} MINUTES")
    print("=" * 60)
    
    start_time = time.time()
    end_time = start_time + (duration_minutes * 60)
    
    previous_status = None
    
    while time.time() < end_time:
        current_status = get_current_status()
        
        if 'error' in current_status:
            print(f"❌ Error: {current_status['error']}")
        else:
            # Show current status
            print(f"\n[{current_status['timestamp']}] 📊 CURRENT STATUS:")
            print(f"   📄 Transcript Files: {current_status['transcript_files']}")
            print(f"   📁 Series Folders: {current_status['series_folders']}")
            print(f"   ✅ Completed Playlists: {current_status['completed_playlists']}/28")
            print(f"   🎥 Videos Processed: {current_status['total_processed']}")
            print(f"   ❌ Videos Failed: {current_status['total_failed']}")
            print(f"   📚 Series Created: {current_status['series_created']}")
            print(f"   🏷️ Subjects Found: {current_status['subjects_found']}")
            print(f"   🗂️ Content Chunks: {current_status['content_chunks']}")
            
            # Show changes since last check
            if previous_status:
                changes = []
                for key in ['transcript_files', 'total_processed', 'completed_playlists', 'series_created']:
                    diff = current_status[key] - previous_status[key]
                    if diff > 0:
                        changes.append(f"+{diff} {key.replace('_', ' ')}")
                
                if changes:
                    print(f"   🔄 Changes: {', '.join(changes)}")
                else:
                    print(f"   ⏸️ No changes (processing may be waiting or rate limited)")
            
            previous_status = current_status
        
        time.sleep(interval_seconds)
    
    print(f"\n✅ Monitoring complete after {duration_minutes} minutes")
    return current_status

def main():
    import sys
    
    duration = 30  # Default 30 minutes
    if len(sys.argv) > 1:
        try:
            duration = int(sys.argv[1])
        except ValueError:
            print("Invalid duration, using 30 minutes")
    
    print(f"Starting {duration}-minute monitoring session...")
    final_status = monitor_progress(duration_minutes=duration)
    
    if 'error' not in final_status:
        print(f"\n🎉 FINAL STATUS:")
        print(f"📄 Total Transcript Files: {final_status['transcript_files']}")
        print(f"✅ Playlists Completed: {final_status['completed_playlists']}/28")
        print(f"🎥 Videos Processed: {final_status['total_processed']}")
        print(f"❌ Videos Failed: {final_status['total_failed']}")

if __name__ == "__main__":
    main()