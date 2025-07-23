"""
Quick status checker for Dallas Willard Knowledge Base processing
"""

import json
import os
from datetime import datetime

def check_processing_status():
    """Check current processing status and provide summary."""
    
    # Load progress data
    if os.path.exists('processing_progress.json'):
        with open('processing_progress.json', 'r') as f:
            progress = json.load(f)
    else:
        print("❌ No processing progress found. Run batch_processor.py first.")
        return
    
    # Load playlist data for totals
    if os.path.exists('dallas_willard_playlists.json'):
        with open('dallas_willard_playlists.json', 'r') as f:
            playlist_data = json.load(f)
        total_playlists = len(playlist_data['playlists'])
    else:
        total_playlists = 28  # Known total
    
    # Check knowledge base directory
    kb_exists = os.path.exists('dallas_willard_transcripts')
    
    print("🎯 DALLAS WILLARD KNOWLEDGE BASE STATUS")
    print("=" * 50)
    
    # Processing Statistics
    completed_playlists = len(progress['completed_playlists'])
    remaining_playlists = total_playlists - completed_playlists
    completion_percentage = (completed_playlists / total_playlists) * 100
    
    print(f"📊 PROCESSING PROGRESS:")
    print(f"   ✅ Completed Playlists: {completed_playlists}/{total_playlists} ({completion_percentage:.1f}%)")
    print(f"   🎥 Videos Processed: {progress['stats']['total_processed']}")
    print(f"   ❌ Videos Failed: {progress['stats']['total_failed']}")
    print(f"   📁 Series Created: {len(progress['stats']['series_created'])}")
    print(f"   🏷️ Subjects Found: {len(progress['stats']['subjects_found'])}")
    
    # Knowledge Base Status
    print(f"\n📚 KNOWLEDGE BASE:")
    if kb_exists:
        print(f"   ✅ Directory Created: dallas_willard_transcripts/")
        
        # Count actual files
        total_files = 0
        total_metadata = 0
        for root, dirs, files in os.walk('dallas_willard_transcripts'):
            for file in files:
                if file.endswith('.txt'):
                    total_files += 1
                elif file.endswith('_metadata.json'):
                    total_metadata += 1
        
        print(f"   📄 Transcript Files: {total_files}")
        print(f"   📋 Metadata Files: {total_metadata}")
        
        # Check index file
        if os.path.exists('dallas_willard_transcripts/knowledge_base_index.json'):
            with open('dallas_willard_transcripts/knowledge_base_index.json', 'r') as f:
                index = json.load(f)
            print(f"   🗂️ Content Chunks: {index.get('total_chunks', 0)}")
            print(f"   📊 Last Updated: {index.get('last_updated', 'Unknown')}")
        
    else:
        print(f"   ❌ Knowledge base directory not found")
    
    # Remaining Work
    print(f"\n⏳ REMAINING WORK:")
    if remaining_playlists > 0:
        remaining_list = []
        all_playlists = [p['title'] for p in playlist_data['playlists']]
        for playlist in all_playlists:
            if playlist not in progress['completed_playlists']:
                remaining_list.append(playlist)
        
        print(f"   📋 Playlists to Process: {remaining_playlists}")
        print(f"   📝 Next Playlists:")
        for i, playlist in enumerate(remaining_list[:5], 1):
            print(f"      {i}. {playlist}")
        if len(remaining_list) > 5:
            print(f"      ... and {len(remaining_list) - 5} more")
    else:
        print("   🎉 All playlists completed!")
    
    # Series Overview
    if progress['stats']['series_created']:
        print(f"\n📚 SERIES CREATED:")
        for series in progress['stats']['series_created']:
            print(f"   • {series}")
    
    # Subjects Overview
    if progress['stats']['subjects_found']:
        print(f"\n🏷️ SUBJECTS IDENTIFIED:")
        subjects_per_line = 3
        subjects = progress['stats']['subjects_found']
        for i in range(0, len(subjects), subjects_per_line):
            line_subjects = subjects[i:i+subjects_per_line]
            print(f"   • {' • '.join(line_subjects)}")
    
    # Next Steps
    print(f"\n🚀 NEXT STEPS:")
    if progress['stats']['total_failed'] > 0 and remaining_playlists > 0:
        print("   1. Wait 24-48 hours for YouTube IP block to expire")
        print("   2. Run: python batch_processor.py resume")
        print("   3. Continue until all playlists are processed")
    elif remaining_playlists > 0:
        print("   1. Run: python batch_processor.py resume")
        print("   2. Continue processing remaining playlists")
    else:
        print("   1. 🎉 Processing complete!")
        print("   2. Build vector database for RAG implementation")
        print("   3. Create search interface")
        print("   4. Generate subject analysis graphs")

if __name__ == "__main__":
    check_processing_status()