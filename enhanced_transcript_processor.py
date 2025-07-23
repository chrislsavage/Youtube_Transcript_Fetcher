"""
Enhanced YouTube Transcript Processor for Dallas Willard Knowledge Base
Processes transcripts with rich metadata extraction, subject analysis, and vector database preparation
"""

import os
import sys
import json
import re
import time
from datetime import datetime
from typing import List, Dict, Optional, Set, Tuple
from youtube_transcript_api import YouTubeTranscriptApi
import requests
from urllib.parse import parse_qs, urlparse

class DallasWillardTranscriptProcessor:
    def __init__(self, rate_limit_per_minute: int = 30):
        self.rate_limit = rate_limit_per_minute
        self.last_request_time = 0
        
        # Enhanced speaker patterns for Dallas Willard content
        self.speaker_patterns = [
            r'^\[([^\]]+)\]:(.+)$',  # [Speaker]: Text
            r'^([^:]+):(.+)$',        # Speaker: Text
            r'^\(([^\)]+)\):(.+)$',   # (Speaker): Text
            r'<([^>]+)>:(.+)$',       # <Speaker>: Text
            r'^(Dallas|Dr\.?\s*Willard|Dallas\s*Willard):(.+)$',  # Dallas Willard variations
            r'^(John|John\s*Ortberg):(.+)$',  # John Ortberg
            r'^(Interviewer|Host|Moderator):(.+)$'  # Common interview roles
        ]
        
        # Key subject areas for Dallas Willard teachings
        self.subject_keywords = {
            'discipleship': ['discipleship', 'disciple', 'apprentice', 'apprenticeship', 'following jesus', 'learning from jesus'],
            'spiritual_formation': ['spiritual formation', 'formation', 'spiritual discipline', 'discipline', 'spiritual practice', 'practice'],
            'kingdom_of_god': ['kingdom of god', 'kingdom of heaven', 'kingdom', 'divine conspiracy', 'gods reign'],
            'prayer': ['prayer', 'praying', 'pray', 'communion with god', 'conversation with god'],
            'hearing_god': ['hearing god', 'gods voice', 'divine guidance', 'listening to god', 'guidance'],
            'spiritual_disciplines': ['meditation', 'solitude', 'silence', 'fasting', 'study', 'worship', 'celebration', 'service'],
            'philosophy': ['philosophy', 'philosophical', 'epistemology', 'knowledge', 'truth', 'reality'],
            'soul': ['soul', 'inner life', 'heart', 'spirit', 'character', 'transformation'],
            'will_of_god': ['will of god', 'gods will', 'divine will', 'purpose', 'calling'],
            'righteousness': ['righteousness', 'righteous', 'holiness', 'holy living', 'virtue', 'character'],
            'scripture': ['bible', 'scripture', 'biblical', 'gods word', 'word of god'],
            'church': ['church', 'christian community', 'fellowship', 'community', 'body of christ'],
            'apologetics': ['apologetics', 'defense of faith', 'rational faith', 'evidence', 'reason'],
            'teaching': ['teaching', 'teacher', 'education', 'knowledge', 'learning', 'instruction']
        }
        
        # Scripture reference patterns
        self.scripture_patterns = [
            r'\b([1-3]?\s*[A-Za-z]+)\s+(\d+):(\d+)(?:-(\d+))?\b',  # Book Chapter:Verse(-Verse)
            r'\b([1-3]?\s*[A-Za-z]+)\s+(\d+)\b',  # Book Chapter
            r'\b(Matthew|Mark|Luke|John|Acts|Romans|Corinthians|Galatians|Ephesians|Philippians|Colossians|Thessalonians|Timothy|Titus|Philemon|Hebrews|James|Peter|John|Jude|Revelation)\s+(\d+):(\d+)(?:-(\d+))?\b'
        ]

    def _get_entry_field(self, entry, field: str):
        """Helper to get field from either object or dict format."""
        return getattr(entry, field) if hasattr(entry, field) else entry[field]

    def extract_subjects(self, text: str) -> Set[str]:
        """Extract key subject areas from text."""
        text_lower = text.lower()
        found_subjects = set()
        
        for subject, keywords in self.subject_keywords.items():
            for keyword in keywords:
                if keyword in text_lower:
                    found_subjects.add(subject)
        
        return found_subjects

    def extract_scripture_references(self, text: str) -> List[str]:
        """Extract scripture references from text."""
        references = []
        
        for pattern in self.scripture_patterns:
            matches = re.finditer(pattern, text, re.IGNORECASE)
            for match in matches:
                reference = match.group(0).strip()
                if reference not in references:
                    references.append(reference)
        
        return references

    def analyze_teaching_context(self, title: str, description: str, playlist_title: str) -> Dict:
        """Analyze the teaching context and extract metadata."""
        combined_text = f"{title} {description} {playlist_title}"
        
        # Determine teaching type
        teaching_type = "sermon"
        if "interview" in combined_text.lower():
            teaching_type = "interview"
        elif "lecture" in combined_text.lower() or "seminary" in combined_text.lower():
            teaching_type = "lecture"
        elif "conference" in combined_text.lower():
            teaching_type = "conference"
        elif "conversation" in combined_text.lower():
            teaching_type = "conversation"
        
        # Extract date information
        date_patterns = [
            r'\b(19|20)\d{2}\b',  # Year
            r'\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+(19|20)\d{2}\b',
            r'\b\d{1,2}/\d{1,2}/(19|20)\d{2}\b'
        ]
        
        dates_found = []
        for pattern in date_patterns:
            matches = re.findall(pattern, combined_text, re.IGNORECASE)
            dates_found.extend(matches)
        
        # Extract series information
        series_name = playlist_title
        if ":" in title:
            potential_series = title.split(":")[0].strip()
            if len(potential_series) > 3:
                series_name = potential_series
        
        return {
            'teaching_type': teaching_type,
            'series_name': series_name,
            'dates_mentioned': dates_found,
            'subjects': list(self.extract_subjects(combined_text)),
            'scripture_references': self.extract_scripture_references(combined_text)
        }

    def process_transcript_content(self, transcript: List[Dict], metadata: Dict) -> Dict:
        """Process transcript content for enhanced analysis."""
        if not transcript:
            return {}
        
        # Combine all transcript text
        full_text = " ".join([self._get_entry_field(entry, 'text') for entry in transcript])
        
        # Extract subjects and scripture from transcript content
        subjects_in_content = self.extract_subjects(full_text)
        scripture_in_content = self.extract_scripture_references(full_text)
        
        # Analyze speaker patterns
        speakers_found = set()
        for entry in transcript:
            text = self._get_entry_field(entry, 'text')
            speaker, _ = self._extract_speaker_and_text(text)
            if speaker:
                speakers_found.add(speaker)
        
        # Calculate content stats
        total_words = len(full_text.split())
        duration_minutes = self._get_entry_field(transcript[-1], 'start') / 60 if transcript else 0
        
        # Create content chunks for vector database
        chunks = self._create_content_chunks(transcript, metadata)
        
        return {
            'content_analysis': {
                'total_words': total_words,
                'duration_minutes': round(duration_minutes, 2),
                'speakers_identified': list(speakers_found),
                'subjects_in_content': list(subjects_in_content),
                'scripture_in_content': scripture_in_content,
                'chunk_count': len(chunks)
            },
            'chunks': chunks
        }

    def _create_content_chunks(self, transcript: List[Dict], metadata: Dict, chunk_size: int = 1000) -> List[Dict]:
        """Create content chunks for vector database processing."""
        chunks = []
        current_chunk = ""
        current_start_time = 0
        chunk_number = 1
        
        for entry in transcript:
            text = self._get_entry_field(entry, 'text')
            start_time = self._get_entry_field(entry, 'start')
            speaker, content = self._extract_speaker_and_text(text)
            
            # Add to current chunk
            if speaker:
                addition = f"[{self._format_timestamp(start_time)}] {speaker}: {content}\n"
            else:
                addition = f"[{self._format_timestamp(start_time)}] {content}\n"
            
            # Check if adding this would exceed chunk size
            if len(current_chunk) + len(addition) > chunk_size and current_chunk:
                # Save current chunk
                chunks.append({
                    'chunk_id': f"{metadata['video_id']}_chunk_{chunk_number:03d}",
                    'content': current_chunk.strip(),
                    'start_timestamp': current_start_time,
                    'end_timestamp': start_time,
                    'metadata': {
                        'video_id': metadata['video_id'],
                        'title': metadata['title'],
                        'series': metadata.get('teaching_context', {}).get('series_name', ''),
                        'chunk_number': chunk_number,
                        'subjects': list(self.extract_subjects(current_chunk)),
                        'scripture_refs': self.extract_scripture_references(current_chunk)
                    }
                })
                
                # Start new chunk
                current_chunk = addition
                current_start_time = start_time
                chunk_number += 1
            else:
                current_chunk += addition
                if not current_chunk.strip():  # First entry in chunk
                    current_start_time = start_time
        
        # Add final chunk if there's content
        if current_chunk.strip():
            chunks.append({
                'chunk_id': f"{metadata['video_id']}_chunk_{chunk_number:03d}",
                'content': current_chunk.strip(),
                'start_timestamp': current_start_time,
                'end_timestamp': self._get_entry_field(transcript[-1], 'start') if transcript else current_start_time,
                'metadata': {
                    'video_id': metadata['video_id'],
                    'title': metadata['title'],
                    'series': metadata.get('teaching_context', {}).get('series_name', ''),
                    'chunk_number': chunk_number,
                    'subjects': list(self.extract_subjects(current_chunk)),
                    'scripture_refs': self.extract_scripture_references(current_chunk)
                }
            })
        
        return chunks

    def save_enhanced_transcript(self, video_url: str, playlist_info: Dict, base_dir: str = "dallas_willard_transcripts") -> Optional[Dict]:
        """
        Save transcript with enhanced metadata and analysis.
        """
        video_id = self._extract_video_id(video_url)
        if not video_id:
            return None

        transcript = self.get_transcript(video_url)
        if not transcript:
            return None

        # Get basic metadata
        metadata = self._get_video_metadata(video_id, transcript)
        
        # Add playlist context
        metadata['playlist'] = {
            'playlist_id': playlist_info['playlist_id'],
            'playlist_title': playlist_info['title'],
            'playlist_description': playlist_info.get('description', '')
        }
        
        # Analyze teaching context
        teaching_context = self.analyze_teaching_context(
            metadata['title'],
            playlist_info.get('description', ''),
            playlist_info['title']
        )
        metadata['teaching_context'] = teaching_context
        
        # Process transcript content
        content_analysis = self.process_transcript_content(transcript, metadata)
        metadata.update(content_analysis)
        
        # Create organized file structure
        series_name = self._sanitize_filename(teaching_context['series_name'])
        safe_title = self._sanitize_filename(metadata['title'])
        
        # Create directory structure: base_dir/series_name/
        series_dir = os.path.join(base_dir, series_name)
        os.makedirs(series_dir, exist_ok=True)
        
        # Save enhanced transcript
        filename = f"{safe_title}_{video_id}.txt"
        filepath = os.path.join(series_dir, filename)
        
        transcript_content = self._create_enhanced_transcript_content(transcript, metadata)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(transcript_content)
        
        # Save metadata separately for easy processing
        metadata_filename = f"{safe_title}_{video_id}_metadata.json"
        metadata_filepath = os.path.join(series_dir, metadata_filename)
        
        with open(metadata_filepath, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)
        
        # Update master knowledge base index
        self._update_knowledge_base_index(metadata, filepath, base_dir)
        
        print(f"Enhanced transcript saved: {filepath}")
        return metadata

    def _create_enhanced_transcript_content(self, transcript: List[Dict], metadata: Dict) -> str:
        """Create enhanced transcript content with rich metadata."""
        content = f"""Title: {metadata['title']}
Video URL: {metadata['url']}
Series: {metadata['teaching_context']['series_name']}
Teaching Type: {metadata['teaching_context']['teaching_type']}
Playlist: {metadata['playlist']['playlist_title']}
Channel: {metadata['channel']['channel_name']}
Key Subjects: {', '.join(metadata['teaching_context']['subjects'])}
Scripture References: {', '.join(metadata['teaching_context']['scripture_references'])}
Duration: {metadata['content_analysis']['duration_minutes']} minutes
Total Words: {metadata['content_analysis']['total_words']}
Speakers Identified: {', '.join(metadata['content_analysis']['speakers_identified'])}
Has Speaker Labels: {metadata['has_speaker_labels']}
Downloaded: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

{'='*80}

"""
        
        for entry in transcript:
            timestamp = self._format_timestamp(self._get_entry_field(entry, 'start'))
            speaker, text = self._extract_speaker_and_text(self._get_entry_field(entry, 'text'))
            
            if speaker:
                content += f"[{timestamp}] {speaker}: {text}\n"
            else:
                content += f"[{timestamp}] {text}\n"
        
        return content

    def _update_knowledge_base_index(self, metadata: Dict, filepath: str, base_dir: str) -> None:
        """Update the master knowledge base index."""
        index_path = os.path.join(base_dir, "knowledge_base_index.json")
        
        try:
            if os.path.exists(index_path):
                with open(index_path, 'r', encoding='utf-8') as f:
                    index_data = json.load(f)
            else:
                index_data = {
                    'last_updated': '',
                    'total_transcripts': 0,
                    'total_chunks': 0,
                    'series': {},
                    'subjects': {},
                    'scripture_index': {},
                    'teaching_types': {}
                }
            
            # Update series tracking
            series_name = metadata['teaching_context']['series_name']
            if series_name not in index_data['series']:
                index_data['series'][series_name] = []
            
            index_data['series'][series_name].append({
                'video_id': metadata['video_id'],
                'title': metadata['title'],
                'filepath': os.path.relpath(filepath, base_dir),
                'subjects': metadata['teaching_context']['subjects'],
                'duration_minutes': metadata['content_analysis']['duration_minutes']
            })
            
            # Update subject tracking
            for subject in metadata['teaching_context']['subjects']:
                if subject not in index_data['subjects']:
                    index_data['subjects'][subject] = []
                index_data['subjects'][subject].append(metadata['video_id'])
            
            # Update scripture index
            for ref in metadata['teaching_context']['scripture_references']:
                if ref not in index_data['scripture_index']:
                    index_data['scripture_index'][ref] = []
                index_data['scripture_index'][ref].append(metadata['video_id'])
            
            # Update teaching type tracking
            teaching_type = metadata['teaching_context']['teaching_type']
            if teaching_type not in index_data['teaching_types']:
                index_data['teaching_types'][teaching_type] = []
            index_data['teaching_types'][teaching_type].append(metadata['video_id'])
            
            # Update totals
            index_data['total_transcripts'] = sum(len(series) for series in index_data['series'].values())
            index_data['total_chunks'] += metadata['content_analysis']['chunk_count']
            index_data['last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Save updated index
            with open(index_path, 'w', encoding='utf-8') as f:
                json.dump(index_data, f, indent=2, ensure_ascii=False)
        
        except Exception as e:
            print(f"Error updating knowledge base index: {e}")

    def process_all_playlists(self, playlists_file: str = "dallas_willard_playlists.json", base_dir: str = "dallas_willard_transcripts") -> Dict:
        """Process all playlists from the extracted playlist data."""
        with open(playlists_file, 'r', encoding='utf-8') as f:
            playlist_data = json.load(f)
        
        results = {
            'processed_videos': 0,
            'failed_videos': 0,
            'playlists_processed': 0,
            'series_created': set(),
            'total_subjects': set(),
            'processing_log': []
        }
        
        for playlist in playlist_data['playlists']:
            print(f"\n{'='*60}")
            print(f"Processing playlist: {playlist['title']}")
            print(f"{'='*60}")
            
            playlist_id = playlist['playlist_id']
            
            try:
                # Get videos from playlist
                video_ids = list(self._get_playlist_videos(playlist_id))
                print(f"Found {len(video_ids)} videos in playlist")
                
                for i, video_id in enumerate(video_ids, 1):
                    video_url = f"https://www.youtube.com/watch?v={video_id}"
                    print(f"Processing video {i}/{len(video_ids)}: {video_id}")
                    
                    try:
                        metadata = self.save_enhanced_transcript(video_url, playlist, base_dir)
                        if metadata:
                            results['processed_videos'] += 1
                            results['series_created'].add(metadata['teaching_context']['series_name'])
                            results['total_subjects'].update(metadata['teaching_context']['subjects'])
                            results['processing_log'].append(f"✓ {metadata['title']}")
                        else:
                            results['failed_videos'] += 1
                            results['processing_log'].append(f"✗ Failed: {video_id}")
                    
                    except Exception as e:
                        print(f"Error processing video {video_id}: {e}")
                        results['failed_videos'] += 1
                        results['processing_log'].append(f"✗ Error {video_id}: {str(e)}")
                    
                    # Rate limiting
                    self._rate_limit_wait()
                
                results['playlists_processed'] += 1
                
            except Exception as e:
                print(f"Error processing playlist {playlist['title']}: {e}")
                results['processing_log'].append(f"✗ Playlist Error: {playlist['title']}: {str(e)}")
        
        # Save processing results
        results['series_created'] = list(results['series_created'])
        results['total_subjects'] = list(results['total_subjects'])
        
        results_file = os.path.join(base_dir, "processing_results.json")
        with open(results_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        
        return results

    # Include all the utility methods from the original Transcript_Fetcher
    def _sanitize_filename(self, name: str) -> str:
        """Convert a string into a valid filename/directory name."""
        name = re.sub(r'[<>:"/\\|?*]', '', name)
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
        """Extract speaker and text from a transcript line."""
        for pattern in self.speaker_patterns:
            match = re.match(pattern, text.strip())
            if match:
                speaker, content = match.groups()
                return speaker.strip(), content.strip()
        
        return None, text.strip()

    def _check_for_speakers(self, transcript: List[Dict]) -> bool:
        """Check if the transcript contains any speaker labels."""
        for entry in transcript:
            speaker, _ = self._extract_speaker_and_text(self._get_entry_field(entry, 'text'))
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

    def _get_playlist_videos(self, playlist_id: str):
        """Get video IDs from a playlist using basic HTML parsing."""
        try:
            playlist_url = f"https://www.youtube.com/playlist?list={playlist_id}"
            response = requests.get(playlist_url)
            
            if response.status_code == 200:
                video_ids = re.findall(r'watch\?v=([a-zA-Z0-9_-]{11})', response.text)
                seen = set()
                
                for video_id in video_ids:
                    if video_id not in seen:
                        seen.add(video_id)
                        yield video_id
                        
        except Exception as e:
            print(f"Error fetching playlist videos: {e}")

    def _get_video_metadata(self, video_id: str, transcript: List[Dict]) -> Dict:
        """Get basic video metadata using oEmbed and check for speaker labels."""
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
        """Fetch transcript for a given YouTube video URL."""
        try:
            video_id = self._extract_video_id(video_url)
            if not video_id:
                print(f"Error: Could not extract video ID from URL: {video_url}")
                return None

            self._rate_limit_wait()
            # Use the correct API method for this version
            transcript_list = YouTubeTranscriptApi().list(video_id)
            transcript = transcript_list.find_transcript(['en']).fetch()
            return transcript

        except Exception as e:
            print(f"Error fetching transcript for {video_url}: {str(e)}")
            return None

def main():
    if len(sys.argv) < 2:
        print("Usage: python enhanced_transcript_processor.py <command> [options]")
        print("\nCommands:")
        print("  process_all - Process all playlists from dallas_willard_playlists.json")
        print("  single <video_url> - Process a single video")
        print("\nExample:")
        print("  python enhanced_transcript_processor.py process_all")
        return
    
    command = sys.argv[1]
    processor = DallasWillardTranscriptProcessor(rate_limit_per_minute=20)
    
    if command == "process_all":
        print("Starting comprehensive Dallas Willard transcript processing...")
        results = processor.process_all_playlists()
        
        print(f"\n{'='*60}")
        print("PROCESSING COMPLETE!")
        print(f"{'='*60}")
        print(f"✓ Processed videos: {results['processed_videos']}")
        print(f"✗ Failed videos: {results['failed_videos']}")
        print(f"📁 Series created: {len(results['series_created'])}")
        print(f"🏷️ Subjects identified: {len(results['total_subjects'])}")
        print(f"📂 Playlists processed: {results['playlists_processed']}")
        
        print(f"\nSeries created:")
        for series in results['series_created']:
            print(f"  - {series}")
        
        print(f"\nSubjects identified:")
        for subject in sorted(results['total_subjects']):
            print(f"  - {subject}")
    
    elif command == "single" and len(sys.argv) > 2:
        video_url = sys.argv[2]
        # For single video, we need playlist info - using a default
        playlist_info = {
            'playlist_id': 'single',
            'title': 'Single Video Processing',
            'description': ''
        }
        
        metadata = processor.save_enhanced_transcript(video_url, playlist_info)
        if metadata:
            print(f"Successfully processed: {metadata['title']}")
        else:
            print("Failed to process video")
    
    else:
        print("Invalid command. Use 'process_all' or 'single <video_url>'")

if __name__ == "__main__":
    main()