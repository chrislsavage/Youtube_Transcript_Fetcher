# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## PROJECT STATUS: Dallas Willard Knowledge Base (In Progress)

**Current Branch:** `dallas-willard-knowledge-base`  
**Status:** 71% Complete - Resume Processing Tomorrow  
**Last Updated:** January 23, 2025

## Project Overview

Enhanced YouTube transcript processing system specifically designed for creating a comprehensive Dallas Willard knowledge base with AI-ready metadata, subject analysis, and vector database preparation.

## Core Architecture

### Main Components
- `enhanced_transcript_processor.py`: Advanced processor with subject analysis and chunking
- `batch_processor.py`: Large-scale processing with resume capability  
- `playlist_extractor.py`: YouTube Data API integration for playlist discovery
- `check_status.py`: Processing status monitoring
- `Transcript_Fetcher.py`: Original basic transcript fetcher (legacy)

## Current Status & Resume Instructions

### Processing Progress
- ✅ 20/28 playlists completed (71%)
- ✅ 6 videos successfully processed with full metadata
- ✅ 4 teaching series created
- ✅ 263 vector-ready content chunks generated
- ⏸️ Paused due to YouTube IP rate limiting (normal)

### Resume Processing (After 24-48 hours)
```bash
cd "/path/to/Youtube Transcripts"
git checkout dallas-willard-knowledge-base
source venv/bin/activate
python batch_processor.py resume
```

### Check Current Status
```bash
python check_status.py
```

## Enhanced Features

### Subject Analysis
Automatically identifies Dallas Willard's key teaching themes:
- discipleship, spiritual_formation, kingdom_of_god, prayer, hearing_god
- spiritual_disciplines, philosophy, soul, will_of_god, righteousness
- scripture, church, apologetics, teaching

### Metadata Extraction
- **Teaching Context**: sermon, interview, lecture, conversation
- **Series Information**: Extracted from titles and descriptions
- **Scripture References**: Automatic Bible verse detection
- **Speaker Identification**: Enhanced patterns for Dallas, John Ortberg, etc.

### Vector Database Preparation
- **Content Chunking**: 1000-word chunks with metadata
- **Semantic Ready**: Prepared for OpenAI/Anthropic embeddings
- **Rich Context**: Each chunk includes subjects, timestamps, references

## Directory Structure

```
dallas_willard_transcripts/
├── [Series-Name]/
│   ├── [Title]_[VideoID].txt (enhanced transcript)
│   └── [Title]_[VideoID]_metadata.json (rich metadata)
├── knowledge_base_index.json (master index)
└── processing_results.json (completion stats)
```

## Key Data Files

- **dallas_willard_playlists.json**: 28 playlists (320+ videos) from YouTube Data API
- **processing_progress.json**: Resume state and progress tracking
- **knowledge_base_index.json**: Master index with series, subjects, scripture refs

## Dependencies

### Python Environment
```bash
python3 -m venv venv
source venv/bin/activate
pip install youtube-transcript-api requests
```

### YouTube Data API
- Requires API key from Google Cloud Console
- Used for playlist discovery and metadata

## Usage Commands

### Main Processing Commands
```bash
# Resume processing (primary command)
python batch_processor.py resume

# Process all (use carefully - may hit rate limits)
python batch_processor.py full

# Small batch test
python batch_processor.py small

# Single video processing  
python enhanced_transcript_processor.py single <video_url>
```

### Monitoring Commands
```bash
# Check processing status
python check_status.py

# Extract playlists (already done)
python playlist_extractor.py <API_KEY> @dallaswillard
```

## Rate Limiting & Recovery

### YouTube API Limits
- Expect IP blocking after ~200 requests
- Wait 24-48 hours before resuming
- All progress is saved and resumable

### Error Handling
- Graceful failure handling with detailed logging
- Automatic progress saving for resume capability
- Comprehensive status reporting

## Next Steps After Completion

1. **Vector Database**: Convert chunks to embeddings for RAG
2. **Search Interface**: Build semantic search capabilities  
3. **Subject Analysis**: Generate topic graphs and connections
4. **Content Creation**: AI-assisted writing from Dallas Willard's teachings

## Important Files for Recovery

- **SESSION_RECOVERY.md**: Complete recovery instructions
- **resume_processing.md**: Detailed resume plan
- **processing_progress.json**: Current state for resume