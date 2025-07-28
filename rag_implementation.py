#!/usr/bin/env python3
"""
RAG Implementation for Dallas Willard Knowledge Base
Converts transcripts to vector embeddings and enables semantic search
"""

import json
import os
import re
from pathlib import Path
from typing import List, Dict, Any, Optional
import chromadb
from chromadb.config import Settings
import openai
from openai import OpenAI

class DallasWillardRAG:
    def __init__(self, 
                 transcripts_dir: str = "dallas_willard_transcripts",
                 chroma_persist_dir: str = "chroma_db",
                 openai_api_key: Optional[str] = None):
        """Initialize RAG system with ChromaDB and OpenAI embeddings"""
        
        self.transcripts_dir = Path(transcripts_dir)
        self.chroma_persist_dir = Path(chroma_persist_dir)
        
        # Initialize OpenAI client
        if openai_api_key:
            self.openai_client = OpenAI(api_key=openai_api_key)
        else:
            # Try environment variable
            self.openai_client = OpenAI()
        
        # Initialize ChromaDB
        self.chroma_client = chromadb.PersistentClient(
            path=str(self.chroma_persist_dir),
            settings=Settings(anonymized_telemetry=False)
        )
        
        # Create or get collection
        self.collection = self.chroma_client.get_or_create_collection(
            name="dallas_willard_teachings",
            metadata={"description": "Dallas Willard transcripts with rich metadata"}
        )
        
        print(f"Initialized RAG system with {self.collection.count()} documents")
    
    def chunk_transcript(self, transcript_text: str, chunk_size: int = 1000, overlap: int = 200) -> List[str]:
        """Split transcript into overlapping chunks for better context"""
        
        # Clean up the transcript text
        text = re.sub(r'\n+', ' ', transcript_text)
        text = re.sub(r'\s+', ' ', text).strip()
        
        chunks = []
        start = 0
        
        while start < len(text):
            end = start + chunk_size
            
            # If we're not at the end, try to break at sentence boundary
            if end < len(text):
                # Look for sentence endings within the last 200 characters
                last_period = text.rfind('.', start + chunk_size - 200, end)
                last_question = text.rfind('?', start + chunk_size - 200, end)
                last_exclamation = text.rfind('!', start + chunk_size - 200, end)
                
                sentence_end = max(last_period, last_question, last_exclamation)
                if sentence_end > start:
                    end = sentence_end + 1
            
            chunk = text[start:end].strip()
            if chunk:
                chunks.append(chunk)
            
            start = max(start + chunk_size - overlap, end - overlap)
        
        return chunks
    
    def load_and_index_transcripts(self, force_reindex: bool = False):
        """Load all transcripts and create vector embeddings"""
        
        if self.collection.count() > 0 and not force_reindex:
            print(f"Collection already has {self.collection.count()} documents")
            print("Use force_reindex=True to rebuild the index")
            return
        
        # Load knowledge base index
        index_path = self.transcripts_dir / "knowledge_base_index.json"
        if not index_path.exists():
            raise FileNotFoundError(f"Knowledge base index not found: {index_path}")
        
        with open(index_path, 'r') as f:
            knowledge_base = json.load(f)
        
        print(f"Loading {knowledge_base['total_transcripts']} transcripts...")
        
        # Clear existing collection if force reindexing
        if force_reindex and self.collection.count() > 0:
            self.chroma_client.delete_collection("dallas_willard_teachings")
            self.collection = self.chroma_client.create_collection(
                name="dallas_willard_teachings",
                metadata={"description": "Dallas Willard transcripts with rich metadata"}
            )
        
        documents = []
        metadatas = []
        ids = []
        chunk_id = 0
        
        # Process each series
        for series_name, videos in knowledge_base["series"].items():
            print(f"Processing series: {series_name}")
            
            for video in videos:
                # Load transcript text
                transcript_path = self.transcripts_dir / video["filepath"]
                metadata_path = transcript_path.with_suffix('_metadata.json')
                
                if not transcript_path.exists():
                    print(f"Warning: Transcript not found: {transcript_path}")
                    continue
                
                # Read transcript content
                with open(transcript_path, 'r', encoding='utf-8') as f:
                    transcript_text = f.read()
                
                # Load metadata if available
                metadata = {}
                if metadata_path.exists():
                    with open(metadata_path, 'r', encoding='utf-8') as f:
                        metadata = json.load(f)
                
                # Chunk the transcript
                chunks = self.chunk_transcript(transcript_text)
                
                # Create embeddings for each chunk
                for i, chunk in enumerate(chunks):
                    chunk_metadata = {
                        "video_id": video["video_id"],
                        "title": video["title"],
                        "series": series_name,
                        "subjects": video.get("subjects", []),
                        "chunk_index": i,
                        "total_chunks": len(chunks),
                        "filepath": video["filepath"],
                        "duration_minutes": video.get("duration_minutes", 0)
                    }
                    
                    # Add additional metadata if available
                    if metadata:
                        chunk_metadata.update({
                            "teaching_type": metadata.get("teaching_context", {}).get("teaching_type", ""),
                            "scripture_refs": metadata.get("teaching_context", {}).get("scripture_references", [])[:5],  # Limit to 5 refs
                            "channel": metadata.get("channel", {}).get("channel_name", ""),
                            "speaker": self._extract_speaker(video["title"])
                        })
                    
                    documents.append(chunk)
                    metadatas.append(chunk_metadata)
                    ids.append(f"chunk_{chunk_id}")
                    chunk_id += 1
                    
                    # Batch process in groups of 100
                    if len(documents) >= 100:
                        self._add_batch_to_collection(documents, metadatas, ids)
                        documents, metadatas, ids = [], [], []
        
        # Add remaining documents
        if documents:
            self._add_batch_to_collection(documents, metadatas, ids)
        
        print(f"Successfully indexed {self.collection.count()} chunks from Dallas Willard teachings")
    
    def _extract_speaker(self, title: str) -> str:
        """Extract primary speaker from video title"""
        if "Dallas Willard" in title:
            return "Dallas Willard"
        elif "John Ortberg" in title:
            return "John Ortberg"
        elif "Richard Foster" in title:
            return "Richard Foster"
        else:
            return "Unknown"
    
    def _add_batch_to_collection(self, documents: List[str], metadatas: List[Dict], ids: List[str]):
        """Add a batch of documents to ChromaDB with embeddings"""
        
        try:
            # Generate embeddings using OpenAI
            embeddings = self._generate_embeddings(documents)
            
            # Add to collection
            self.collection.add(
                documents=documents,
                metadatas=metadatas,
                ids=ids,
                embeddings=embeddings
            )
            
            print(f"Added batch of {len(documents)} chunks")
            
        except Exception as e:
            print(f"Error adding batch: {e}")
    
    def _generate_embeddings(self, texts: List[str]) -> List[List[float]]:
        """Generate embeddings using OpenAI's embedding model"""
        
        try:
            response = self.openai_client.embeddings.create(
                model="text-embedding-3-small",
                input=texts
            )
            return [data.embedding for data in response.data]
        except Exception as e:
            print(f"Error generating embeddings: {e}")
            raise
    
    def search(self, query: str, n_results: int = 5, filter_criteria: Optional[Dict] = None) -> Dict[str, Any]:
        """Search the knowledge base using semantic similarity"""
        
        try:
            # Generate embedding for query
            query_embedding = self._generate_embeddings([query])[0]
            
            # Search collection
            results = self.collection.query(
                query_embeddings=[query_embedding],
                n_results=n_results,
                where=filter_criteria
            )
            
            return {
                "query": query,
                "results": [
                    {
                        "content": doc,
                        "metadata": meta,
                        "distance": dist
                    }
                    for doc, meta, dist in zip(
                        results["documents"][0],
                        results["metadatas"][0], 
                        results["distances"][0]
                    )
                ]
            }
            
        except Exception as e:
            print(f"Error searching: {e}")
            return {"query": query, "results": []}
    
    def generate_answer(self, query: str, context_results: List[Dict], model: str = "gpt-4o-mini") -> str:
        """Generate answer using RAG with context from search results"""
        
        # Prepare context from search results
        context_pieces = []
        for result in context_results:
            meta = result["metadata"]
            context_pieces.append(
                f"From '{meta['title']}' (Series: {meta['series']}):\n{result['content']}\n"
            )
        
        context = "\n---\n".join(context_pieces)
        
        # Create RAG prompt
        prompt = f"""You are an expert on Dallas Willard's teachings about spiritual formation, the Kingdom of God, and Christian discipleship. Based on the provided context from Dallas Willard's transcripts, please answer the user's question accurately and thoughtfully.

Context from Dallas Willard's teachings:
{context}

Question: {query}

Please provide a comprehensive answer based on the context above. If the context doesn't fully address the question, acknowledge this and provide what insights you can from the available material. Include relevant quotes where helpful."""

        try:
            response = self.openai_client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": "You are an expert on Dallas Willard's teachings, helping people understand his insights on spiritual formation, discipleship, and Kingdom living."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                max_tokens=1500
            )
            
            return response.choices[0].message.content
            
        except Exception as e:
            return f"Error generating answer: {e}"
    
    def ask(self, question: str, n_results: int = 5, filter_criteria: Optional[Dict] = None) -> Dict[str, Any]:
        """Complete RAG pipeline: search + generate answer"""
        
        # Search for relevant content
        search_results = self.search(question, n_results, filter_criteria)
        
        # Generate answer using retrieved context
        answer = self.generate_answer(question, search_results["results"])
        
        return {
            "question": question,
            "answer": answer,
            "sources": search_results["results"],
            "n_sources": len(search_results["results"])
        }


def main():
    """Test the RAG implementation with sample queries"""
    
    # Initialize RAG system
    rag = DallasWillardRAG()
    
    # Load and index transcripts (only if not already indexed)
    rag.load_and_index_transcripts()
    
    # Test queries
    test_questions = [
        "What does Dallas Willard teach about prayer?",
        "How does Dallas Willard define the Kingdom of God?",
        "What are the spiritual disciplines according to Dallas Willard?",
        "What does Dallas Willard say about hearing God?",
        "How does Dallas Willard understand discipleship?"
    ]
    
    print("\n" + "="*80)
    print("TESTING DALLAS WILLARD RAG SYSTEM")
    print("="*80)
    
    for question in test_questions:
        print(f"\nQ: {question}")
        print("-" * 60)
        
        result = rag.ask(question, n_results=3)
        print(f"A: {result['answer']}")
        
        print(f"\nSources ({result['n_sources']} relevant chunks):")
        for i, source in enumerate(result['sources'][:2], 1):
            meta = source['metadata']
            print(f"  {i}. {meta['title']} (Series: {meta['series']})")
        
        print("\n" + "="*80)


if __name__ == "__main__":
    main()