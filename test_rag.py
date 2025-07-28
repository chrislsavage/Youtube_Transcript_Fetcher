#!/usr/bin/env python3
"""
Test script for Dallas Willard RAG system
Quick CLI testing without full Streamlit interface
"""

import os
import sys
from rag_implementation import DallasWillardRAG

def test_rag_system():
    """Test the RAG system with sample queries"""
    
    print("🚀 Testing Dallas Willard RAG System")
    print("=" * 60)
    
    # Check for OpenAI API key
    if not os.getenv("OPENAI_API_KEY"):
        print("❌ Error: OPENAI_API_KEY environment variable not set")
        print("Please set your OpenAI API key:")
        print("export OPENAI_API_KEY='your-api-key-here'")
        return False
    
    try:
        # Initialize RAG system
        print("🔧 Initializing RAG system...")
        rag = DallasWillardRAG()
        
        # Check if index exists
        if rag.collection.count() == 0:
            print("📚 No documents found. Indexing transcripts...")
            rag.load_and_index_transcripts()
        else:
            print(f"✅ Found {rag.collection.count():,} chunks already indexed")
        
        # Test questions
        test_questions = [
            "What does Dallas Willard teach about prayer?",
            "How does Dallas Willard define the Kingdom of God?",
            "What are the spiritual disciplines?",
            "What does Dallas Willard say about hearing God?",
            "How does Dallas Willard understand discipleship?"
        ]
        
        print("\n🔍 Testing search functionality...")
        
        for i, question in enumerate(test_questions[:2], 1):  # Test first 2 questions
            print(f"\n{'='*60}")
            print(f"Question {i}: {question}")
            print("-" * 60)
            
            # Test search only first
            search_results = rag.search(question, n_results=3)
            print(f"Found {len(search_results['results'])} relevant excerpts")
            
            for j, result in enumerate(search_results['results'][:2], 1):
                meta = result['metadata']
                content = result['content'][:200] + "..." if len(result['content']) > 200 else result['content']
                print(f"\n{j}. {meta['title']}")
                print(f"   Series: {meta['series']}")
                print(f"   Excerpt: {content}")
            
            # Test full RAG answer
            print(f"\n💬 Generating answer...")
            full_result = rag.ask(question, n_results=3)
            print(f"\nAnswer: {full_result['answer'][:300]}...")
        
        print(f"\n{'='*60}")
        print("✅ RAG system test completed successfully!")
        print("\nTo use the full interface:")
        print("1. pip install streamlit")
        print("2. streamlit run streamlit_rag_interface.py")
        
        return True
        
    except Exception as e:
        print(f"❌ Error testing RAG system: {e}")
        return False

def interactive_mode():
    """Interactive CLI mode for testing"""
    
    print("🚀 Dallas Willard RAG - Interactive Mode")
    print("Type 'quit' to exit\n")
    
    try:
        rag = DallasWillardRAG()
        
        if rag.collection.count() == 0:
            print("📚 Indexing transcripts (this may take a few minutes)...")
            rag.load_and_index_transcripts()
        
        print(f"✅ Ready! Knowledge base has {rag.collection.count():,} chunks")
        print("Ask any question about Dallas Willard's teachings...\n")
        
        while True:
            question = input("❓ Your question: ").strip()
            
            if question.lower() in ['quit', 'exit', 'q']:
                break
            
            if not question:
                continue
            
            print("🔍 Searching...")
            result = rag.ask(question, n_results=3)
            
            print(f"\n💬 Answer:")
            print(result['answer'])
            
            print(f"\n📚 Sources:")
            for i, source in enumerate(result['sources'][:2], 1):
                meta = source['metadata']
                print(f"  {i}. {meta['title']} (Series: {meta['series']})")
            
            print("\n" + "-" * 60 + "\n")
        
        print("👋 Goodbye!")
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "interactive":
        interactive_mode()
    else:
        test_rag_system()