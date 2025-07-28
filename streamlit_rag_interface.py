#!/usr/bin/env python3
"""
Streamlit interface for Dallas Willard RAG system
Simple web interface for querying the knowledge base
"""

import streamlit as st
import os
from rag_implementation import DallasWillardRAG
import json

# Page configuration
st.set_page_config(
    page_title="Dallas Willard Knowledge Base",
    page_icon="📚",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize session state
if 'rag_system' not in st.session_state:
    st.session_state.rag_system = None
if 'messages' not in st.session_state:
    st.session_state.messages = []

def initialize_rag():
    """Initialize the RAG system"""
    if st.session_state.rag_system is None:
        with st.spinner("Initializing Dallas Willard Knowledge Base..."):
            try:
                st.session_state.rag_system = DallasWillardRAG()
                return True
            except Exception as e:
                st.error(f"Failed to initialize RAG system: {e}")
                st.error("Make sure you have set your OPENAI_API_KEY environment variable")
                return False
    return True

def main():
    """Main Streamlit application"""
    
    # Header
    st.title("📚 Dallas Willard Knowledge Base")
    st.markdown("*Explore the teachings of Dallas Willard through AI-powered semantic search*")
    
    # Sidebar
    with st.sidebar:
        st.header("Search Options")
        
        # Number of results
        n_results = st.slider("Number of results", min_value=1, max_value=10, value=5)
        
        # Filter options
        st.subheader("Filter by:")
        
        # Series filter
        series_options = [
            "All Series",
            "Knowledge of Christ in Today's World", 
            "Kingdom Living",
            "Dallas Willard - Beyond Belief",
            "Renovaré Institute Series",
            "Philosophy and Apologetics",
            "The Divine Conspiracy",
            "Dallas Willard - Hearing God"
        ]
        selected_series = st.selectbox("Series", series_options)
        
        # Subject filter
        subject_options = [
            "All Subjects",
            "discipleship",
            "spiritual_formation", 
            "kingdom_of_god",
            "prayer",
            "hearing_god",
            "spiritual_disciplines",
            "philosophy",
            "soul",
            "will_of_god",
            "righteousness",
            "scripture",
            "church",
            "apologetics",
            "teaching"
        ]
        selected_subject = st.selectbox("Subject", subject_options)
        
        # Speaker filter
        speaker_options = ["All Speakers", "Dallas Willard", "John Ortberg", "Richard Foster"]
        selected_speaker = st.selectbox("Speaker", speaker_options)
        
        # Build filter criteria
        filter_criteria = {}
        if selected_series != "All Series":
            filter_criteria["series"] = selected_series
        if selected_subject != "All Subjects":
            filter_criteria["subjects"] = {"$contains": selected_subject}
        if selected_speaker != "All Speakers":
            filter_criteria["speaker"] = selected_speaker
        
        # System status
        st.subheader("System Status")
        if st.session_state.rag_system:
            try:
                count = st.session_state.rag_system.collection.count()
                st.success(f"✅ {count:,} chunks indexed")
            except:
                st.warning("⚠️ System initialized but database not accessible")
        else:
            st.warning("⚠️ System not initialized")
    
    # Initialize RAG system
    if not initialize_rag():
        st.stop()
    
    # Main interface
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("Ask a Question")
        
        # Sample questions
        sample_questions = [
            "What does Dallas Willard teach about prayer?",
            "How does Dallas Willard define the Kingdom of God?", 
            "What are the spiritual disciplines according to Dallas Willard?",
            "What does Dallas Willard say about hearing God?",
            "How does Dallas Willard understand discipleship?",
            "What is the role of the will in spiritual formation?",
            "How does Dallas Willard view the relationship between faith and reason?"
        ]
        
        # Quick question buttons
        st.write("**Quick Questions:**")
        cols = st.columns(3)
        for i, question in enumerate(sample_questions[:6]):
            if cols[i % 3].button(f"📝 {question[:30]}...", key=f"q_{i}"):
                st.session_state.messages.append({"role": "user", "content": question})
        
        # Chat interface
        question = st.chat_input("Ask about Dallas Willard's teachings...")
        
        if question:
            st.session_state.messages.append({"role": "user", "content": question})
        
        # Display chat messages
        for message in st.session_state.messages:
            with st.chat_message(message["role"]):
                if message["role"] == "user":
                    st.write(message["content"])
                else:
                    # Display answer
                    st.write(message["content"]["answer"])
                    
                    # Display sources
                    with st.expander(f"📚 Sources ({message['content']['n_sources']} relevant excerpts)"):
                        for i, source in enumerate(message["content"]["sources"], 1):
                            meta = source["metadata"]
                            st.write(f"**{i}. {meta['title']}**")
                            st.write(f"*Series: {meta['series']} | Speaker: {meta.get('speaker', 'Unknown')}*")
                            
                            # Show content excerpt
                            content = source["content"]
                            if len(content) > 300:
                                content = content[:300] + "..."
                            st.write(f"📄 {content}")
                            
                            # Show metadata
                            subjects = meta.get("subjects", [])
                            if subjects:
                                st.write(f"🏷️ **Subjects:** {', '.join(subjects)}")
                            
                            st.write("---")
        
        # Process new question
        if question and st.session_state.rag_system:
            with st.chat_message("assistant"):
                with st.spinner("Searching Dallas Willard's teachings..."):
                    # Apply filters
                    filter_dict = filter_criteria if filter_criteria else None
                    
                    # Get answer
                    result = st.session_state.rag_system.ask(
                        question, 
                        n_results=n_results,
                        filter_criteria=filter_dict
                    )
                    
                    # Display answer
                    st.write(result["answer"])
                    
                    # Display sources
                    with st.expander(f"📚 Sources ({result['n_sources']} relevant excerpts)"):
                        for i, source in enumerate(result["sources"], 1):
                            meta = source["metadata"]
                            st.write(f"**{i}. {meta['title']}**")
                            st.write(f"*Series: {meta['series']} | Speaker: {meta.get('speaker', 'Unknown')}*")
                            
                            # Show content excerpt
                            content = source["content"]
                            if len(content) > 300:
                                content = content[:300] + "..."
                            st.write(f"📄 {content}")
                            
                            # Show metadata
                            subjects = meta.get("subjects", [])
                            if subjects:
                                st.write(f"🏷️ **Subjects:** {', '.join(subjects)}")
                            
                            st.write("---")
                    
                    # Save to session
                    st.session_state.messages.append({"role": "assistant", "content": result})
    
    with col2:
        st.subheader("Knowledge Base Stats")
        
        if st.session_state.rag_system:
            try:
                # Load knowledge base stats
                index_path = "dallas_willard_transcripts/knowledge_base_index.json"
                if os.path.exists(index_path):
                    with open(index_path, 'r') as f:
                        kb_stats = json.load(f)
                    
                    st.metric("Total Transcripts", kb_stats.get("total_transcripts", 0))
                    st.metric("Total Chunks", kb_stats.get("total_chunks", 0))
                    st.metric("Series Count", len(kb_stats.get("series", {})))
                    
                    # Series breakdown
                    st.subheader("Series Overview")
                    for series_name, videos in kb_stats.get("series", {}).items():
                        if series_name != "Dallas Willard - Beyond Belief":  # Skip duplicate entries
                            st.write(f"📁 **{series_name}**: {len(videos)} videos")
            except Exception as e:
                st.error(f"Error loading stats: {e}")
        
        st.subheader("About")
        st.markdown("""
        This knowledge base contains **164 transcripts** from Dallas Willard's teachings, covering:
        
        - 🏰 **Kingdom of God** theology
        - 🙏 **Prayer** and hearing God  
        - 📖 **Spiritual Disciplines**
        - 👥 **Discipleship** and formation
        - 🧠 **Philosophy** and apologetics
        - ⛪ **Church** and ministry
        
        *Powered by AI embeddings and semantic search*
        """)
        
        # Clear chat button
        if st.button("🗑️ Clear Chat"):
            st.session_state.messages = []
            st.rerun()

if __name__ == "__main__":
    main()