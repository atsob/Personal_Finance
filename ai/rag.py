import os
import threading
import streamlit as st
from llama_index.core import StorageContext, load_index_from_storage
from llama_index.core import VectorStoreIndex
from llama_index.readers.database import DatabaseReader
from config.settings import ENV_CONFIG, DB_URI

def load_and_index_from_db():
    """Load data from PostgreSQL and create index."""
    db_reader = DatabaseReader(uri=DB_URI)
    
    query = """
        SELECT 
            concat('Security: ', s.security_name, ' - Price: ', h.price_close, ' - Date: ', h.price_date) as text
        FROM securities s
        JOIN historical_prices h ON s.id = h.security_id
        LIMIT 10;
    """
    
    docs = db_reader.load_data(query=query)
    if not docs:
        raise ValueError("No data found in database.")
    
    index = VectorStoreIndex.from_documents(docs, show_progress=True)
    return index

def init_rag_index():
    """Initialize or load RAG index."""
#    persist_dir = "/app/storage_rag"
    persist_dir=ENV_CONFIG['persist_dir']
    docstore_path = os.path.join(persist_dir, "docstore.json")
    
    if not os.path.exists(docstore_path):
        st.info("🔄 Creating new RAG Index from database...")
        index = load_and_index_from_db()
        os.makedirs(persist_dir, exist_ok=True)
        index.storage_context.persist(persist_dir=persist_dir)
        st.success("✅ Index saved successfully!")
    else:
        st.caption("📂 Loading RAG Index from disk...")
        storage_context = StorageContext.from_defaults(persist_dir=persist_dir)
        index = load_index_from_storage(storage_context)
    
    return index

def run_indexing_thread():
    """Run indexing in background thread."""
    try:
        index = init_rag_index()
        st.session_state['rag_status'] = 'ready'
        st.session_state['rag_index'] = index
        st.session_state['rag_engine'] = index.as_query_engine()
    except Exception as e:
        st.session_state['rag_status'] = f'error: {str(e)}'