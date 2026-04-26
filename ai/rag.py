import os
import threading
from xmlrpc import client
from database.connection import get_connection
import streamlit as st
from llama_index.core import StorageContext, load_index_from_storage
from llama_index.core import VectorStoreIndex
from llama_index.readers.database import DatabaseReader
from ollama import Client
from config.settings import ENV_CONFIG, DB_URI

def load_and_index_from_db():
    """Load data from PostgreSQL and create index."""

    ollamaclient = Client(host=ENV_CONFIG['OLLAMA_URL'])

    dummy_query_vector = ollamaclient.embeddings(model="nomic-embed-text", prompt="initial load")["embedding"]

    db_reader = DatabaseReader(uri=DB_URI)

    # 1. Change the question to an Embedding
#    q_res = ollamaclient.embed(model="nomic-embed-text", prompt=question)
#    q_vector = q_res["embedding"]
    
    query = """
        SELECT 
            concat('Security: ', s.securities_name, ' - Price: ', h.close, ' - Date: ', h.date) as text
        FROM securities s
        JOIN historical_prices h ON s.id = h.security_id
        LIMIT 10;
    """

    # 2. Connect to the database and search (Similarity Search)
    # We search in the transactions for the 5 most relevant transactions
#    conn = get_connection()
#    cur = conn.cursor()

    search_query = """
        SELECT  DISTINCT t.transactions_id, 
                t.date, 
                t.description, 
                CAST(t.total_amount AS DECIMAL(15,2)) as total_amount,
                t.accounts_id,
                a.accounts_name,
                a.accounts_type,
                curr.currencies_shortname,
                (SELECT COALESCE(Payees_Name, 'UNKNOWN') FROM Payees WHERE Payees_Id = t.payees_id) as payee_name,
                COALESCE(
                    (WITH RECURSIVE CategoryHierarchy AS (
                        SELECT Categories_Id, Categories_Name::TEXT as Full_Path
                        FROM Categories 
                        WHERE Categories_Id_Parent IS NULL
                        UNION ALL
                        SELECT c1.Categories_Id, ch.Full_Path || ' : ' || c1.Categories_Name
                        FROM Categories c1
                        JOIN CategoryHierarchy ch ON c1.Categories_Id_Parent = ch.Categories_Id
                    )
                    SELECT Full_Path
                    FROM CategoryHierarchy
                    WHERE Categories_Id = s.Categories_Id 
                    LIMIT 1 
                    ), 
                    'Money Transfer'
                ) as category_path
        FROM    Transactions t,
                Splits s,
                Accounts a,
                Currencies curr
        WHERE   s.transactions_id = t.transactions_id
        AND     a.accounts_id = t.accounts_id
        AND     curr.currencies_id = a.currencies_id
        AND		t.total_amount <> 0
        AND		s.Amount <> 0
        AND		ABS(s.Amount) = (SELECT MAX(ABS(Amount)) FROM Splits WHERE transactions_id = t.transactions_id)
        ORDER BY embedding <=> %s::vector
        LIMIT 10;
    """

 #   cur.execute(search_query, (q_vector,))
 #   rows = cur.fetchall()

    # 3. Context creation for the Llama
 #   context = "\n".join([f"Amount: {r[1]}, Description: {r[0]}, Date: {r[2]}" for r in rows])

    docs = db_reader.load_data(query=search_query, query_params=(dummy_query_vector,))
    
 #   docs = db_reader.load_data(query=search_query, query_params=(q_vector,))
    if not docs:
        raise ValueError("No data found in database.")
    
    index = VectorStoreIndex.from_documents(docs, show_progress=True)
#    index = VectorStoreIndex.from_vector_store(docs, show_progress=True)
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