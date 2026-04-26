import streamlit as st
from langchain_community.chat_message_histories import StreamlitChatMessageHistory
from langchain_community.agent_toolkits.sql.toolkit import SQLDatabaseToolkit
from langchain_community.agent_toolkits import create_sql_agent
from langchain_core.runnables.history import RunnableWithMessageHistory
from langchain_core.chat_history import BaseChatMessageHistory

from config.settings import ENV_CONFIG
from utils.session_state import init_session_state
from utils.helpers import configure_warnings_and_ssl
from ai.llm import init_llm
from ai.rag import init_rag_index
from ai.agent import create_ai_agent
from database.connection import get_connection, get_sql_database
from ui.dashboard import render_dashboard
from ui.register import render_register
from ui.investments import render_investments
from ui.reports import render_reports
from ui.market_data import render_market_data
from ui.ai_assistant import render_ai_assistant
from ui.settings import render_settings

def main():
    """Main application entry point."""
    # Page configuration
    st.set_page_config(page_title="Personal Finance", layout="wide")
    
    # Initialize
    configure_warnings_and_ssl()
    init_session_state()
    
    # Initialize LLM and database
    llm = init_llm()
    db = get_sql_database()
    
    # Initialize chat history
    msgs = StreamlitChatMessageHistory(key="sql_agent_history")
    
    def get_session_history(session_id: str) -> BaseChatMessageHistory:
        return msgs
    
    # Try to initialize RAG if available
    rag_engine = None
    try:
        import os
        from llama_index.core import StorageContext, load_index_from_storage
    #    persist_dir = "/app/storage_rag"
        persist_dir=ENV_CONFIG['persist_dir']
        docstore_path = os.path.join(persist_dir, "docstore.json")
        if os.path.exists(docstore_path):
            storage_context = StorageContext.from_defaults(persist_dir=persist_dir)
            index = load_index_from_storage(storage_context)
            rag_engine = index.as_query_engine()
            st.session_state['rag_ready'] = True
            st.session_state['rag_status'] = 'ready'
    except Exception as e:
        pass
    
    # Initialize agent if RAG is available
    if rag_engine:
        agent_executor = create_ai_agent(llm, db, rag_engine)
        agent_with_history = RunnableWithMessageHistory(
            agent_executor,
            get_session_history,
            input_messages_key="input",
            history_messages_key="chat_history",
        )
    else:
        # Create agent without RAG with better error handling
        toolkit = SQLDatabaseToolkit(db=db, llm=llm)
        agent_executor = create_sql_agent(
            llm=llm,
            db=db,
            agent_type="zero-shot-react-description",
            verbose=True,
            handle_parsing_errors=True,  # Critical
            max_iterations=3,
            early_stopping_method="generate",
            allow_dangerous_requests=True
        )        
        agent_with_history = RunnableWithMessageHistory(
            agent_executor,
            get_session_history,
            input_messages_key="input",
            history_messages_key="chat_history",
        )
    
    # Sidebar navigation
    st.sidebar.title("💰 Personal Finance")
    
    menu = st.sidebar.radio(
        "Menu", 
        [
            "🏛️ Dashboard",
            "📝 Register",
            "🥧 Investments (DEPRECIATED)",
            "⏳ Reports",
            "🌍 Market Data",
            "🧠 AI Assistant",
            "🔧 Settings"
        ]
    )
    
    # Database connection for page rendering
    conn = get_connection()
    
    try:
        if menu == "🏛️ Dashboard":
            render_dashboard(conn)
        elif menu == "📝 Register":
            render_register(conn)
        elif menu == "🥧 Investments (DEPRECIATED)":
            render_investments(conn)
        elif menu == "⏳ Reports":
            render_reports(conn)
        elif menu == "🌍 Market Data":
            render_market_data(conn)
        elif menu == "🧠 AI Assistant":
            render_ai_assistant(llm, agent_with_history, rag_engine)
        elif menu == "🔧 Settings":
            render_settings(conn)
    finally:
        conn.close()

if __name__ == "__main__":
    main()