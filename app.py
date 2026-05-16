import streamlit as st
from langchain_community.chat_message_histories import StreamlitChatMessageHistory
from langchain_core.runnables.history import RunnableWithMessageHistory
from langchain_core.chat_history import BaseChatMessageHistory

from config.settings import ENV_CONFIG
from utils.session_state import init_session_state
from utils.helpers import configure_warnings_and_ssl
from ai.llm import init_llm
from ai.rag import PgVectorRagEngine
from ai.agent import create_ai_agent
from database.connection import get_connection, get_sql_database
from ui.dashboard import render_dashboard
from ui.register import render_register
from ui.reports import render_reports
from ui.market_data import render_market_data
from ui.ai_assistant import render_ai_assistant
from ui.tools import render_tools
from ui.static_data import render_static_data

@st.cache_resource
def startup_db_maintenance():
    conn = get_connection()
    with conn.cursor() as cursor:
        cursor.execute("ANALYZE;")
    return True

def main():
    """Main application entry point."""
    # Page configuration
    st.set_page_config(page_title="Personal Finance", layout="wide")
    
    # Initialize
    startup_db_maintenance()
    configure_warnings_and_ssl()
    init_session_state()
    
    # Initialize LLM and database
    # Cache the SQLDatabase so the SQLAlchemy engine (and its connection pool)
    # is reused across Streamlit reruns rather than rebuilt on every interaction.
    @st.cache_resource
    def _cached_sql_database():
        return get_sql_database()

    llm = init_llm()
    db = _cached_sql_database()
    
    # Initialize chat history
    msgs = StreamlitChatMessageHistory(key="sql_agent_history")
    
    def get_session_history(session_id: str) -> BaseChatMessageHistory:
        return msgs
    
    rag_engine = PgVectorRagEngine()

    agent_executor = create_ai_agent(llm, db, rag_engine)
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
        #    "🥧 Investments (DEPRECIATED)",
            "⏳ Reports",
            "📋 Static Data",
            "🌍 Market Data",
            "🛠️ Tools",  
            "🧠 AI Assistant",                      
        ]
    )
    
    # Database connection for page rendering
    conn = get_connection()
    
    try:
        if menu == "🏛️ Dashboard":
            render_dashboard(conn)
        elif menu == "📝 Register":
        #    render_register(conn)
            render_register()
    #    elif menu == "🥧 Investments (DEPRECIATED)":
    #        render_investments(conn)
        elif menu == "⏳ Reports":
    #        render_reports(conn)
            render_reports()
        elif menu == "📋 Static Data":
            render_static_data()
        elif menu == "🌍 Market Data":
            render_market_data()
        elif menu == "🛠️ Tools":
            render_tools(conn)       
        elif menu == "🧠 AI Assistant":
            render_ai_assistant(llm, agent_with_history, rag_engine, db=db)            

    finally:
        conn.close()

if __name__ == "__main__":
    main()