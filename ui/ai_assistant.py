import os
import threading
import streamlit as st
from streamlit.runtime.scriptrunner import add_script_run_ctx
from langchain_community.callbacks.streamlit import StreamlitCallbackHandler
from langchain_community.chat_message_histories import StreamlitChatMessageHistory
from langchain_core.messages import HumanMessage, AIMessage
from ai.rag import run_indexing_thread
from ai.web_search import web_search
from ai.update_vector import update_all_embeddings
from config.settings import ENV_CONFIG

def run_ai_assistant(user_input: str, llm, agent_with_history, rag_engine) -> str:
    """Route question to appropriate handler."""
    import datetime as dt_lib
    full_date = dt_lib.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    full_prompt = f"Today is {full_date}. {user_input}"
    
    # First, check if this is a general question that doesn't need SQL
    # This prevents the agent from trying to parse natural language responses
    
    # Simple check for general greetings and thank you messages
    general_phrases = ['thank', 'thanks', 'hello', 'hi', 'hey', 'good morning', 
                       'good afternoon', 'good evening', 'what can you do', 
                       'help me', 'how are you', 'nice to meet']
    
    is_general = any(phrase in user_input.lower() for phrase in general_phrases)
    
    if is_general:
        st.info("🤖 Answering directly...")
        response = llm.invoke(full_prompt).content
        return response
    
    # Classify the question for more complex queries
    try:
        decision = llm.invoke(
            f"Today is {full_date}. Classify this question into exactly one category:\n"
            f"- SQL   → needs data from a personal finance database (requires SELECT queries)\n"
            f"- WEB   → needs current market data, news, or external financial info\n"
            f"- GENERAL → everything else (conversation, greetings, analysis requests)\n\n"
            f"Question: {user_input}\n"
            f"Answer with ONLY one word: SQL, WEB, or GENERAL."
        ).content.strip().upper()
    except Exception as e:
        # If classification fails, default to GENERAL
        decision = "GENERAL"
    
    if "SQL" in decision:
        st.info("🗄️ Querying your database...")
        try:
            st_callback = StreamlitCallbackHandler(st.container())
            config = {"configurable": {"session_id": "Personal_Finance"}, "callbacks": [st_callback]}
            
            # Add a system message to guide the agent
            enhanced_prompt = f"""{full_prompt}

Remember: You MUST respond using the JSON action format when querying the database.
If you don't need to query the database, just provide a direct answer.

For database queries, use this format:
Thought: [your reasoning]
Action:
{{
  "action": "sql_db_query",
  "action_input": "SELECT ..."
}}

For direct answers without database queries, just provide the answer normally."""
            
            response = agent_with_history.invoke({"input": enhanced_prompt}, config=config)
            return response["output"]
        except Exception as e:
            # If agent fails, fall back to direct LLM
            st.warning(f"Agent error, falling back to direct response: {str(e)[:100]}")
            return llm.invoke(full_prompt).content
    
    elif "WEB" in decision:
        st.info("🌐 Searching the web...")
        web_results = web_search(user_input)
        synthesis_prompt = (
            f"Today is {full_date}.\n"
            f"The user asked: {user_input}\n\n"
            f"Here are web search results:\n{web_results}\n\n"
            f"Please summarise the relevant information and answer the user's question clearly."
        )
        return llm.invoke(synthesis_prompt).content
    
    else:
        st.info("🤖 Answering directly...")
        return llm.invoke(full_prompt).content


def render_ai_assistant(llm, agent_with_history, rag_engine):
    """Render the AI Assistant page."""
    st.title("🧠 AI Assistant")
    st.caption(f"Powered by {ENV_CONFIG['ollama_model']} · Fully local · Your data stays on your Pi")
    
    # Add helpful examples
    with st.expander("📋 Example Questions"):
        st.markdown("""
        **Database Questions (SQL):**
        - Show me all my bank transactions from last month
        - What's my current net worth?
        - List all my investments in my brokerage account
        - Show me my expenses by category for this year
        
        **Web Search Questions:**
        - What's the current price of AAPL stock?
        - Latest news about the stock market
        - Latest news about the crypto market
        - EUR/USD exchange rate today
        
        **General Questions:**
        - What can you help me with?
        - Analyze my investment portfolio
        - Analyze my income and expense transactions
        - Give me financial advice based on my data
        """)
    
    #persist_dir = "/app/storage_rag"
    persist_dir=ENV_CONFIG['persist_dir']
    docstore_path = os.path.join(persist_dir, "docstore.json")
    
    st.button("🔄 Update Vendor Embeddings (if you added new data)", on_click=update_all_embeddings)

    if not os.path.exists(docstore_path):
        if st.session_state['rag_status'] == 'idle':
            if st.button("🚀 Start Indexing (Background)"):
                st.session_state['rag_status'] = 'running'

                thread = threading.Thread(target=run_indexing_thread)
                add_script_run_ctx(thread) # Αυτό επιτρέπει στο thread να βλέπει το st.session_state
                thread.start()

            #    thread = threading.Thread(target=run_indexing_thread)
            #    thread.start()
                st.rerun()
        
        if st.session_state['rag_status'] == 'running':
            st.info("⏳ RAG Indexing running in background... You can use the rest of the application.")
    elif st.session_state['rag_status'] == 'ready' or os.path.exists(docstore_path):
        st.success("✅ RAG is ready!")
    
    col1, col2 = st.columns([6, 1])
    with col2:
        if st.button("🗑️ Clear chat"):
            msgs = StreamlitChatMessageHistory(key="sql_agent_history")
            msgs.clear()
            st.rerun()
    
    msgs = StreamlitChatMessageHistory(key="sql_agent_history")
    for msg in msgs.messages:
        st.chat_message(msg.type).write(msg.content)
    
    user_input = st.chat_input("Ask about your finances, holdings, transactions, or market news...")
    
    if user_input:
        st.chat_message("user").write(user_input)
        with st.chat_message("assistant"):
            with st.spinner("Thinking..."):
                try:
                    answer = run_ai_assistant(user_input, llm, agent_with_history, rag_engine)
                    st.write(answer)
                    
                    # Save to history
                    msgs.add_user_message(user_input)
                    msgs.add_ai_message(answer)
                except Exception as e:
                    error_msg = f"Sorry, I encountered an error: {str(e)[:200]}"
                    st.error(error_msg)
                    msgs.add_user_message(user_input)
                    msgs.add_ai_message(error_msg)