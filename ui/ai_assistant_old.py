import os
import threading
import streamlit as st
from langchain_community.callbacks.streamlit import StreamlitCallbackHandler
from langchain_community.chat_message_histories import StreamlitChatMessageHistory
from ai.rag import run_indexing_thread
from ai.web_search import web_search
from config.settings import ENV_CONFIG

def run_ai_assistant(user_input: str, llm, agent_with_history, rag_engine) -> str:
    """Route question to appropriate handler."""
    import datetime as dt_lib
    full_date = dt_lib.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    full_prompt = f"Today is {full_date}. {user_input}"
    
    decision = llm.invoke(
        f"Today is {full_date}. Classify this question into exactly one category:\n"
        f"- SQL   → needs data from a personal finance database\n"
        f"- WEB   → needs current market data, news, or external financial info\n"
        f"- GENERAL → everything else\n\n"
        f"Question: {user_input}\n"
        f"Answer with ONLY one word: SQL, WEB, or GENERAL."
    ).content.strip().upper()
    
    if "SQL" in decision:
        st.info("🗄️ Querying your database...")
        st_callback = StreamlitCallbackHandler(st.container())
        config = {"configurable": {"session_id": "Personal_Finance"}, "callbacks": [st_callback]}
        response = agent_with_history.invoke({"input": full_prompt}, config=config)
        return response["output"]
    
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
    
    persist_dir = "/app/storage_rag"
    docstore_path = os.path.join(persist_dir, "docstore.json")
    
    if not os.path.exists(docstore_path):
        if st.session_state['rag_status'] == 'idle':
            if st.button("🚀 Start Indexing (Background)"):
                st.session_state['rag_status'] = 'running'
                thread = threading.Thread(target=run_indexing_thread)
                thread.start()
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
                answer = run_ai_assistant(user_input, llm, agent_with_history, rag_engine)
            st.write(answer)
        if not any(m.content == user_input for m in msgs.messages if m.type == "human"):
            msgs.add_user_message(user_input)
            msgs.add_ai_message(answer)