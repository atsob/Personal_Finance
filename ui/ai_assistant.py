import os
import re
import decimal
import datetime
import threading
import streamlit as st
from ai.agent import _compact_schema
from database.connection import get_connection
from streamlit.runtime.scriptrunner import add_script_run_ctx
from langchain_community.callbacks.streamlit import StreamlitCallbackHandler
from langchain_community.chat_message_histories import StreamlitChatMessageHistory
from langchain_core.messages import HumanMessage, AIMessage
from ai.rag import run_indexing_thread
from ai.web_search import web_search
from ai.update_vector import update_all_embeddings
from config.settings import ENV_CONFIG

def _classify_question(user_input: str) -> str:
    """Classify without an LLM call — saves a full round-trip on slow hardware."""
    text = user_input.lower()

    web_keywords = [
        "current price", "stock price", "share price", "today's price",
        "market news", "latest news", "news about", "eur/usd", "usd/eur",
        "exchange rate today", "crypto", "bitcoin", "live price",
        "what is the price", "how is the market",
    ]
    if any(kw in text for kw in web_keywords):
        return "WEB"

    general_keywords = [
        "hello", "hi ", "hey ", "good morning", "good afternoon", "good evening",
        "thank", "what can you", "how are you", "nice to meet", "help me understand",
        "what is a", "what is an", "explain ", "define ",
    ]
    if any(kw in text for kw in general_keywords):
        return "GENERAL"

    # Everything else is treated as a database question — better to try SQL and fail
    # gracefully than to incorrectly refuse a finance question.
    return "SQL"


def _fmt_cell(v) -> str:
    """Format a single cell value for display."""
    if v is None:
        return ""
    if isinstance(v, decimal.Decimal):
        return f"{v:,.2f}"
    if isinstance(v, (datetime.date, datetime.datetime)):
        return str(v)
    return str(v)


def _to_markdown_table(colnames: list, rows: list, max_rows: int = 200) -> str:
    """Render query results as a GitHub-flavoured markdown table."""
    if not rows:
        return "No matching records found."
    truncated = len(rows) > max_rows
    display = rows[:max_rows]
    header = "| " + " | ".join(colnames) + " |"
    sep    = "| " + " | ".join(["---"] * len(colnames)) + " |"
    body   = "\n".join(
        "| " + " | ".join(_fmt_cell(c) for c in row) + " |"
        for row in display
    )
    note = f"\n\n*Showing first {max_rows} of {len(rows)} rows.*" if truncated else ""
    return f"{header}\n{sep}\n{body}{note}"


def _run_sql(user_input: str, full_date: str, llm, db) -> str:
    """Generate SQL → execute with real cursor → format result. At most 2 LLM calls."""
    if db is None:
        return "Sorry, I couldn't reach the database."

    schema_info = _compact_schema(db)

    sql_prompt = (
        f"Today is {full_date}.\n"
        f"PostgreSQL expert. Write ONE SELECT query for the question below.\n"
        f"Rules:\n"
        f"- ILIKE for text matching\n"
        f"- DATE_TRUNC for period filters\n"
        f"- Always JOIN to get human-readable names "
        f"(e.g. JOIN payees ON payees.payees_id = transactions.payees_id to show payees_name)\n"
        f"- Alias every aggregate column\n"
        f"- LIMIT 200 unless the user asks for everything\n"
        f"Output the SQL query ONLY — no prose, no markdown fences.\n\n"
        f"SCHEMA:\n{schema_info}\n\n"
        f"QUESTION: {user_input}"
    )

    try:
        sql = llm.invoke(sql_prompt).content.strip()
        sql = re.sub(r"^```[a-z]*\n?", "", sql, flags=re.IGNORECASE)
        sql = re.sub(r"\n?```$", "", sql)
        sql = sql.strip()
    except Exception as e:
        return f"I couldn't generate a SQL query: {str(e)[:150]}"

    # Execute with a real cursor so we get column names
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(sql)
        colnames = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        cur.close()
    except Exception as e:
        return f"I tried to query the database but encountered an error: {str(e)[:200]}"
    finally:
        if conn:
            conn.close()

    if not rows:
        return "No matching records found."

    # Single scalar (e.g. SUM, COUNT) — plain answer, no LLM needed
    if len(rows) == 1 and len(colnames) == 1:
        return f"**{_fmt_cell(rows[0][0])}**"

    # Small aggregation (a few labelled values) — plain answer
    if len(rows) <= 5 and len(colnames) <= 2:
        lines = [" | ".join(_fmt_cell(c) for c in row) for row in rows]
        return "\n".join(lines)

    # List / detail result — render as markdown table, no LLM needed
    return _to_markdown_table(colnames, rows)


def run_ai_assistant(user_input: str, llm, agent_with_history, rag_engine, db=None) -> str:
    """Route question to appropriate handler."""
    import datetime as dt_lib
    full_date = dt_lib.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    full_prompt = f"Today is {full_date}. {user_input}"

    # Keyword-based routing — zero LLM calls, instant
    decision = _classify_question(user_input)

    if decision == "SQL":
        st.info("🗄️ Querying your database...")
        result = _run_sql(user_input, full_date, llm, db)
        if not result.startswith("I tried to query") and not result.startswith("Sorry"):
            return result
        # Only fall back to the full agent if the simple path failed
        st.warning("Retrying with agent...")
        try:
            st_callback = StreamlitCallbackHandler(st.container())
            config = {"configurable": {"session_id": "Personal_Finance"}, "callbacks": [st_callback]}
            response = agent_with_history.invoke({"input": full_prompt}, config=config)
            return response["output"]
        except Exception as e:
            return f"I wasn't able to answer that question. Error: {str(e)[:150]}"

    elif decision == "WEB":
        st.info("🌐 Searching the web...")
        web_results = web_search(user_input)
        synthesis_prompt = (
            f"Today is {full_date}.\n"
            f"User asked: {user_input}\n\n"
            f"Web results:\n{web_results}\n\n"
            f"Answer clearly and concisely."
        )
        return llm.invoke(synthesis_prompt).content

    else:
        st.info("🤖 Answering directly...")
        return llm.invoke(full_prompt).content


def render_ai_assistant(llm, agent_with_history, rag_engine, db=None):
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
                    answer = run_ai_assistant(user_input, llm, agent_with_history, rag_engine, db=db)
                    st.write(answer)
                    
                    # Save to history
                    msgs.add_user_message(user_input)
                    msgs.add_ai_message(answer)
                except Exception as e:
                    error_msg = f"Sorry, I encountered an error: {str(e)[:200]}"
                    st.error(error_msg)
                    msgs.add_user_message(user_input)
                    msgs.add_ai_message(error_msg)