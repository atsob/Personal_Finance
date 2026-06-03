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
from ui.importers import render_importers
from ui.static_data import render_static_data
from ui.recurring import render_recurring

@st.cache_resource
def startup_db_maintenance():
    conn = get_connection()
    with conn.cursor() as cursor:
        cursor.execute("ANALYZE;")
        # Normalise sentinel strings that older downloader runs may have written
        # into text columns that should be NULL when no real value exists.
        cursor.execute("""
            UPDATE Securities
            SET Analyst_Rating = NULL
            WHERE LOWER(TRIM(Analyst_Rating)) IN ('none', 'n/a', 'na', '');
        """)
        # Drop Wikipedia columns that were added experimentally and are no longer used.
        cursor.execute("ALTER TABLE Institutions DROP COLUMN IF EXISTS Wikipedia_Title;")
        cursor.execute("ALTER TABLE Institutions DROP COLUMN IF EXISTS Ratings_Updated;")
        # Tax-exempt flag for securities whose income is not subject to income tax
        # (e.g. Hellenic T-Bills purchased at primary market).
        cursor.execute("""
            ALTER TABLE Securities
            ADD COLUMN IF NOT EXISTS Is_Tax_Exempt BOOLEAN DEFAULT FALSE
        """)
        # Instrument type at transaction level — captures the exact traded instrument
        # (e.g. CFDOnETF, CFDOnStock, CFDOnIndex, CFDOnFutures, CFDOnFund) independently of the security master.
        # Useful for Saxo Bank imports where the same underlying security may be traded
        # as a regular ETF on one occasion and a CFD on another.
        cursor.execute("""
            ALTER TABLE Investments
            ADD COLUMN IF NOT EXISTS Instrument_Type VARCHAR(50)
        """)
        # ── Migration 005: Recurring Templates ───────────────────────────────
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Recurring_Templates (
                Templates_Id        SERIAL PRIMARY KEY,
                Name                VARCHAR(100) NOT NULL,
                Accounts_Id         INTEGER NOT NULL REFERENCES Accounts(Accounts_Id) ON DELETE CASCADE,
                Payees_Id           INTEGER REFERENCES Payees(Payees_Id),
                Description         TEXT,
                Total_Amount        NUMERIC(28, 18),
                Periodicity         VARCHAR(20) NOT NULL DEFAULT 'Monthly',
                Next_Due_Date       DATE NOT NULL,
                End_Date            DATE,
                Auto_Confirm        BOOLEAN DEFAULT FALSE,
                Active              BOOLEAN DEFAULT TRUE,
                Accounts_Id_Target  INTEGER REFERENCES Accounts(Accounts_Id),
                Created_At          TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Recurring_Template_Splits (
                Splits_Id       SERIAL PRIMARY KEY,
                Templates_Id    INTEGER NOT NULL
                    REFERENCES Recurring_Templates(Templates_Id) ON DELETE CASCADE,
                Categories_Id   INTEGER REFERENCES Categories(Categories_Id),
                Amount          NUMERIC(28, 18),
                Memo            TEXT
            )
        """)
        cursor.execute("""
            ALTER TABLE Transactions
                ADD COLUMN IF NOT EXISTS Is_Draft   BOOLEAN DEFAULT FALSE
        """)
        cursor.execute("""
            ALTER TABLE Transactions
                ADD COLUMN IF NOT EXISTS Templates_Id INTEGER
                    REFERENCES Recurring_Templates(Templates_Id) ON DELETE SET NULL
        """)
        # Draft-aware balance trigger (replaces existing function in-place)
        cursor.execute("""
            CREATE OR REPLACE FUNCTION public.update_accounts_balance_with_transfer()
                RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                IF (TG_OP = 'INSERT') THEN
                    IF NEW.Is_Draft THEN RETURN NULL; END IF;
                    UPDATE Accounts SET Accounts_Balance = Accounts_Balance + NEW.Total_Amount
                     WHERE Accounts_Id = NEW.Accounts_Id;
                    IF NEW.Accounts_Id_Target IS NOT NULL THEN
                        UPDATE Accounts
                           SET Accounts_Balance = Accounts_Balance + COALESCE(NEW.Total_Amount_Target, -NEW.Total_Amount)
                         WHERE Accounts_Id = NEW.Accounts_Id_Target;
                    END IF;
                ELSIF (TG_OP = 'DELETE') THEN
                    IF OLD.Is_Draft THEN RETURN NULL; END IF;
                    UPDATE Accounts SET Accounts_Balance = Accounts_Balance - OLD.Total_Amount
                     WHERE Accounts_Id = OLD.Accounts_Id;
                    IF OLD.Accounts_Id_Target IS NOT NULL THEN
                        UPDATE Accounts
                           SET Accounts_Balance = Accounts_Balance - COALESCE(OLD.Total_Amount_Target, -OLD.Total_Amount)
                         WHERE Accounts_Id = OLD.Accounts_Id_Target;
                    END IF;
                ELSIF (TG_OP = 'UPDATE') THEN
                    IF OLD.Is_Draft AND NEW.Is_Draft THEN
                        RETURN NULL;
                    ELSIF OLD.Is_Draft AND NOT NEW.Is_Draft THEN
                        UPDATE Accounts SET Accounts_Balance = Accounts_Balance + NEW.Total_Amount
                         WHERE Accounts_Id = NEW.Accounts_Id;
                        IF NEW.Accounts_Id_Target IS NOT NULL THEN
                            UPDATE Accounts
                               SET Accounts_Balance = Accounts_Balance + COALESCE(NEW.Total_Amount_Target, -NEW.Total_Amount)
                             WHERE Accounts_Id = NEW.Accounts_Id_Target;
                        END IF;
                    ELSIF NOT OLD.Is_Draft AND NEW.Is_Draft THEN
                        UPDATE Accounts SET Accounts_Balance = Accounts_Balance - OLD.Total_Amount
                         WHERE Accounts_Id = OLD.Accounts_Id;
                        IF OLD.Accounts_Id_Target IS NOT NULL THEN
                            UPDATE Accounts
                               SET Accounts_Balance = Accounts_Balance - COALESCE(OLD.Total_Amount_Target, -OLD.Total_Amount)
                             WHERE Accounts_Id = OLD.Accounts_Id_Target;
                        END IF;
                    ELSE
                        UPDATE Accounts SET Accounts_Balance = Accounts_Balance - OLD.Total_Amount
                         WHERE Accounts_Id = OLD.Accounts_Id;
                        UPDATE Accounts SET Accounts_Balance = Accounts_Balance + NEW.Total_Amount
                         WHERE Accounts_Id = NEW.Accounts_Id;
                    END IF;
                END IF;
                RETURN NULL;
            END;
            $$
        """)
        # Extend the securities_type ENUM with new instrument categories.
        # ADD VALUE IF NOT EXISTS is safe to run repeatedly; the label must be
        # a literal in the DDL statement (parameters are not accepted here).
        for _new_type in ('CFD', 'Closed-End Fund'):
            cursor.execute(
                f"ALTER TYPE securities_type ADD VALUE IF NOT EXISTS '{_new_type}'"
            )
    conn.commit()
    return True

_MOBILE_CSS = """
<style>
/* ── Mobile-first responsive tweaks ───────────────────────────────────────── */

/* Prevent iOS Safari from auto-zooming on input focus (requires font-size≥16px) */
input, select, textarea,
.stTextInput > div > div > input,
.stNumberInput input,
.stTextArea textarea,
.stSelectbox > div > div > div,
.stMultiSelect > div > div > div {
    font-size: 16px !important;
}

/* Plotly chart containers: allow horizontal scroll instead of squashing */
[data-testid="stPlotlyChart"] > div {
    overflow-x: auto !important;
    -webkit-overflow-scrolling: touch;
}
.js-plotly-plot {
    min-width: 0;         /* let Plotly control its own width */
}

/* NOTE: do NOT add overflow:auto/hidden to [data-testid="stDataFrame"] —
   it creates a clipping context that hides the data editor toolbar (search,
   delete, fullscreen buttons). Streamlit data editors scroll internally. */

@media (max-width: 768px) {
    /* Tighter content padding on phones */
    .main .block-container {
        padding-left: 0.75rem !important;
        padding-right: 0.75rem !important;
        padding-top: 1rem !important;
    }

    /* Bigger touch targets for buttons (Apple HIG recommends ≥44 pt) */
    .stButton > button {
        min-height: 44px !important;
        font-size: 15px !important;
        padding: 8px 16px !important;
    }

    /* Bigger touch targets for selectboxes and inputs */
    .stSelectbox > div,
    .stMultiSelect > div,
    .stTextInput > div,
    .stNumberInput > div {
        min-height: 44px !important;
    }

    /* Tabs: slightly smaller text so they fit without wrapping */
    .stTabs [data-baseweb="tab"] {
        padding: 6px 10px !important;
        font-size: 13px !important;
    }

    /* Sidebar toggle button — easier to tap */
    [data-testid="collapsedControl"] {
        width: 44px !important;
        height: 44px !important;
    }

    /* Stack metric columns more naturally */
    [data-testid="column"] {
        min-width: 140px;
    }
}
</style>
"""

def main():
    """Main application entry point."""
    # Page configuration
    st.set_page_config(page_title="Personal Finance", layout="wide")

    # Inject responsive / mobile-friendly CSS
    st.markdown(_MOBILE_CSS, unsafe_allow_html=True)

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
            "🔁 Recurring",
            "⏳ Reports",
            "📋 Static Data",
            "🌍 Market Data",
            "📥 Importers",
            "🛠️ Tools",
            "🧠 AI Assistant",
        ]
    )

    st.sidebar.divider()
    if st.sidebar.button("Clear Cache", width="stretch"):
        st.cache_data.clear()
        st.sidebar.success("Cache cleared.")

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
        elif menu == "📥 Importers":
            render_importers()
        elif menu == "🔁 Recurring":
            render_recurring()
        elif menu == "🛠️ Tools":
            render_tools(conn)
        elif menu == "🧠 AI Assistant":
            render_ai_assistant(llm, agent_with_history, rag_engine, db=db)            

    finally:
        conn.close()

if __name__ == "__main__":
    main()