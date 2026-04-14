from langchain_community.agent_toolkits import create_sql_agent
from langchain_community.agent_toolkits.sql.toolkit import SQLDatabaseToolkit
from langchain_core.tools import Tool

def create_ai_agent(llm, db, rag_engine):
    """Create the AI agent with SQL and RAG tools."""
    toolkit = SQLDatabaseToolkit(db=db, llm=llm)
    sql_tools = toolkit.get_tools()
    
    rag_tool = Tool(
        name="Financial_Knowledge_Base",
        func=lambda q: str(rag_engine.query(q)),
        description="Useful for summaries, text analysis and qualitative financial information. Use this for analyzing financial concepts, explaining terms, or providing financial education."
    )
    
    # Improved prompt template
    custom_prefix = """
You are a helpful financial assistant with access to a personal finance database.

IMPORTANT RULES:
1. If the user asks a general question (greeting, help, what you can do), answer directly without using any tools.
2. Only use the sql_db_query tool when you need to retrieve specific data from the database.
3. For analysis of existing data, first query the database, then provide analysis.
4. Never try to parse your own natural language responses as JSON actions.

When you need to query the database, use this EXACT format:
Thought: I need to query the database to answer this question.
Action:
{
  "action": "sql_db_query",
  "action_input": "SELECT ..."
}

After receiving results, provide your final answer naturally.

If you don't need to query the database, just respond directly without any special formatting.
"""
    
    custom_suffix = """
Question: {input}
{agent_scratchpad}

Remember: If you already have the answer or don't need database access, just respond directly without using the Action format.
"""
    
    agent_executor = create_sql_agent(
        llm=llm,
        db=db,
        extra_tools=[rag_tool],
        agent_type="zero-shot-react-description",
        verbose=True,
        prefix=custom_prefix,
        suffix=custom_suffix,
        handle_parsing_errors=True,  # This is critical - handles parsing errors gracefully
        max_iterations=3,  # Limit iterations to prevent loops
        early_stopping_method="generate",  # Generate response instead of forcing format
        allow_dangerous_requests=True
    )
    
    return agent_executor