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
        description="Useful for summaries, text analysis and qualitative financial information."
    )
    
    custom_suffix = """
You MUST respond using the following format:

Thought: [your reasoning]
Action:
{
  "action": "sql_db_query",
  "action_input": "SELECT name FROM securities WHERE name ILIKE 'A%';"
}

Observation: [result of the action]
... (this repeats until you have the answer)
Final Answer: [your final response]

IMPORTANT: Do not use Markdown code blocks (```) for the JSON action.
Always use double quotes for JSON keys and values.
Table names are lowercase. Use ILIKE for case-insensitive text matching.
If a query returns no data, do not retry more than 2 times — tell the user no data was found.

Question: {input}
{agent_scratchpad}
"""
    
    agent_executor = create_sql_agent(
        llm=llm,
        db=db,
        extra_tools=[rag_tool],
        agent_type="zero-shot-react-description",
        verbose=True,
        suffix=custom_suffix,
        handle_parsing_errors=True,
        return_intermediate_steps=True,
        allow_dangerous_requests=True
    )
    
    return agent_executor