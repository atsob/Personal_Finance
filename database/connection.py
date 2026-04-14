import psycopg2
from langchain_community.utilities import SQLDatabase
from config.settings import ENV_CONFIG, DB_URI

def get_connection():
    """Create and return a database connection."""
    return psycopg2.connect(
        dbname=ENV_CONFIG['db_name'],
        user=ENV_CONFIG['db_user'],
        password=ENV_CONFIG['db_password'],
        host=ENV_CONFIG['db_host'],
        port=int(ENV_CONFIG['db_port'])
    )

def get_sql_database():
    """Get SQLDatabase instance for LangChain."""
    return SQLDatabase.from_uri(
        DB_URI,
        include_tables=[
            'accounts', 'bank_transactions', 'bank_transaction_splits',
            'categories', 'currencies', 'financialinstitutions',
            'historical_fx', 'historical_prices', 'holdings',
            'investment_transactions', 'payees', 'securities'
        ],
        sample_rows_in_table_info=30
    )