import os

def get_env_config():
    """Get environment configuration."""
    return {
        'db_user': os.getenv("DB_USER", "admin"),
        'db_password': os.getenv("DB_PASSWORD", "31.12.1969"),
        'db_host': os.getenv("DB_HOST", "192.168.4.20"),
        'db_port': os.getenv("DB_PORT", "5432"),
        'db_name': os.getenv("DB_NAME", "Finance"),
        # Timezone applied to every PostgreSQL session so that TIMESTAMPTZ columns
        # (e.g. Historical_Prices.Downloaded_At) display in local time rather than UTC.
        # The PostgreSQL Docker container typically runs UTC; this client-side setting
        # converts stored UTC values to the configured zone without touching the server.
        'db_timezone': os.getenv("DB_TIMEZONE", "Europe/Athens"),
        'ollama_ip': os.getenv("OLLAMA_IP", "192.168.4.20"),
        'ollama_port': os.getenv("OLLAMA_PORT", "11434"),
        'ollama_model': os.getenv("OLLAMA_MODEL", "llama3.2:3b"),
        'persist_dir': os.getenv("PERSIST_DIR", "/app/storage_rag"),
        'eodhd_api_key': os.getenv("EODHD_API_KEY", "69f3a12eacbd88.96449070"),
        # GoCardless Bank Account Data (PSD2 open banking) — optional
        # Set these to pre-fill credentials in the GoCardless import tab.
        # If unset, the user must enter them manually in the UI each session.
        'gocardless_secret_id':  os.getenv("GOCARDLESS_SECRET_ID",  ""),
        'gocardless_secret_key': os.getenv("GOCARDLESS_SECRET_KEY", ""),
        # Salt Edge Account Information API v5 (PSD2 open banking) — optional
        # Set these to pre-fill credentials in the Salt Edge import tab.
        # Obtain from: https://www.saltedge.com/dashboard → Applications
        'saltedge_app_id': os.getenv("SALTEDGE_APP_ID", ""),
        'saltedge_secret': os.getenv("SALTEDGE_SECRET", ""),
    }

ENV_CONFIG = get_env_config()
DB_URI = f"postgresql+psycopg2://{ENV_CONFIG['db_user']}:{ENV_CONFIG['db_password']}@{ENV_CONFIG['db_host']}:{ENV_CONFIG['db_port']}/{ENV_CONFIG['db_name']}"
OLLAMA_URL = f"http://{ENV_CONFIG['ollama_ip']}:{ENV_CONFIG['ollama_port']}"