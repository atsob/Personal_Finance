import psycopg2
from config.settings import ENV_CONFIG

try:
    conn = psycopg2.connect(
        dbname=ENV_CONFIG['db_name'],
        user=ENV_CONFIG['db_user'],
        password=ENV_CONFIG['db_password'],
        host=ENV_CONFIG['db_host'],
        port=ENV_CONFIG['db_port']
    )
    cur = conn.cursor()
    
    print("Creating transfer_id_seq sequence...")
    cur.execute("CREATE SEQUENCE IF NOT EXISTS transfer_id_seq START 1 INCREMENT 1;")
    
    print("Adding Transfer_Id column...")
    cur.execute("ALTER TABLE Bank_Transactions ADD COLUMN IF NOT EXISTS Transfer_Id BIGINT;")
    
    print("Creating index on Transfer_Id...")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_transfer_id ON Bank_Transactions(Transfer_Id) WHERE Transfer_Id IS NOT NULL;")
    
    conn.commit()
    print("✓ Migration completed successfully!")
    conn.close()
except Exception as e:
    print(f"✗ Migration failed: {e}")
    if conn:
        conn.rollback()
        conn.close()
