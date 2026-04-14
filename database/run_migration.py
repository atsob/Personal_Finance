#!/usr/bin/env python3
"""
Migration runner to add Transfer_Id column to Bank_Transactions table.
Run this before starting the app.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.connection import get_connection

def run_migration():
    """Apply the migration to add Transfer_Id column."""
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        # Create sequence
        print("Creating transfer_id_seq sequence...")
        cur.execute("CREATE SEQUENCE IF NOT EXISTS transfer_id_seq START 1 INCREMENT 1;")
        
        # Add column if it doesn't exist
        print("Adding Transfer_Id column...")
        cur.execute("""
            ALTER TABLE Bank_Transactions 
            ADD COLUMN IF NOT EXISTS Transfer_Id BIGINT;
        """)
        
        # Create index
        print("Creating index on Transfer_Id...")
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_transfer_id 
            ON Bank_Transactions(Transfer_Id) 
            WHERE Transfer_Id IS NOT NULL;
        """)
        
        conn.commit()
        print("✓ Migration completed successfully!")
        conn.close()
        
    except Exception as e:
        print(f"✗ Migration failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    run_migration()
