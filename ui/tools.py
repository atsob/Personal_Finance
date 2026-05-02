import streamlit as st
import pandas as pd
from data.qif_importer import render_qif_importer
from data.transfer_issues import render_transfer_issues
from database.backup import render_backup_restore
from database.backup import render_backup_restore_simple
from database.backup import render_backup_restore_quick


def render_tools(conn):
    """Render the Tools page."""
    st.title("System Tools")
    t1, t2, t3, t4, t5 = st.tabs(["📁 QIF Importer", "📝 Transfer Issues", "💾 Backup & Restore", "💾 Backup & Restore Simple", "💾 Quick Backup & Restore"])
    
    with t1:  # QIF Importer tab
        render_qif_importer()

    with t2:  # Transfer Issues tab
        render_transfer_issues()

    with t3:  # Backup & Restore
        render_backup_restore()

    with t4:  # Backup & Restore Simple
        render_backup_restore_simple()

    with t5:  # Backup & Restore Quick
        render_backup_restore_quick()
        