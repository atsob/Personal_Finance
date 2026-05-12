import pandas as pd
import streamlit as st
import streamlit.components.v1 as _st_components

# Βοηθητική συνάρτηση για το χρώμα (πράσινο αν > 0, κόκκινο αν < 0)
def get_color(val):
    return "#28a745" if val >= 0 else "#dc3545"

def color_negative_red(val):
    """Return CSS color for negative values (red), positive (green), zero (blue)."""
    if val < 0:
        return 'color: red'
    elif val > 0:
        return 'color: green'
    else:
        return 'color: blue'

def color_quantity(val):
    """Return CSS color for quantity values."""
    if val < 0:
        return 'color: red'
    elif val > 0:
        return 'color: green'
    else:
        return 'color: blue'

def color_value(val):
    """Return CSS color for value (€) values."""
    if val < 0:
        return 'color: red'
    elif val > 0:
        return 'color: green'
    else:
        return 'color: blue'

def style_qty_display(series_or_df):
    # Δημιουργούμε μια λίστα από κενά styles με το ίδιο μέγεθος
    styles = ['' for _ in range(len(series_or_df))]
    
    for i in range(len(series_or_df)):
        val = series_or_df['qty'].iloc[i] # Κοιτάμε την αριθμητική τιμή της qty
        
        if val < 0:
            styles[i] = 'color: red'
        elif val == 0:
            styles[i] = 'color: blue'
        else:
            styles[i] = 'color: green'
            
    return styles

def format_qty_display(row):
    """Format quantity for display based on type."""
    if row['type'] in ['Cash', 'Assets', 'Pension']:
        val = f"{row['qty']:,.2f}"
        symbols = {'EUR': '€', 'USD': '$', 'GBP': '£'}
        sym = symbols.get(row['curr'], row['curr'])
        return f"{sym} {val}"
    else:
        val = f"{row['qty']:,.8f}"
        if '.' in val:
            val = val.rstrip('0').rstrip('.')
        if val in ["0", "-0"]:
            val = "0"
        return val
    
def copy_df_button(df, key: str, label: str = "📋 Copy") -> None:
    """Render a button that copies the dataframe as TSV to the clipboard (paste directly into Excel)."""
    raw = df.data if hasattr(df, "data") else df
    if not isinstance(raw, pd.DataFrame):
        raw = pd.DataFrame(raw)
    # If the index carries meaningful labels (named or MultiIndex with names),
    # promote it to columns so it appears in the copy.
    idx = raw.index
    has_named_index = bool(
        getattr(idx, 'name', None) or
        any(n for n in getattr(idx, 'names', []))
    )
    if has_named_index:
        raw = raw.reset_index()
    tsv = raw.to_csv(sep="\t", index=False)
    # Escape characters that would break the JS template literal
    tsv_js = tsv.replace("\\", "\\\\").replace("`", "\\`").replace("$", "\\$")
    html = f"""
    <button onclick="navigator.clipboard.writeText(`{tsv_js}`)
        .then(()=>{{this.innerText='✅ Copied!';setTimeout(()=>this.innerText='{label}',2000)}})
        .catch(()=>this.innerText='❌ Failed');"
      style="background:#262730;color:white;border:1px solid #555;padding:4px 12px;
             border-radius:4px;cursor:pointer;font-size:13px;font-family:sans-serif">
      {label}
    </button>
    """
    _st_components.html(html, height=38)


def custom_metric(label, value, pnl_value):
    # Color definition: Green (>0), Red (<0), Blue (==0)
    if pnl_value > 0:
        color = "#28a745"  # Green
    elif pnl_value < 0:
        color = "#dc3545"  # Red
    else:
        color = "#007bff"  # Blue (Standard Blue)

    st.markdown(
        f"""
        <div style="
            background-color: rgba(0,0,0,0.05); 
            padding: 15px; 
            border-radius: 10px; 
            border-left: 5px solid {color};">
            <p style="color: grey; font-size: 14px; margin: 0; font-family: sans-serif;">{label}</p>
            <p style="color: {color}; font-size: 24px; font-weight: bold; margin: 0; font-family: sans-serif;">
                {value}
            </p>
        </div>
        """, 
        unsafe_allow_html=True
    )
