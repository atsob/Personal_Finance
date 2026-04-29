import streamlit as st

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
