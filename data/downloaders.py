import logging
import streamlit as st
import yfinance as yf
from database.connection import get_connection
from ai.llm import get_custom_session

def download_historical_fx(tsperiod):
    """Download historical FX rates from Yahoo Finance."""
    conn = get_connection()
    cur = conn.cursor()
    custom_session = get_custom_session()
    
    try:
        cur.execute("SELECT Currencies_Id FROM Currencies WHERE Currencies_ShortName = 'EUR'")
        target_id = cur.fetchone()[0]
        
        cur.execute("SELECT Currencies_Id, Currencies_ShortName FROM Currencies WHERE Currencies_ShortName != 'EUR'")
        currencies = cur.fetchall()
        
        for base_id, symbol in currencies:
            logging.info(f"Downloading historical data for {symbol}...")
            ticker_symbol = f"EUR{symbol}=X"
            ticker = yf.Ticker(ticker_symbol, session=custom_session)
            hist = ticker.history(period=tsperiod)
            
            if hist.empty:
                logging.warning(f"No data found for {ticker_symbol}")
                continue
            
            for date, row in hist.iterrows():
                rate_to_eur = float(1 / row['Close'])
                formatted_date = date.strftime('%Y-%m-%d')
                
                cur.execute("""
                    INSERT INTO Historical_FX (Base_Currency_Id, Target_Currency_Id, FX_Date, FX_Rate)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (Base_Currency_Id, Target_Currency_Id, FX_Date)
                    DO UPDATE SET FX_Rate = EXCLUDED.FX_Rate
                """, (base_id, target_id, formatted_date, rate_to_eur))
            
            conn.commit()
            logging.info(f"Completed import for {symbol}")
            
    except Exception as e:
        st.error(f"❌ Error: {e}")
        logging.error(f"Error: {e}")
    finally:
        cur.close()
        conn.close()

def download_historical_prices_from_yahoo(tsperiod):
    """Download historical security prices from Yahoo Finance."""
    conn = get_connection()
    cur = conn.cursor()
    custom_session = get_custom_session()
    
    try:
        cur.execute("""
            SELECT Securities_Id, Security_Name, Yahoo_Ticker 
            FROM Securities 
            WHERE Yahoo_Ticker IS NOT NULL 
            AND Yahoo_Ticker != '' 
            AND Security_Name NOT LIKE 'Hellenic T-Bill%' 
            ORDER BY Security_Name ASC
        """)
        
        securities = cur.fetchall()
        
        for sec_id, sec_name, symbol in securities:
            logging.info(f"Downloading historical data for {sec_name}...")
            ticker = yf.Ticker(symbol, session=custom_session)
            hist = ticker.history(period=tsperiod)
            
            if hist.empty:
                logging.warning(f"No data found for {sec_name}")
                continue
            
            for date, row in hist.iterrows():
                rate = float(row['Close'])
                volume = float(row['Volume'])
                formatted_date = date.strftime('%Y-%m-%d')
                
                cur.execute("""
                    INSERT INTO Historical_Prices (Securities_Id, Price_Date, Price_Close, Volume)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (Securities_Id, Price_Date)
                    DO UPDATE SET Price_Close = EXCLUDED.Price_Close, Volume = EXCLUDED.Volume
                """, (sec_id, formatted_date, rate, volume))
            
            conn.commit()
            logging.info(f"Completed import for {symbol}")
            
    except Exception as e:
        st.error(f"❌ Error: {e}")
        logging.error(f"Error: {e}")
    finally:
        cur.close()
        conn.close()