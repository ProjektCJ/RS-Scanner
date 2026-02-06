import yfinance as yf
import pandas as pd
import numpy as np
import os
import requests
import io
import time

def get_tickers():
    print("Hole Ticker-Listen von Wikipedia...")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    
    tickers = []
    
    # --- S&P 500 ---
    try:
        url_sp500 = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
        resp = requests.get(url_sp500, headers=headers, timeout=15)
        df_list = pd.read_html(io.StringIO(resp.text))
        for table in df_list:
            if 'Symbol' in table.columns:
                tickers += table['Symbol'].tolist()
                print(f"S&P 500: {len(table)} Ticker gefunden.")
                break
    except Exception as e:
        print(f"Hinweis: S&P 500 Fehler ({e})")

    time.sleep(1)

    # --- Nasdaq 100 (Maximale Robustheit) ---
    try:
        # Wir probieren die normale Desktop-Seite
        url_nas100 = 'https://en.wikipedia.org/wiki/Nasdaq-100'
        resp = requests.get(url_nas100, headers=headers, timeout=15)
        df_list = pd.read_html(io.StringIO(resp.text))
        
        found_nas = False
        for table in df_list:
            # Wir suchen eine Tabelle, die ca. 100 Zeilen hat und 'Ticker' oder 'Symbol' enthält
            cols = [str(c) for c in table.columns]
            if any(id_col in cols for id_col in ['Ticker', 'Symbol']):
                target_col = 'Ticker' if 'Ticker' in cols else 'Symbol'
                potential_tickers = table[target_col].tolist()
                if 90 <= len(potential_tickers) <= 105:
                    tickers += potential_tickers
                    print(f"Nasdaq 100: {len(potential_tickers)} Ticker gefunden.")
                    found_nas = True
                    break
        
        if not found_nas:
            print("Zweiter Versuch für Nasdaq 100...")
            # Fallback: Suche nach einer Tabelle, die 'Company' und einen Ticker-ähnlichen Wert hat
            for table in df_list:
                if len(table) > 90 and len(table) < 110:
                    # Nimm die erste Spalte, die meist den Ticker enthält
                    tickers += table.iloc[:, 1].tolist() # Oft ist Spalte 2 der Ticker
                    print(f"Nasdaq 100 (Alternativsuche): {len(table)} Ticker gefunden.")
                    break

    except Exception as e:
        print(f"Hinweis: Nasdaq 100 Fehler ({e})")

    # Bereinigung
    tickers = [str(t).strip().replace('.', '-') for t in tickers if str(t) != 'nan']
    tickers = sorted(list(set(tickers)))
    print(f"Gesamtanzahl Ticker: {len(tickers)}")
    return tickers

def update_data():
    if not os.path.exists('Data'):
        os.makedirs('Data')

    tickers = get_tickers()
    all_symbols = tickers + ["SPY"]
    
    print(f"Lade aktuelle Kurse für {len(all_symbols)} Ticker von Yahoo Finance...")
    # Threads auf False, um Windows-Datenbankfehler zu vermeiden
    data = yf.download(all_symbols, period="2y", interval="1d", group_by='ticker', threads=False)

    screener_rows = []
    spy_data = {}

    for ticker in all_symbols:
        try:
            if ticker not in data.columns.levels[0]: continue
            df = data[ticker].dropna()
            if df.empty or len(df) < 252: continue
            
            close = df['Close']
            curr_price = float(close.iloc[-1])
            
            res = {
                "Symbol": ticker,
                "Description": ticker,
                "Price": curr_price,
                "Price - Currency": "USD",
                "Performance % 1 week": ((curr_price / close.iloc[-5]) - 1) * 100,
                "Performance % 1 month": ((curr_price / close.iloc[-21]) - 1) * 100,
                "Performance % 3 months": ((curr_price / close.iloc[-63]) - 1) * 100,
                "Performance % 6 months": ((curr_price / close.iloc[-126]) - 1) * 100,
                "Performance % 1 year": ((curr_price / close.iloc[-252]) - 1) * 100,
                "Simple Moving Average (200) 1 day": close.rolling(200).mean().iloc[-1],
                "Simple Moving Average (50) 1 day": close.rolling(50).mean().iloc[-1],
                "Simple Moving Average (20) 1 day": close.rolling(20).mean().iloc[-1],
                "Simple Moving Average (10) 1 day": close.rolling(10).mean().iloc[-1],
                "High 52 weeks": close.iloc[-252:].max(),
                "High All Time": close.max(),
                "Sector": "Market Leader"
            }

            if ticker == "SPY":
                spy_data = res
            else:
                screener_rows.append(res)
        except:
            continue

    if screener_rows:
        pd.DataFrame(screener_rows).to_csv("Data/Screener_Data.csv", index=False)
        print(f"Screener_Data.csv ({len(screener_rows)} Aktien) gespeichert.")
    
    if spy_data:
        spy_df = pd.DataFrame([spy_data])
        cols = ["Symbol", "Description", "Price", "Price - Currency", 
                "Performance % 1 week", "Performance % 1 month", 
                "Performance % 3 months", "Performance % 6 months", "Performance % 1 year"]
        spy_df[cols].to_csv("Data/SPY_Data.csv", index=False)
        print("SPY_Data.csv gespeichert.")
        
    print("\n--- ERFOLG ---")

if __name__ == "__main__":
    update_data()