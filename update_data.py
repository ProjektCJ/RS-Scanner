import yfinance as yf
import pandas as pd
import os
import requests
import io
import time
import numpy as np

def get_tickers():
    print("--- SCHRITT 1: Ticker-Listen laden ---")
    headers = {"User-Agent": "Mozilla/5.0"}
    tickers = []
    # Wir holen S&P 500 und Nasdaq 100
    for url, match in [('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', 'Symbol'), 
                        ('https://en.wikipedia.org/wiki/Nasdaq-100', 'Ticker')]:
        try:
            resp = requests.get(url, headers=headers, timeout=10)
            df = pd.read_html(io.StringIO(resp.text), match=match)[0]
            tickers += df[match].tolist()
        except: continue
    # Bereinigung: Punkte durch Bindestriche ersetzen (BRK.B -> BRK-B)
    return sorted(list(set([str(t).strip().replace('.', '-') for t in tickers if str(t) != 'nan'])))

def calculate_rs_rank(series):
    """Berechnet das Ranking von 0 bis 99"""
    if series.empty: return series
    return (series.rank(pct=True) * 99).fillna(0).astype(int)

def update_data():
    if not os.path.exists('Data'): os.makedirs('Data')
    symbols = get_tickers()
    
    print(f"\n--- SCHRITT 2: Kurse laden (Bulk-Download) ---")
    # Wir laden 2 Jahre Historie für alle Berechnungen
    data = yf.download(symbols + ["SPY"], period="2y", interval="1d", group_by='ticker', threads=False)
    
    results = []
    print(f"\n--- SCHRITT 3: Verarbeitung & Fundamentaldaten ---")
    print("(Das dauert etwas, da wir für jede Aktie Sektor & Co. abfragen...)")
    
    for i, t in enumerate(symbols):
        try:
            if t not in data.columns.levels[0]: continue
            df = data[t].dropna(subset=['Close']) # Nur Zeilen mit Preis behalten
            if len(df) < 200: continue # Zu junge Aktien überspringen
            
            # --- Technische Daten ---
            close = df['Adj Close'] if 'Adj Close' in df.columns else df['Close']
            cur = float(close.iloc[-1])
            highs = df['High']
            lows = df['Low']
            vol = df['Volume']
            
            # Moving Averages & Trend
            sma10 = close.rolling(10).mean().iloc[-1]
            sma20 = close.rolling(20).mean().iloc[-1]
            sma50 = close.rolling(50).mean().iloc[-1]
            sma200 = close.rolling(200).mean().iloc[-1]
            
            # Volatilität (ADR) & Volumen
            adr_val = (((highs/lows)-1)*100).rolling(20).mean().iloc[-1]
            avg_vol_30 = vol.rolling(30).mean().iloc[-1]
            rel_vol = vol.iloc[-1] / avg_vol_30 if avg_vol_30 > 0 else 1.0
            
            # 52-Wochen Hoch
            h52 = highs.iloc[-252:].max() if len(df) >= 252 else highs.max()
            pct_from_52w = ((cur / h52) - 1) * 100

            # --- Fundamentaldaten (Sektor, Market Cap etc.) ---
            # Standardwerte falls Yahoo keine Info liefert
            sector, industry, mcap = "N/A", "N/A", 0
            eps_q_yoy, rev_q_yoy, roe_ttm = 0, 0, 0
            full_name = t
            
            try:
                info = yf.Ticker(t).info
                sector = info.get('sector', 'N/A')
                industry = info.get('industry', 'N/A')
                mcap = info.get('marketCap', 0)
                full_name = info.get('longName', t)
                # Wachstum & Zahlen (in Dezimalform von Yahoo, also *100 für %)
                eps_q_yoy = info.get('earningsQuarterlyGrowth', 0) * 100
                rev_q_yoy = info.get('revenueGrowth', 0) * 100
                roe_ttm = info.get('returnOnEquity', 0) * 100
            except: pass

            # --- Datensatz zusammenbauen ---
            # WICHTIG: Hier bereiten wir alle Daten vor, die Formatierung kommt am Schluss
            res = {
                "Ticker": t,
                "Name": full_name,
                "Price": cur,
                "Sector": sector,
                "Industry": industry,
                "Mkt Cap": mcap, # Wird später formatiert
                "Volume": vol.iloc[-1], # Wird später formatiert
                "Avg Vol 30D": avg_vol_30,
                "Rel Vol 1D": rel_vol,
                "ADR%": adr_val,
                # Fundamentals
                "EPS Qtr YoY": eps_q_yoy,
                "Rev Qtr YoY": rev_q_yoy,
                "ROE TTM": roe_ttm,
                # Trend Booleans (String "Yes"/"No" oder True/False je nach App, wir nehmen Bool/Int für Sortierung)
                "P>10": cur > sma10,
                "P>20": cur > sma20,
                "P>50": cur > sma50,
                "P>200": cur > sma200,
                "50>200": sma50 > sma200,
                "% From 52W High": pct_from_52w,
                # Performance Rohdaten (für RS Ranking)
                "_raw_1w": ((cur / close.iloc[-min(5, len(df))]) - 1) * 100,
                "_raw_1m": ((cur / close.iloc[-min(21, len(df))]) - 1) * 100,
                "_raw_3m": ((cur / close.iloc[-min(63, len(df))]) - 1) * 100,
                "_raw_6m": ((cur / close.iloc[-min(126, len(df))]) - 1) * 100,
                "_raw_1y": ((cur / close.iloc[-min(252, len(df))]) - 1) * 100,
                "% 1D": ((cur / close.iloc[-2]) - 1) * 100 if len(df) > 1 else 0
            }
            results.append(res)
            if (i+1) % 50 == 0: print(f"> {i+1} Aktien verarbeitet...")
        except: continue

    df = pd.DataFrame(results)
    
    # --- RS Rankings berechnen (0-99) ---
    for p in ['1w', '1m', '3m', '6m', '1y']:
        df[f"RS {p.upper()}"] = calculate_rs_rank(df[f"_raw_{p}"])
        # Performance Spalten formatieren
        df[f"% {p.upper()}"] = df[f"_raw_{p}"].map(lambda x: f"{x:.2f}%")

    df["% 1D"] = df["% 1D"].map(lambda x: f"{x:.2f}%")

    # --- FORMATIERUNG & ENDGÜLTIGE STRUKTUR ---
    # Hier sorgen wir dafür, dass die Spalten EXAKT so heißen und stehen wie im Original
    
    # 1. Zahlen formatieren
    df["Price"] = df["Price"].map(lambda x: f"${x:.2f}")
    df["Volume"] = df["Volume"].map(lambda x: f"{x/1e6:.2f}M")
    df["ADR%"] = df["ADR%"].map(lambda x: f"{x:.2f}%")
    df["Mkt Cap"] = df["Mkt Cap"].map(lambda x: f"{x/1e9:.2f}B")
    df["Rel Vol 1D"] = df["Rel Vol 1D"].map(lambda x: f"{x:.2f}")
    
    # Fundamental Formatierung
    for col in ["EPS Qtr YoY", "Rev Qtr YoY", "ROE TTM", "% From 52W High"]:
        df[col] = df[col].map(lambda x: f"{x:.2f}%")

    # Boolean Formatierung (True -> "Yes")
    for col in ["P>10", "P>20", "P>50", "P>200", "50>200"]:
        df[col] = df[col].map(lambda x: "Yes" if x else "No")

    # WICHTIG: Die exakte Reihenfolge der Spalten festlegen
    # Wir nehmen die Standard-Sicht der App + alle Filter-Optionen
    final_columns = [
        "Ticker", "Name", "Price", "Volume", "ADR%", "Sector", "Industry", "Mkt Cap",
        "RS 1W", "RS 1M", "RS 3M", "RS 6M", "RS 1Y", 
        "% 1D", "% 1W", "% 1M", "% 3M", "% 6M", "% 1Y",
        "Rel Vol 1D", "EPS Qtr YoY", "Rev Qtr YoY", "ROE TTM", 
        "P>10", "P>20", "P>50", "P>200", "50>200", "% From 52W High"
    ]
    
    # Nur Spalten nehmen, die wir auch wirklich haben (Safety Check)
    cols_to_save = [c for c in final_columns if c in df.columns]
    
    df[cols_to_save].to_csv("Data/Screener_Data.csv", index=False)
    
    # SPY Update
    try:
        spy = data["SPY"].dropna()
        spy_perf = ((spy['Adj Close'].iloc[-1] / spy['Adj Close'].iloc[-min(252, len(spy))]) - 1) * 100
        pd.DataFrame([{"Symbol": "SPY", "Price": round(spy['Adj Close'].iloc[-1], 2), "% 1Y": f"{spy_perf:.2f}%"}]).to_csv("Data/SPY_Data.csv", index=False)
    except: pass
    
    print(f"\n--- ERFOLG: {len(df)} Aktien im Original-Format gespeichert. ---")

if __name__ == "__main__":
    update_data()
