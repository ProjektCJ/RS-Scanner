import yfinance as yf
import pandas as pd
import os
import requests
import io
import time

def get_tickers():
    print("--- SCHRITT 1: Ticker-Listen laden ---")
    headers = {"User-Agent": "Mozilla/5.0"}
    tickers = []
    for url, match_word in [('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', 'Symbol'), 
                            ('https://en.wikipedia.org/wiki/Nasdaq-100', 'Ticker')]:
        try:
            resp = requests.get(url, headers=headers, timeout=10)
            df = pd.read_html(io.StringIO(resp.text), match=match_word)[0]
            tickers += df[match_word].tolist()
        except: continue
    tickers = sorted(list(set([str(t).strip().replace('.', '-') for t in tickers if str(t) != 'nan'])))
    print(f"Gesamtanzahl Ticker gefunden: {len(tickers)}")
    return tickers

def calculate_rs_rank(series):
    if series.empty: return series
    return (series.rank(pct=True) * 99).fillna(0).astype(int)

def update_data():
    if not os.path.exists('Data'): os.makedirs('Data')
    symbols = get_tickers()
    
    print(f"\n--- SCHRITT 2: Kurse laden ---")
    # Wir laden 2 Jahre Daten, um sicher genug Historie zu haben
    data = yf.download(symbols + ["SPY"], period="2y", interval="1d", group_by='ticker', threads=False)
    
    results = []
    print(f"\n--- SCHRITT 3: Verarbeitung (Detail-Check) ---")
    
    for i, t in enumerate(symbols):
        try:
            # Sicherstellen, dass der Ticker in den Daten existiert
            if t not in data.columns.levels[0]:
                continue
            
            # Wir extrahieren die Daten für diesen Ticker
            df = data[t].copy()
            # Nur Zeilen löschen, bei denen der Preis fehlt (nicht das Volumen)
            df = df.dropna(subset=['Close'])
            
            # Toleranterer Check: Wir brauchen mindestens 200 Tage für die SMAs
            if len(df) < 200:
                continue
            
            # Spalten-Mapping (manchmal heißt es Adj Close, manchmal nur Close)
            close_col = 'Adj Close' if 'Adj Close' in df.columns else 'Close'
            close = df[close_col]
            cur = float(close.iloc[-1])
            highs, lows, vol = df['High'], df['Low'], df['Volume']
            
            # Technische Indikatoren (RS Metriken aus image_d4541e.png)
            sma50 = close.rolling(50).mean().iloc[-1]
            sma200 = close.rolling(200).mean().iloc[-1]
            h52w = highs.iloc[-252:].max() if len(df) >= 252 else highs.max()
            adr = (((highs/lows)-1)*100).rolling(20).mean().iloc[-1]
            
            avg_vol_30d = vol.rolling(30).mean().iloc[-1]
            rel_vol = vol.iloc[-1] / avg_vol_30d if avg_vol_30d > 0 else 0

            # Fundamentaldaten (image_d453fa.png)
            sector, industry, mcap, eps_g, rev_g, name = "N/A", "N/A", "0", "0", "0", t
            try:
                # Wir holen Fundamentals nur wenn nötig, um Zeit zu sparen
                info = yf.Ticker(t).info
                sector = info.get('sector', 'N/A')
                industry = info.get('industry', 'N/A')
                mcap = f"{info.get('marketCap', 0) / 1e9:.2f}B"
                eps_g = f"{info.get('earningsQuarterlyGrowth', 0)*100:.1f}%"
                rev_g = f"{info.get('revenueGrowth', 0)*100:.1f}%"
                name = info.get('longName', t)
            except: pass

            res = {
                "Ticker": t, "Name": name, "Price": round(cur, 2),
                "Sector": sector, "Industry": industry, "Market Cap": mcap,
                "Volume": f"{vol.iloc[-1]/1e6:.2f}M", "Rel Vol 1D": round(rel_vol, 2), "ADR%": f"{adr:.2f}%",
                "EPS Qtr YoY": eps_g, "Rev Qtr YoY": rev_g,
                "P>50": "Yes" if cur > sma50 else "No", "P>200": "Yes" if cur > sma200 else "No",
                "% From 52W High": f"{((cur/h52w)-1)*100:.2f}%",
                # Performance-Grundlagen für Rankings
                "_raw_1w": ((cur / close.iloc[-min(5, len(df))]) - 1) * 100,
                "_raw_1m": ((cur / close.iloc[-min(21, len(df))]) - 1) * 100,
                "_raw_3m": ((cur / close.iloc[-min(63, len(df))]) - 1) * 100,
                "_raw_6m": ((cur / close.iloc[-min(126, len(df))]) - 1) * 100,
                "_raw_1y": ((cur / close.iloc[-min(252, len(df))]) - 1) * 100,
                "% 1D": ((cur / close.iloc[-2]) - 1) * 100 if len(df) > 1 else 0
            }
            results.append(res)
            
            if (i+1) % 50 == 0: print(f"> {i+1} Aktien erfolgreich verarbeitet...")
        except Exception as e:
            continue

    if not results:
        print("KRITISCHER FEHLER: Keine Aktien-Daten aufbereitet. Prüfe Internetverbindung oder Ticker-Format.")
        return

    df_final = pd.DataFrame(results)

    # RS-Rankings berechnen (0-99)
    for p in ['1w', '1m', '3m', '6m', '1y']:
        col = f"_raw_{p}"
        if col in df_final.columns:
            df_final[f"RS {p.upper()}"] = calculate_rs_rank(df_final[col])
            df_final[f"% {p.upper()}"] = df_final[col].map(lambda x: f"{x:.2f}%")
            df_final.drop(columns=[col], inplace=True)

    df_final["% 1D"] = df_final["% 1D"].map(lambda x: f"{x:.2f}%")

    # Spaltenordnung für app.py
    cols_order = ["Ticker", "Name", "Price", "Sector", "Industry", "Market Cap", "Volume", "Rel Vol 1D", "ADR%", 
                  "EPS Qtr YoY", "Rev Qtr YoY", "P>50", "P>200", "% From 52W High", 
                  "RS 1W", "RS 1M", "RS 3M", "RS 6M", "RS 1Y", "% 1D", "% 1W", "% 1M", "% 3M", "% 6M", "% 1Y"]
    
    final_cols = [c for c in cols_order if c in df_final.columns]
    df_final[final_cols].to_csv("Data/Screener_Data.csv", index=False)
    
    # SPY Update
    try:
        spy = data["SPY"].dropna()
        spy_perf = ((spy[close_col].iloc[-1] / spy[close_col].iloc[-min(252, len(spy))]) - 1) * 100
        pd.DataFrame([{"Symbol": "SPY", "Price": round(spy[close_col].iloc[-1], 2), "% 1Y": f"{spy_perf:.2f}%"}]).to_csv("Data/SPY_Data.csv", index=False)
    except: pass
    
    print(f"\n--- ERFOLG: {len(df_final)} Aktien gespeichert ---")

if __name__ == "__main__":
    update_data()
