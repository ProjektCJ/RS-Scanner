import yfinance as yf
import pandas as pd
import os
import requests
import io

def get_tickers():
    headers = {"User-Agent": "Mozilla/5.0"}
    tickers = []
    for url, match in [('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', 'Symbol'), 
                        ('https://en.wikipedia.org/wiki/Nasdaq-100', 'Ticker')]:
        try:
            resp = requests.get(url, headers=headers, timeout=10)
            df = pd.read_html(io.StringIO(resp.text), match=match)[0]
            tickers += df[match].tolist()
        except: continue
    return sorted(list(set([str(t).strip().replace('.', '-') for t in tickers if str(t) != 'nan'])))

def calculate_rs_rank(series):
    if series.empty: return series
    return (series.rank(pct=True) * 99).fillna(0).astype(int)

def update_data():
    if not os.path.exists('Data'): os.makedirs('Data')
    symbols = get_tickers()
    
    # Download 2 Jahre für saubere 1Y-Performance
    data = yf.download(symbols + ["SPY"], period="2y", interval="1d", group_by='ticker', threads=False)
    
    results = []
    for t in symbols:
        try:
            if t not in data.columns.levels[0]: continue
            df = data[t].dropna(subset=['Close'])
            if len(df) < 200: continue
            
            close = df['Adj Close'] if 'Adj Close' in df.columns else df['Close']
            cur = float(close.iloc[-1])
            
            # Wir füllen Sektor erst einmal statisch, um Rate-Limits auf GitHub zu vermeiden
            res = {
                "Ticker": t,
                "Name": t,
                "Price": round(cur, 2),
                "Sector": "Equity",
                "_raw_1w": ((cur / close.iloc[-min(5, len(df))]) - 1) * 100,
                "_raw_1m": ((cur / close.iloc[-min(21, len(df))]) - 1) * 100,
                "_raw_3m": ((cur / close.iloc[-min(63, len(df))]) - 1) * 100,
                "_raw_6m": ((cur / close.iloc[-min(126, len(df))]) - 1) * 100,
                "_raw_1y": ((cur / close.iloc[-min(252, len(df))]) - 1) * 100,
                "% 1D": ((cur / close.iloc[-2]) - 1) * 100 if len(df) > 1 else 0
            }
            results.append(res)
        except: continue

    if not results: return

    df_final = pd.DataFrame(results)

    # RS-Rankings berechnen
    for p in ['1w', '1m', '3m', '6m', '1y']:
        col = f"_raw_{p}"
        df_final[f"RS {p.upper()}"] = calculate_rs_rank(df_final[col])
        df_final[f"% {p.upper()}"] = df_final[col].map(lambda x: f"{x:.2f}%")
        df_final.drop(columns=[col], inplace=True)

    df_final["% 1D"] = df_final["% 1D"].map(lambda x: f"{x:.2f}%")

    # ABSOLUTE REIHENFOLGE-KONTROLLE (Fix für 106% Fehler)
    # Diese Liste muss exakt zu den Spaltenköpfen deiner App passen
    cols_order = ["Ticker", "Name", "Price", "Sector", 
                  "RS 1W", "RS 1M", "RS 3M", "RS 6M", "RS 1Y", 
                  "% 1D", "% 1W", "% 1M", "% 3M", "% 6M", "% 1Y"]
    
    df_final[cols_order].to_csv("Data/Screener_Data.csv", index=False)
    
    # SPY Update
    try:
        spy = data["SPY"].dropna()
        spy_p = ((spy.iloc[-1]['Close'] / spy.iloc[-min(252, len(spy))]['Close']) - 1) * 100
        pd.DataFrame([{"Symbol": "SPY", "Price": round(spy.iloc[-1]['Close'], 2), "% 1Y": f"{spy_p:.2f}%"}]).to_csv("Data/SPY_Data.csv", index=False)
    except: pass
    
    print(f"Update fertig: {len(df_final)} Aktien.")

if __name__ == "__main__":
    update_data()
