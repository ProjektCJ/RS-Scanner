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
            resp = requests.get(url, headers=headers)
            df = pd.read_html(io.StringIO(resp.text), match=match)[0]
            tickers += df[match].tolist()
        except: continue
    return sorted(list(set([str(t).replace('.', '-') for t in tickers if str(t) != 'nan'])))

def calculate_rs_rank(series):
    return (series.rank(pct=True) * 99).fillna(0).astype(int)

def update_data():
    if not os.path.exists('Data'): os.makedirs('Data')
    symbols = get_tickers()
    data = yf.download(symbols + ["SPY"], period="2y", interval="1d", group_by='ticker', threads=False)
    
    results = []
    for t in symbols:
        try:
            if t not in data.columns.levels[0]: continue
            df = data[t].dropna()
            if len(df) < 252: continue
            
            close = df['Adj Close']
            cur = float(close.iloc[-1])
            
            # Fundamentals (Sektor-Abfrage)
            info = yf.Ticker(t).info
            
            res = {
                "Ticker": t,
                "Name": info.get('longName', t),
                "Price": round(cur, 2),
                "Sector": info.get('sector', 'Market Leader'), # Hier sitzt der Verschiebe-Faktor!
                # Performance-Grundlagen
                "_raw_1w": ((cur / close.iloc[-5]) - 1) * 100,
                "_raw_1m": ((cur / close.iloc[-21]) - 1) * 100,
                "_raw_3m": ((cur / close.iloc[-63]) - 1) * 100,
                "_raw_6m": ((cur / close.iloc[-126]) - 1) * 100,
                "_raw_1y": ((cur / close.iloc[-252]) - 1) * 100,
                "% 1D": ((cur / close.iloc[-2]) - 1) * 100
            }
            results.append(res)
        except: continue

    df_final = pd.DataFrame(results)

    # RS-Rankings berechnen
    for p in ['1w', '1m', '3m', '6m', '1y']:
        col = f"_raw_{p}"
        df_final[f"RS {p.upper()}"] = calculate_rs_rank(df_final[col])
        df_final[f"% {p.upper()}"] = df_final[col].map(lambda x: f"{x:.2f}%")
        df_final.drop(columns=[col], inplace=True)

    df_final["% 1D"] = df_final["% 1D"].map(lambda x: f"{x:.2f}%")

    # EXAKTE REIHENFOLGE FÜR DEINE APP (image_d52cfc.png)
    # Ticker, Name, Price, Sector, RS 1W, RS 1M, RS 3M, RS 6M, RS 1Y, % 1D...
    cols_order = ["Ticker", "Name", "Price", "Sector", 
                  "RS 1W", "RS 1M", "RS 3M", "RS 6M", "RS 1Y", 
                  "% 1D", "% 1W", "% 1M", "% 3M", "% 6M", "% 1Y"]
    
    df_final[cols_order].to_csv("Data/Screener_Data.csv", index=False)
    
    # SPY Update
    spy = data["SPY"].dropna()
    spy_perf = ((spy['Adj Close'].iloc[-1] / spy['Adj Close'].iloc[-252]) - 1) * 100
    pd.DataFrame([{"Symbol": "SPY", "Price": round(spy['Adj Close'].iloc[-1], 2), "% 1Y": f"{spy_perf:.2f}%"}]).to_csv("Data/SPY_Data.csv", index=False)
    print(f"Update abgeschlossen: {len(df_final)} Aktien geeicht.")

if __name__ == "__main__":
    update_data()
