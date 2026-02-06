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
    return sorted(list(set([str(t).strip().replace('.', '-') for t in tickers if str(t) != 'nan'])))

def update_data():
    if not os.path.exists('Data'): os.makedirs('Data')
    symbols = get_tickers()
    data = yf.download(symbols + ["SPY"], period="2y", interval="1d", group_by='ticker', threads=False)
    
    # --- SPY_DATA (Original Struktur: 9 Spalten) ---
    try:
        spy_df = data["SPY"].dropna()
        cur_spy = float(spy_df['Adj Close'].iloc[-1])
        s_p = lambda d: ((cur_spy / spy_df['Adj Close'].iloc[-min(d, len(spy_df))]) - 1) * 100
        pd.DataFrame([{
            "Symbol": "SPY", "Description": "State Street SPDR S&P 500 ETF", "Price": cur_spy, "Price - Currency": "USD",
            "Performance % 1 week": s_p(5), "Performance % 1 month": s_p(21), "Performance % 3 months": s_p(63),
            "Performance % 6 months": s_p(126), "Performance % 1 year": s_p(252)
        }]).to_csv("Data/SPY_Data.csv", index=False)
    except: print("Fehler bei SPY Update")

    # --- SCREENER_DATA (Original Struktur: 39 Spalten) ---
    # Liste exakt nach deiner Screener_Data.csv Vorlage
    orig_cols = [
        "Symbol", "Description", "Price", "Price - Currency", "Gap % 1 day", "Price Change % 1 day", 
        "Market capitalization", "Market capitalization - Currency", "Volume 1 day", "Volume Change % 1 day", 
        "Volume Change % 1 week", "Volume Change % 1 month", "Average Volume 30 days", "Relative Volume 1 day", 
        "Relative Volume 1 week", "Relative Volume 1 month", "Free float", "Performance % 1 week", 
        "Performance % 1 month", "Performance % 3 months", "Performance % 6 months", "Performance % 1 year", 
        "Earnings per share diluted growth %, Quarterly YoY", "Earnings per share diluted growth %, Annual YoY", 
        "Revenue growth %, Quarterly YoY", "Revenue growth %, Annual YoY", "Return on equity %, Trailing 12 months", 
        "Pretax margin %, Trailing 12 months", "High 52 weeks", "High 52 weeks - Currency", "High All Time", 
        "High All Time - Currency", "Average Daily Range %", "Average True Range % (14) 1 day", 
        "Simple Moving Average (200) 1 day", "Simple Moving Average (50) 1 day", "Simple Moving Average (20) 1 day", 
        "Simple Moving Average (10) 1 day", "Sector"
    ]

    results = []
    for t in symbols:
        try:
            if t not in data.columns.levels[0]: continue
            df = data[t].dropna(subset=['Close'])
            close = df['Adj Close']
            cur = float(close.iloc[-1])
            def p(d): return ((cur / close.iloc[-min(d, len(df))]) - 1) * 100
            
            # Wir füllen nur das ab, was Yahoo stabil liefert, den Rest auf 0.0 für die Struktur
            res = {
                "Symbol": t, "Description": t, "Price": cur, "Price - Currency": "USD",
                "Gap % 1 day": ((df['Open'].iloc[-1]/df['Close'].iloc[-2])-1)*100 if len(df)>1 else 0,
                "Price Change % 1 day": p(2), "Market capitalization": 0.0, "Market capitalization - Currency": "USD",
                "Volume 1 day": df['Volume'].iloc[-1], "Volume Change % 1 day": 0.0, "Volume Change % 1 week": 0.0, 
                "Volume Change % 1 month": 0.0, "Average Volume 30 days": df['Volume'].rolling(30).mean().iloc[-1],
                "Relative Volume 1 day": df['Volume'].iloc[-1]/df['Volume'].rolling(30).mean().iloc[-1] if len(df)>30 else 1,
                "Relative Volume 1 week": 0.0, "Relative Volume 1 month": 0.0, "Free float": 0.0,
                "Performance % 1 week": p(5), "Performance % 1 month": p(21), "Performance % 3 months": p(63),
                "Performance % 6 months": p(126), "Performance % 1 year": p(252),
                "Earnings per share diluted growth %, Quarterly YoY": 0.0, "Earnings per share diluted growth %, Annual YoY": 0.0,
                "Revenue growth %, Quarterly YoY": 0.0, "Revenue growth %, Annual YoY": 0.0,
                "Return on equity %, Trailing 12 months": 0.0, "Pretax margin %, Trailing 12 months": 0.0,
                "High 52 weeks": df['High'].iloc[-min(252, len(df)):].max(), "High 52 weeks - Currency": "USD",
                "High All Time": df['High'].max(), "High All Time - Currency": "USD",
                "Average Daily Range %": (((df['High']/df['Low'])-1)*100).rolling(20).mean().iloc[-1],
                "Average True Range % (14) 1 day": 0.0,
                "Simple Moving Average (200) 1 day": close.rolling(200).mean().iloc[-1] if len(df)>=200 else cur,
                "Simple Moving Average (50) 1 day": close.rolling(50).mean().iloc[-1] if len(df)>=50 else cur,
                "Simple Moving Average (20) 1 day": close.rolling(20).mean().iloc[-1] if len(df)>=20 else cur,
                "Simple Moving Average (10) 1 day": close.rolling(10).mean().iloc[-1] if len(df)>=10 else cur,
                "Sector": "Market Leader"
            }
            results.append(res)
        except: continue

    pd.DataFrame(results).reindex(columns=orig_cols).to_csv("Data/Screener_Data.csv", index=False)
    print(f"Update fertig: {len(results)} Aktien.")

if __name__ == "__main__":
    update_data()
