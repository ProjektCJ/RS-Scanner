import yfinance as yf
import pandas as pd
import os
import requests
import io

def get_tickers():
    print("--- SCHRITT 1: Ticker-Listen laden ---")
    headers = {"User-Agent": "Mozilla/5.0"}
    tickers = []
    # S&P 500 & Nasdaq 100
    for url, match in [('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', 'Symbol'), 
                        ('https://en.wikipedia.org/wiki/Nasdaq-100', 'Ticker')]:
        try:
            resp = requests.get(url, headers=headers, timeout=10)
            df = pd.read_html(io.StringIO(resp.text), match=match)[0]
            tickers += df[match].tolist()
        except: continue
    
    # Bereinigung
    final_tickers = sorted(list(set([str(t).strip().replace('.', '-') for t in tickers if str(t) != 'nan'])))
    print(f"Ticker gefunden: {len(final_tickers)}")
    return final_tickers

def calculate_rs_rank(series):
    if series.empty: return series
    return (series.rank(pct=True) * 99).fillna(0).astype(int)

def update_data():
    if not os.path.exists('Data'): os.makedirs('Data')
    symbols = get_tickers()
    
    print(f"--- SCHRITT 2: Kurse laden ---")
    # Wichtig: SPY muss explizit dabei sein
    data = yf.download(symbols + ["SPY"], period="2y", interval="1d", group_by='ticker', threads=False)
    
    # --- SPY UPDATE VORZIEHEN (Damit es nicht vergessen wird) ---
    try:
        if "SPY" in data.columns.levels[0]:
            spy_df = data["SPY"].dropna(subset=['Close'])
            if len(spy_df) > 200:
                # Performance Berechnung
                spy_perf = ((spy_df['Adj Close'].iloc[-1] / spy_df['Adj Close'].iloc[-min(252, len(spy_df))]) - 1) * 100
                spy_price = spy_df['Adj Close'].iloc[-1]
                
                # Speichern
                spy_res = pd.DataFrame([{"Symbol": "SPY", "Price": round(spy_price, 2), "% 1Y": f"{spy_perf:.2f}%"}])
                spy_res.to_csv("Data/SPY_Data.csv", index=False)
                print("ERFOLG: SPY_Data.csv wurde aktualisiert.")
            else:
                print("WARNUNG: SPY Daten unvollständig.")
        else:
            print("FEHLER: SPY nicht im Download enthalten.")
    except Exception as e:
        print(f"FEHLER beim SPY Update: {e}")

    # --- AKTIEN UPDATE ---
    results = []
    print(f"--- SCHRITT 3: Aktien verarbeiten ---")
    
    for t in symbols:
        try:
            if t not in data.columns.levels[0]: continue
            df = data[t].dropna(subset=['Close'])
            if len(df) < 200: continue
            
            # Datenpunkte holen
            # Fallback: Falls Adj Close fehlt, nimm Close
            close = df['Adj Close'] if 'Adj Close' in df.columns else df['Close']
            cur = float(close.iloc[-1])
            highs = df['High']
            lows = df['Low']
            vol = df['Volume']
            
            # Berechnungen für Volume & ADR
            avg_vol = vol.rolling(20).mean().iloc[-1]
            if pd.isna(avg_vol): avg_vol = 0
            
            adr_val = (((highs/lows)-1)*100).rolling(20).mean().iloc[-1]
            if pd.isna(adr_val): adr_val = 0

            # Performance Rohdaten
            raw_1w = ((cur / close.iloc[-min(5, len(df))]) - 1) * 100
            raw_1m = ((cur / close.iloc[-min(21, len(df))]) - 1) * 100
            raw_3m = ((cur / close.iloc[-min(63, len(df))]) - 1) * 100
            raw_6m = ((cur / close.iloc[-min(126, len(df))]) - 1) * 100
            raw_1y = ((cur / close.iloc[-min(252, len(df))]) - 1) * 100
            raw_1d = ((cur / close.iloc[-2]) - 1) * 100 if len(df) > 1 else 0

            # Dictionary erstellen
            res = {
                "Ticker": t,
                "Name": t, # Yahoo Name oft langsam, wir nehmen Ticker als Fallback
                "Price": cur,
                "Volume": avg_vol,
                "ADR%": adr_val,
                "_raw_1w": raw_1w, "_raw_1m": raw_1m, "_raw_3m": raw_3m, 
                "_raw_6m": raw_6m, "_raw_1y": raw_1y,
                "% 1D": raw_1d,
                "% 1W": raw_1w,
                "% 1M": raw_1m,
                "% 3M": raw_3m,
                "% 6M": raw_6m,
                "% 1Y": raw_1y
            }
            results.append(res)
        except: continue

    if not results:
        print("KRITISCH: Keine Aktien verarbeitet.")
        return

    df_final = pd.DataFrame(results)

    # RS Rankings
    for p in ['1w', '1m', '3m', '6m', '1y']:
        df_final[f"RS {p.upper()}"] = calculate_rs_rank(df_final[f"_raw_{p}"])

    # --- FORMATIERUNG (Strings für die Anzeige) ---
    df_final["Price"] = df_final["Price"].map(lambda x: f"${x:.2f}")
    df_final["Volume"] = df_final["Volume"].map(lambda x: f"{x/1e6:.2f}M")
    df_final["ADR%"] = df_final["ADR%"].map(lambda x: f"{x:.2f}%")
    
    # Prozent-Formatierung für alle Performance Spalten
    perc_cols = ["% 1D", "% 1W", "% 1M", "% 3M", "% 6M", "% 1Y"]
    for col in perc_cols:
        df_final[col] = df_final[col].map(lambda x: f"{x:.2f}%")

    # --- FINALE SPALTEN-ORDNUNG ERZWINGEN ---
    # Das ist die exakte Reihenfolge aus deinem Original-Screenshot (image_d3d07e.png)
    # Ticker, Name, Price, Volume, ADR%, RS 1W...
    
    target_columns = [
        "Ticker", "Name", "Price", "Volume", "ADR%", 
        "RS 1W", "RS 1M", "RS 3M", "RS 6M", "RS 1Y", 
        "% 1D", "% 1W", "% 1M", "% 3M", "% 6M", "% 1Y"
    ]
    
    # Wir erstellen ein neues DataFrame nur mit diesen Spalten.
    # Falls eine Spalte fehlt, füllt Pandas sie mit NaN (verhindert Absturz)
    df_final = df_final.reindex(columns=target_columns, fill_value="N/A")
    
    # Speichern
    df_final.to_csv("Data/Screener_Data.csv", index=False)
    print(f"ERFOLG: {len(df_final)} Aktien im korrekten Format gespeichert.")

if __name__ == "__main__":
    update_data()
