from datetime import datetime, timezone
import os
import re

import numpy as np
import pandas as pd
import streamlit as st

# ============================================================
# CONFIG
# ============================================================
st.set_page_config(page_title="Relative Strength Stock Screener", layout="wide")

BENCHMARK = "SPY"

# NOTE: Your repo folder is "Data" (capital D). Linux is case-sensitive.
DATA_FILE = "Data/Screener_Data.csv"
SPY_FILE = "Data/SPY_Data.csv"


def _asof_ts():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


# =========================
# CSS (MATCH YOUR DASHBOARD)
# =========================
CSS = """
<style>
.block-container {max-width: 1750px; padding-top: 1.0rem; padding-bottom: 2rem;}
.section-title {font-weight: 900; font-size: 1.15rem; margin: 0.65rem 0 0.4rem 0;}
.small-muted {opacity: 0.75; font-size: 0.9rem;}
.hr {border-top: 1px solid rgba(255,255,255,0.12); margin: 14px 0;}
.card {
  border: 1px solid rgba(255,255,255,0.10);
  background: rgba(255,255,255,0.03);
  border-radius: 12px;
  padding: 12px 14px;
  margin-bottom: 12px;
}
.card h3{margin:0 0 8px 0; font-size: 1.02rem; font-weight: 950;}
.card .hint{opacity:0.72; font-size:0.88rem; margin-top:-2px; margin-bottom:10px;}

.pl-table-wrap {border-radius: 10px; overflow: hidden; border: 1px solid rgba(255,255,255,0.10);}
table.pl-table {border-collapse: collapse; width: 100%; font-size: 13px;}
table.pl-table thead th {
  position: sticky; top: 0;
  background: rgba(255,255,255,0.06);
  color: rgba(255,255,255,0.92);
  text-align: left;
  padding: 8px 10px;
  border-bottom: 1px solid rgba(255,255,255,0.12);
  font-weight: 900;
}
table.pl-table tbody td{
  padding: 7px 10px;
  border-bottom: 1px solid rgba(255,255,255,0.08);
  vertical-align: middle;
}
td.mono {font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;}
td.ticker {font-weight: 900;}
td.name {white-space: normal; line-height: 1.15;}

/* Tight RS GAP column */
th.rs-gap, td.rs-gap{
  width: 85px;
  min-width: 85px;
  max-width: 85px;
  white-space: nowrap;
}
</style>
"""
st.markdown(CSS, unsafe_allow_html=True)


# ============================================================
# HELPERS
# ============================================================
def normalize_ticker(t: str) -> str:
    t = (t or "").strip().upper()
    t = t.replace(" ", "")
    t = t.replace("/", "-")
    return t


def to_float_pct_series(s: pd.Series) -> pd.Series:
    """
    Converts percent-units to fractional returns.
    Example: 12.3 -> 0.123, "12.3%" -> 0.123
    """
    if s is None:
        return pd.Series(np.nan)

    if getattr(s.dtype, "kind", "") in "if":
        return pd.to_numeric(s, errors="coerce") / 100.0

    ss = s.astype(str).str.strip()
    ss = ss.str.replace("%", "", regex=False).str.replace(",", "", regex=False).str.strip()
    ss = ss.str.replace(r"[^0-9\.\-\+]", "", regex=True)
    return pd.to_numeric(ss, errors="coerce") / 100.0


def find_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    cols = [str(c) for c in df.columns]
    low = {c.lower().strip(): c for c in cols}

    for cand in candidates:
        cand_l = str(cand).lower().strip()
        if cand_l in low:
            return low[cand_l]

    for c in cols:
        cl = c.lower()
        for cand in candidates:
            if str(cand).lower() in cl:
                return c
    return None


def rs_bg(v):
    try:
        v = float(v)
    except:
        return ""
    if np.isnan(v):
        return ""
    x = (v - 1) / 98.0
    if x < 0.5:
        r = 255
        g = int(80 + (x / 0.5) * (180 - 80))
    else:
        r = int(255 - ((x - 0.5) / 0.5) * (255 - 40))
        g = 200
    b = 60
    return (
        f"background-color: rgb({r},{g},{b}); color:#0B0B0B; font-weight:900; "
        "border-radius:6px; padding:2px 6px; display:inline-block; min-width:32px; text-align:center;"
    )


def pct_style(v):
    try:
        v = float(v)
    except:
        return ""
    if np.isnan(v):
        return ""
    if v > 0:
        return "color:#7CFC9A; font-weight:800;"
    if v < 0:
        return "color:#FF6B6B; font-weight:800;"
    return "opacity:0.9; font-weight:700;"


def fmt_price(v):
    try:
        if v is None or (isinstance(v, float) and np.isnan(v)):
            return ""
        return f"${float(v):,.2f}"
    except:
        return ""


def fmt_pct(v):
    try:
        if v is None or (isinstance(v, float) and np.isnan(v)):
            return ""
        return f"{float(v):.2%}"
    except:
        return ""


def fmt_rs(v):
    try:
        if v is None or (isinstance(v, float) and np.isnan(v)):
            return ""
        return f"{float(v):.0f}"
    except:
        return ""


def fmt_big_num(v):
    try:
        if v is None or (isinstance(v, float) and np.isnan(v)):
            return ""
        x = float(v)
        ax = abs(x)
        if ax >= 1e12:
            return f"{x/1e12:.2f}T"
        if ax >= 1e9:
            return f"{x/1e9:.2f}B"
        if ax >= 1e6:
            return f"{x/1e6:.2f}M"
        if ax >= 1e3:
            return f"{x/1e3:.2f}K"
        return f"{x:,.0f}"
    except:
        return ""


def render_table_html(df: pd.DataFrame, columns: list[str], height_px: int = 900):
    ths = []
    for c in columns:
        th_cls = "rs-gap" if c == "RS GAP" else ""
        ths.append(f'<th class="{th_cls}">{c}</th>')
    th = "".join(ths)

    trs = []
    for _, row in df.iterrows():
        tds = []
        for c in columns:
            val = row.get(c, "")
            td_class = ""

            if c == "Ticker":
                td_class = "ticker"
            elif c == "Name":
                td_class = "name"
            elif c == "RS GAP":
                td_class = "mono rs-gap"
            elif c in ["Price", "Mkt Cap", "Volume", "Avg Vol 30D", "Float", "SMA200", "SMA50", "SMA20", "SMA10"]:
                td_class = "mono"
            elif c.startswith("% ") or c.startswith("RS ") or c in [
                "ADR%", "ATR%", "% From 52W High", "% From ATH",
                "Vol Chg 1D", "Vol Chg 1W", "Vol Chg 1M",
                "EPS Qtr YoY", "EPS Ann YoY", "Rev Qtr YoY", "Rev Ann YoY",
                "ROE TTM", "PreTax Mgn TTM"
            ]:
                td_class = "mono"
            elif c in ["P>200", "P>50", "P>20", "P>10", "50>200"]:
                td_class = "mono"

            if isinstance(val, (bool, np.bool_)) and c in ["P>200", "P>50", "P>20", "P>10", "50>200"]:
                cell_html = "✓" if bool(val) else ""
            elif c == "Price":
                cell_html = fmt_price(val)
            elif c in ["SMA200", "SMA50", "SMA20", "SMA10"]:
                cell_html = fmt_price(val)
            elif c in ["Mkt Cap", "Volume", "Avg Vol 30D", "Float"]:
                cell_html = fmt_big_num(val)
            elif c.startswith("% ") or c in [
                "ADR%", "ATR%", "% From 52W High", "% From ATH",
                "Vol Chg 1D", "Vol Chg 1W", "Vol Chg 1M",
                "EPS Qtr YoY", "EPS Ann YoY", "Rev Qtr YoY", "Rev Ann YoY",
                "ROE TTM", "PreTax Mgn TTM"
            ]:
                txt = fmt_pct(val)
                stl = pct_style(val)
                cell_html = f'<span style="{stl}">{txt}</span>' if stl and txt != "" else txt
            elif c.startswith("RS "):
                txt = fmt_rs(val)
                stl = rs_bg(val)
                cell_html = f'<span style="{stl}">{txt}</span>' if stl and txt != "" else txt
            elif c == "RS GAP":
                try:
                    if val is None or (isinstance(val, float) and np.isnan(val)):
                        cell_html = ""
                    else:
                        cell_html = f"{float(val):+.0f}"
                except:
                    cell_html = ""
            else:
                cell_html = "" if (val is None or (isinstance(val, float) and np.isnan(val))) else str(val)

            tds.append(f'<td class="{td_class}">{cell_html}</td>')

        trs.append("<tr>" + "".join(tds) + "</tr>")

    table = f"""
    <div class="pl-table-wrap" style="max-height:{height_px}px; overflow:auto;">
      <table class="pl-table">
        <thead><tr>{th}</tr></thead>
        <tbody>
          {''.join(trs)}
        </tbody>
      </table>
    </div>
    """
    st.markdown(table, unsafe_allow_html=True)


# ============================================================
# TICKER LOOKUP (NEW)
# ============================================================
TICKER_LOOKUP_DEFAULT_COLS = [
    "Ticker", "Name", "Price",
    "RS 1W", "RS 1M", "RS 3M", "RS 6M", "RS 1Y",
    "% 1D", "% 1W", "% 1M", "% 3M", "% 6M", "% 1Y",
]


def _init_ticker_lookup_state():
    if "tl_enabled" not in st.session_state:
        st.session_state["tl_enabled"] = False
    if "tl_ticker" not in st.session_state:
        st.session_state["tl_ticker"] = ""
    if "tl_cols" not in st.session_state:
        st.session_state["tl_cols"] = TICKER_LOOKUP_DEFAULT_COLS.copy()


def _tl_set_default():
    st.session_state["tl_cols"] = TICKER_LOOKUP_DEFAULT_COLS.copy()


def _tl_set_all(df_cols: list[str]):
    # only include columns that actually exist
    st.session_state["tl_cols"] = [c for c in df_cols if c in df_cols]


def _tl_clear():
    st.session_state["tl_cols"] = ["Ticker"]


def _tl_sidebar_controls(df_cols: list[str]):
    """
    Sidebar UI. Uses checkbox lists organized by groups.
    Returns (enabled: bool, ticker: str, selected_cols: list[str])
    """
    _init_ticker_lookup_state()

    st.markdown("### Ticker Lookup")
    enabled = st.checkbox("Enable", key="tl_enabled")

    if not enabled:
        return False, "", []

    ticker_in = st.text_input("Ticker (ex: NVDA)", key="tl_ticker", placeholder="NVDA")
    ticker = normalize_ticker(ticker_in)

    c1, c2, c3 = st.columns(3)
    with c1:
        st.button("Default", use_container_width=True, on_click=_tl_set_default)
    with c2:
        # add all columns (organized checkboxes below still control selection)
        if st.button("All", use_container_width=True):
            st.session_state["tl_cols"] = df_cols.copy()
    with c3:
        st.button("Clear", use_container_width=True, on_click=_tl_clear)

    # Group definitions using *your actual column names* (no guessing)
    groups = {
        "Core": ["Ticker", "Name", "Price", "Sector"],
        "RS": ["RS 1W", "RS 1M", "RS 3M", "RS 6M", "RS 1Y", "RS GAP"],
        "% Performance": ["% 1D", "% 1W", "% 1M", "% 3M", "% 6M", "% 1Y"],
        "Liquidity": ["Mkt Cap", "Float"],
        "Volume": ["Volume", "Avg Vol 30D", "Vol Chg 1D", "Vol Chg 1W", "Vol Chg 1M", "Rel Vol 1D", "Rel Vol 1W", "Rel Vol 1M"],
        "Fundamentals": ["Rev Qtr YoY", "Rev Ann YoY", "EPS Qtr YoY", "EPS Ann YoY", "ROE TTM", "PreTax Mgn TTM"],
        "Volatility": ["ADR%", "ATR%"],
        "Highs": ["% From 52W High", "% From ATH"],
        "Trend": ["P>200", "P>50", "P>20", "P>10", "50>200", "SMA200", "SMA50", "SMA20", "SMA10"],
    }

    # Only show options that exist in df
    groups = {g: [c for c in cols if c in df_cols] for g, cols in groups.items()}
    groups = {g: cols for g, cols in groups.items() if len(cols) > 0}

    selected = set([c for c in st.session_state["tl_cols"] if c in df_cols])

    st.caption("Select what fields to display for the ticker dashboard:")

    # Core groups expanded by default
    expand_default = {"Core", "RS", "% Performance"}

    for gname, cols in groups.items():
        with st.expander(gname, expanded=(gname in expand_default)):
            for col in cols:
                key = f"tl_cb_{gname}_{col}"
                checked = col in selected
                new_val = st.checkbox(col, value=checked, key=key)
                if new_val:
                    selected.add(col)
                else:
                    selected.discard(col)

    # Ensure Ticker stays included
    if "Ticker" in df_cols:
        selected.add("Ticker")

    # Persist order: keep in df_cols order
    ordered = [c for c in df_cols if c in selected]
    st.session_state["tl_cols"] = ordered

    # A small helper hint if they selected nothing meaningful
    if len(ordered) <= 1:
        st.info("Pick at least a few fields (or click Default).")

    return True, ticker, ordered


def render_ticker_lookup_dashboard(df_master: pd.DataFrame):
    """
    Uses df_master (your full df with RS/perf/fundamentals already computed).
    Renders the dashboard above scanner results when enabled and a ticker is provided.
    """
    if df_master is None or df_master.empty:
        return

    df_cols = df_master.columns.tolist()

    with st.sidebar:
        enabled, ticker, selected_cols = _tl_sidebar_controls(df_cols)

    if not enabled:
        return

    if not ticker:
        st.markdown('<div class="section-title">Ticker Lookup</div>', unsafe_allow_html=True)
        st.caption("Type a ticker in the sidebar to view its dashboard.")
        return

    # Find row
    tnorm = normalize_ticker(ticker)
    hit = df_master[df_master["Ticker"].astype(str).map(normalize_ticker) == tnorm]

    st.markdown('<div class="section-title">Ticker Lookup</div>', unsafe_allow_html=True)

    if hit.empty:
        # show suggestions
        starts = df_master[df_master["Ticker"].astype(str).map(normalize_ticker).str.startswith(tnorm, na=False)]
        if not starts.empty:
            st.warning(f"No exact match for {ticker}. Closest tickers:")
            st.write(", ".join(starts["Ticker"].head(15).tolist()))
        else:
            st.warning(f"No match found for {ticker}.")
        return

    row = hit.iloc[[0]].copy()  # keep as DF

    # Always enforce valid columns
    selected_cols = [c for c in (selected_cols or []) if c in row.columns]
    if len(selected_cols) == 0:
        selected_cols = ["Ticker"]

    # Render as your same styled table (single row)
    render_table_html(row[selected_cols], selected_cols, height_px=240)

    st.markdown('<div class="hr"></div>', unsafe_allow_html=True)


# ============================================================
# CUSTOM FILTER STATE HELPERS (RESET + PRESETS)
# ============================================================
CUSTOM_KEYS_DEFAULTS = {
    "cf_min_mktcap": 0.0,
    "cf_min_float": 0.0,

    "cf_min_vol1d": 0.0,
    "cf_min_avgvol30": 0.0,

    "cf_min_volchg_1d": 0.0,
    "cf_min_volchg_1w": 0.0,
    "cf_min_volchg_1m": 0.0,

    "cf_min_rvol_1d": 0.0,
    "cf_min_rvol_1w": 0.0,
    "cf_min_rvol_1m": 0.0,

    "cf_min_eps_q": 0.0,
    "cf_min_eps_a": 0.0,
    "cf_min_rev_q": 0.0,
    "cf_min_rev_a": 0.0,
    "cf_min_roe": 0.0,
    "cf_min_pretax": 0.0,

    "cf_max_from_52w": 0.0,
    "cf_max_from_ath": 0.0,

    "cf_min_adr": 0.0,
    "cf_min_atr": 0.0,

    "cf_sector_choice": "All",

    "cf_p_above_200": False,
    "cf_p_above_50": False,
    "cf_p_above_20": False,
    "cf_p_above_10": False,

    "cf_trend_template_1": False,

    "cf_preset": "None",
}


def _init_custom_state():
    for k, v in CUSTOM_KEYS_DEFAULTS.items():
        if k not in st.session_state:
            st.session_state[k] = v


# IMPORTANT: These are used as callbacks (on_click), so do NOT call st.rerun() here.
def _reset_custom_filters():
    for k, v in CUSTOM_KEYS_DEFAULTS.items():
        st.session_state[k] = v


def _apply_preset_super_performers():
    # Preset: Super Performers (Growth + Trend)
    # RS >= 87 (global RS slider)
    # Rev Qtr >= 15%, Rev Ann >= 15%
    # EPS Qtr >= 20%, EPS Ann >= 25%
    # Price above 200 + 50
    st.session_state["rs_min"] = 87

    st.session_state["cf_min_rev_q"] = 15.0
    st.session_state["cf_min_rev_a"] = 15.0
    st.session_state["cf_min_eps_q"] = 20.0
    st.session_state["cf_min_eps_a"] = 25.0

    st.session_state["cf_p_above_200"] = True
    st.session_state["cf_p_above_50"] = True


# ============================================================
# LOAD DATA
# ============================================================
@st.cache_data(show_spinner=False)
def load_csv(path: str) -> pd.DataFrame:
    return pd.read_csv(path)


if not os.path.exists(DATA_FILE):
    st.error(f"Could not find universe file at: {DATA_FILE}")
    st.stop()

if not os.path.exists(SPY_FILE):
    st.error(f"Could not find SPY file at: {SPY_FILE}")
    st.stop()

df_raw = load_csv(DATA_FILE)
spy_raw = load_csv(SPY_FILE)

if df_raw.empty:
    st.error(f"{DATA_FILE} loaded but is empty.")
    st.stop()

if spy_raw.empty:
    st.error(f"{SPY_FILE} loaded but is empty.")
    st.stop()

# ------------------------------------------------------------
# Map core columns (existing)
# ------------------------------------------------------------
c_symbol = find_col(df_raw, ["Symbol"])
c_name = find_col(df_raw, ["Description", "Name"])
c_price = find_col(df_raw, ["Price", "Last"])

c_1d = find_col(df_raw, ["Price Change % 1 day", "1 day", "daily"])
c_1w = find_col(df_raw, ["Performance % 1 week", "1 week", "weekly"])
c_1m = find_col(df_raw, ["Performance % 1 month", "1 month", "monthly"])
c_3m = find_col(df_raw, ["Performance % 3 months", "3 months", "quarter"])
c_6m = find_col(df_raw, ["Performance % 6 months", "6 months", "half"])
c_1y = find_col(df_raw, ["Performance % 1 year", "1 year", "annual"])

if not c_symbol:
    st.error("Universe CSV must include a Symbol column (or similar).")
    st.stop()

# ------------------------------------------------------------
# Map Custom filter columns (TradingView export)
# ------------------------------------------------------------
c_mktcap = find_col(df_raw, ["Market capitalization"])
c_vol1d = find_col(df_raw, ["Volume 1 day", "Volume"])
c_volchg_1d = find_col(df_raw, ["Volume Change % 1 day"])
c_volchg_1w = find_col(df_raw, ["Volume Change % 1 week"])
c_volchg_1m = find_col(df_raw, ["Volume Change % 1 month"])
c_avgvol30 = find_col(df_raw, ["Average Volume 30 days"])
c_rvol1d = find_col(df_raw, ["Relative Volume 1 day"])
c_rvol1w = find_col(df_raw, ["Relative Volume 1 week"])
c_rvol1m = find_col(df_raw, ["Relative Volume 1 month"])
c_float = find_col(df_raw, ["Free float"])

c_eps_q = find_col(df_raw, ["Earnings per share diluted growth %, Quarterly YoY"])
c_eps_a = find_col(df_raw, ["Earnings per share diluted growth %, Annual YoY"])
c_rev_q = find_col(df_raw, ["Revenue growth %, Quarterly YoY"])
c_rev_a = find_col(df_raw, ["Revenue growth %, Annual YoY"])
c_roe = find_col(df_raw, ["Return on equity %, Trailing 12 months"])
c_pretax = find_col(df_raw, ["Pretax margin %, Trailing 12 months"])

c_high52 = find_col(df_raw, ["High 52 weeks"])
c_ath = find_col(df_raw, ["High All Time"])

c_adr = find_col(df_raw, ["Average Daily Range %"])
c_atr = find_col(df_raw, ["Average True Range % (14) 1 day", "Average True Range %"])

c_sma200 = find_col(df_raw, ["Simple Moving Average (200) 1 day"])
c_sma50 = find_col(df_raw, ["Simple Moving Average (50) 1 day"])
c_sma20 = find_col(df_raw, ["Simple Moving Average (20) 1 day"])
c_sma10 = find_col(df_raw, ["Simple Moving Average (10) 1 day"])

c_sector = find_col(df_raw, ["Sector"])

# ------------------------------------------------------------
# SPY column map
# ------------------------------------------------------------
spy_symbol = find_col(spy_raw, ["Symbol"]) or c_symbol
spy_1w = find_col(spy_raw, [c_1w]) if c_1w else None
spy_1m = find_col(spy_raw, [c_1m]) if c_1m else None
spy_3m = find_col(spy_raw, [c_3m]) if c_3m else None
spy_6m = find_col(spy_raw, [c_6m]) if c_6m else None
spy_1y = find_col(spy_raw, [c_1y]) if c_1y else None

# ============================================================
# BUILD UNIVERSE FRAME
# ============================================================
df = pd.DataFrame()
df["Ticker"] = df_raw[c_symbol].astype(str).map(normalize_ticker)
df = df[df["Ticker"].str.len() > 0].drop_duplicates(subset=["Ticker"]).copy()
df["Name"] = df_raw[c_name].astype(str) if c_name else df["Ticker"]
df["Price"] = pd.to_numeric(df_raw[c_price], errors="coerce") if c_price else np.nan

# Returns (fractional)
df["r_1d"] = to_float_pct_series(df_raw[c_1d]) if c_1d else np.nan
df["r_1w"] = to_float_pct_series(df_raw[c_1w]) if c_1w else np.nan
df["r_1m"] = to_float_pct_series(df_raw[c_1m]) if c_1m else np.nan
df["r_3m"] = to_float_pct_series(df_raw[c_3m]) if c_3m else np.nan
df["r_6m"] = to_float_pct_series(df_raw[c_6m]) if c_6m else np.nan
df["r_1y"] = to_float_pct_series(df_raw[c_1y]) if c_1y else np.nan

# Custom raw numeric fields
df["Mkt Cap"] = pd.to_numeric(df_raw[c_mktcap], errors="coerce") if c_mktcap else np.nan
df["Volume"] = pd.to_numeric(df_raw[c_vol1d], errors="coerce") if c_vol1d else np.nan
df["Avg Vol 30D"] = pd.to_numeric(df_raw[c_avgvol30], errors="coerce") if c_avgvol30 else np.nan
df["Float"] = pd.to_numeric(df_raw[c_float], errors="coerce") if c_float else np.nan

# Volume changes + growth/margins as FRACTIONS
df["Vol Chg 1D"] = to_float_pct_series(df_raw[c_volchg_1d]) if c_volchg_1d else np.nan
df["Vol Chg 1W"] = to_float_pct_series(df_raw[c_volchg_1w]) if c_volchg_1w else np.nan
df["Vol Chg 1M"] = to_float_pct_series(df_raw[c_volchg_1m]) if c_volchg_1m else np.nan

df["Rel Vol 1D"] = pd.to_numeric(df_raw[c_rvol1d], errors="coerce") if c_rvol1d else np.nan
df["Rel Vol 1W"] = pd.to_numeric(df_raw[c_rvol1w], errors="coerce") if c_rvol1w else np.nan
df["Rel Vol 1M"] = pd.to_numeric(df_raw[c_rvol1m], errors="coerce") if c_rvol1m else np.nan

df["EPS Qtr YoY"] = to_float_pct_series(df_raw[c_eps_q]) if c_eps_q else np.nan
df["EPS Ann YoY"] = to_float_pct_series(df_raw[c_eps_a]) if c_eps_a else np.nan
df["Rev Qtr YoY"] = to_float_pct_series(df_raw[c_rev_q]) if c_rev_q else np.nan
df["Rev Ann YoY"] = to_float_pct_series(df_raw[c_rev_a]) if c_rev_a else np.nan
df["ROE TTM"] = to_float_pct_series(df_raw[c_roe]) if c_roe else np.nan
df["PreTax Mgn TTM"] = to_float_pct_series(df_raw[c_pretax]) if c_pretax else np.nan

df["_high52"] = pd.to_numeric(df_raw[c_high52], errors="coerce") if c_high52 else np.nan
df["_ath"] = pd.to_numeric(df_raw[c_ath], errors="coerce") if c_ath else np.nan

df["ADR%"] = to_float_pct_series(df_raw[c_adr]) if c_adr else np.nan
df["ATR%"] = to_float_pct_series(df_raw[c_atr]) if c_atr else np.nan

df["_sma200"] = pd.to_numeric(df_raw[c_sma200], errors="coerce") if c_sma200 else np.nan
df["_sma50"] = pd.to_numeric(df_raw[c_sma50], errors="coerce") if c_sma50 else np.nan
df["_sma20"] = pd.to_numeric(df_raw[c_sma20], errors="coerce") if c_sma20 else np.nan
df["_sma10"] = pd.to_numeric(df_raw[c_sma10], errors="coerce") if c_sma10 else np.nan

# Display MA values
df["SMA200"] = df["_sma200"]
df["SMA50"] = df["_sma50"]
df["SMA20"] = df["_sma20"]
df["SMA10"] = df["_sma10"]

df["Sector"] = df_raw[c_sector].astype(str) if c_sector else ""

# % from highs (fractional, usually negative)
df["% From 52W High"] = np.where(
    np.isfinite(df["_high52"]) & (df["_high52"] > 0) & np.isfinite(df["Price"]),
    (df["Price"] / df["_high52"]) - 1.0,
    np.nan,
)
df["% From ATH"] = np.where(
    np.isfinite(df["_ath"]) & (df["_ath"] > 0) & np.isfinite(df["Price"]),
    (df["Price"] / df["_ath"]) - 1.0,
    np.nan,
)

# Price above MA booleans
df["P>200"] = np.where(np.isfinite(df["Price"]) & np.isfinite(df["_sma200"]), df["Price"] > df["_sma200"], False)
df["P>50"] = np.where(np.isfinite(df["Price"]) & np.isfinite(df["_sma50"]), df["Price"] > df["_sma50"], False)
df["P>20"] = np.where(np.isfinite(df["Price"]) & np.isfinite(df["_sma20"]), df["Price"] > df["_sma20"], False)
df["P>10"] = np.where(np.isfinite(df["Price"]) & np.isfinite(df["_sma10"]), df["Price"] > df["_sma10"], False)
df["50>200"] = np.where(np.isfinite(df["_sma50"]) & np.isfinite(df["_sma200"]), df["_sma50"] > df["_sma200"], False)

# ============================================================
# SPY RETURNS
# ============================================================
spy_raw["__sym__"] = spy_raw[spy_symbol].astype(str).map(normalize_ticker)
spy_row = spy_raw[spy_raw["__sym__"] == normalize_ticker(BENCHMARK)]
if spy_row.empty:
    st.error(f"No row found for {BENCHMARK} in {SPY_FILE}. Make sure Symbol=SPY exists.")
    st.stop()
spy_row = spy_row.iloc[0]


def spy_ret(col):
    if not col or col not in spy_raw.columns:
        return np.nan
    return float(to_float_pct_series(pd.Series([spy_row[col]])).iloc[0])


b_1w = spy_ret(spy_1w)
b_1m = spy_ret(spy_1m)
b_3m = spy_ret(spy_3m)
b_6m = spy_ret(spy_6m)
b_1y = spy_ret(spy_1y)


def rel_ret(r: pd.Series, b: float) -> pd.Series:
    if not np.isfinite(b):
        return pd.Series(np.nan, index=r.index)
    return (1.0 + r) / (1.0 + b) - 1.0


df["rr_1w"] = rel_ret(df["r_1w"], b_1w)
df["rr_1m"] = rel_ret(df["r_1m"], b_1m)
df["rr_3m"] = rel_ret(df["r_3m"], b_3m)
df["rr_6m"] = rel_ret(df["r_6m"], b_6m)
df["rr_1y"] = rel_ret(df["r_1y"], b_1y)


def to_rs_1_99(s: pd.Series) -> pd.Series:
    x = pd.to_numeric(s, errors="coerce")
    return (x.rank(pct=True) * 99).round().clip(1, 99)


df["RS 1W"] = to_rs_1_99(df["rr_1w"])
df["RS 1M"] = to_rs_1_99(df["rr_1m"])
df["RS 3M"] = to_rs_1_99(df["rr_3m"])
df["RS 6M"] = to_rs_1_99(df["rr_6m"])
df["RS 1Y"] = to_rs_1_99(df["rr_1y"])

# Display % columns (absolute)
df["% 1D"] = df["r_1d"]
df["% 1W"] = df["r_1w"]
df["% 1M"] = df["r_1m"]
df["% 3M"] = df["r_3m"]
df["% 6M"] = df["r_6m"]
df["% 1Y"] = df["r_1y"]


# ============================================================
# UI
# ============================================================
st.title("Relative Strength Stock Screener")
st.caption(f"As of: {_asof_ts()} • RS Benchmark: {BENCHMARK}")

with st.sidebar:
    st.subheader("Controls")

    primary_tf = st.selectbox(
        "Rank by",
        ["RS 1M", "RS 3M", "RS 6M", "RS 1Y", "RS 1W"],
        index=0,
        key="primary_tf",
    )

    rs_min = st.slider("Minimum RS (Primary)", 1, 99, 70, 1, key="rs_min")

    mode = st.selectbox(
        "Scan Mode",
        [
            "Primary timeframe only",
            "All timeframes >= threshold",
            "Accelerating",
            "Decelerating",
            "Custom",
        ],
        index=0,
        key="mode",
    )

    accel_decel_mode = mode in ["Accelerating", "Decelerating"]
    custom_mode = mode == "Custom"

    if accel_decel_mode:
        rs_gap = st.slider("Acceleration/Deceleration Strength (RS Gap)", 0, 60, 15, 1, key="rs_gap")
        strict_chain = st.checkbox("Require smooth trend (1Y→6M→3M→1M)", value=True, key="strict_chain")

        sort_mode = st.selectbox(
            "Sort results by",
            ["RS Gap (shift)", "Primary timeframe"],
            index=0,
            key="sort_mode",
        )
    else:
        rs_gap = 0
        strict_chain = False
        sort_mode = "Primary timeframe"

    # Defaults so variables always exist
    min_mktcap = min_vol1d = min_avgvol30 = min_float = 0.0
    min_volchg_1d = min_volchg_1w = min_volchg_1m = 0.0
    min_rvol_1d = min_rvol_1w = min_rvol_1m = 0.0
    min_eps_q = min_eps_a = min_rev_q = min_rev_a = 0.0
    min_roe = min_pretax = 0.0
    max_from_52w = max_from_ath = 0.0
    min_adr = min_atr = 0.0
    sector_choice = "All"
    p_above_200 = p_above_50 = p_above_20 = p_above_10 = False
    trend_template_1 = False

    if custom_mode:
        _init_custom_state()

        st.markdown("### Custom Filters")

        preset = st.selectbox(
            "Preset",
            ["None", "Super Performers (Growth + Trend)"],
            index=0,
            key="cf_preset",
        )

        c1, c2 = st.columns(2)
        with c1:
            if preset == "Super Performers (Growth + Trend)":
                st.button(
                    "Apply Preset",
                    use_container_width=True,
                    on_click=_apply_preset_super_performers,
                )
            else:
                st.button("Apply Preset", use_container_width=True, disabled=True)
        with c2:
            st.button(
                "Reset Filters",
                use_container_width=True,
                on_click=_reset_custom_filters,
            )

        st.caption("Collapse sections to keep it simple. Leave inputs at 0 / unchecked to ignore.")

        # --------------------------------------------------------
        # Missing column warnings (optional but helpful)
        # --------------------------------------------------------
        missing = []
        if c_mktcap is None: missing.append("Market Cap")
        if c_float is None: missing.append("Float")
        if c_avgvol30 is None: missing.append("Avg Vol 30D")
        if c_rvol1d is None: missing.append("Rel Vol 1D")
        if c_eps_q is None: missing.append("EPS Qtr YoY")
        if c_rev_q is None: missing.append("Rev Qtr YoY")
        if c_roe is None: missing.append("ROE TTM")
        if c_pretax is None: missing.append("PreTax Margin TTM")
        if c_sma200 is None: missing.append("SMA200")
        if c_sma50 is None: missing.append("SMA50")
        if c_sector is None: missing.append("Sector")
        if missing:
            st.info("Missing in CSV (filters may not work): " + ", ".join(missing))

        with st.expander("Liquidity (Market Cap, Float)", expanded=False):
            min_mktcap = st.number_input(
                "Market Cap (Min)",
                min_value=0.0,
                step=1_000_000.0,
                format="%.0f",
                key="cf_min_mktcap",
            )
            min_float = st.number_input(
                "Float (Min)",
                min_value=0.0,
                step=1_000_000.0,
                format="%.0f",
                key="cf_min_float",
            )

        with st.expander("Volume (Raw, Avg, Change, Relative)", expanded=False):
            min_vol1d = st.number_input(
                "Volume 1D (Min)",
                min_value=0.0,
                step=100_000.0,
                format="%.0f",
                key="cf_min_vol1d",
            )
            min_avgvol30 = st.number_input(
                "Avg Volume 30D (Min)",
                min_value=0.0,
                step=100_000.0,
                format="%.0f",
                key="cf_min_avgvol30",
            )

            st.markdown("**Volume Change % (Min)**")
            min_volchg_1d = st.number_input(
                "Vol Change % 1D (Min)",
                min_value=0.0,
                step=5.0,
                format="%.2f",
                key="cf_min_volchg_1d",
            )
            min_volchg_1w = st.number_input(
                "Vol Change % 1W (Min)",
                min_value=0.0,
                step=5.0,
                format="%.2f",
                key="cf_min_volchg_1w",
            )
            min_volchg_1m = st.number_input(
                "Vol Change % 1M (Min)",
                min_value=0.0,
                step=5.0,
                format="%.2f",
                key="cf_min_volchg_1m",
            )

            st.markdown("**Relative Volume (Min)**")
            min_rvol_1d = st.number_input(
                "Rel Vol 1D (Min)",
                min_value=0.0,
                step=0.1,
                format="%.2f",
                key="cf_min_rvol_1d",
            )
            min_rvol_1w = st.number_input(
                "Rel Vol 1W (Min)",
                min_value=0.0,
                step=0.1,
                format="%.2f",
                key="cf_min_rvol_1w",
            )
            min_rvol_1m = st.number_input(
                "Rel Vol 1M (Min)",
                min_value=0.0,
                step=0.1,
                format="%.2f",
                key="cf_min_rvol_1m",
            )

        with st.expander("Fundamentals (Growth + Quality)", expanded=False):
            st.markdown("**Growth % YoY (Min)**")
            min_rev_q = st.number_input(
                "Revenue Growth Quarterly % YoY (Min)",
                min_value=0.0,
                step=5.0,
                format="%.2f",
                key="cf_min_rev_q",
            )
            min_rev_a = st.number_input(
                "Revenue Growth Annual % YoY (Min)",
                min_value=0.0,
                step=5.0,
                format="%.2f",
                key="cf_min_rev_a",
            )
            min_eps_q = st.number_input(
                "EPS Growth Quarterly % YoY (Min)",
                min_value=0.0,
                step=5.0,
                format="%.2f",
                key="cf_min_eps_q",
            )
            min_eps_a = st.number_input(
                "EPS Growth Annual % YoY (Min)",
                min_value=0.0,
                step=5.0,
                format="%.2f",
                key="cf_min_eps_a",
            )

            st.markdown("**Quality % (Min)**")
            min_roe = st.number_input(
                "ROE % TTM (Min)",
                min_value=0.0,
                step=1.0,
                format="%.2f",
                key="cf_min_roe",
            )
            min_pretax = st.number_input(
                "Pre-Tax Margin % TTM (Min)",
                min_value=0.0,
                step=1.0,
                format="%.2f",
                key="cf_min_pretax",
            )

        with st.expander("Volatility (ADR / ATR)", expanded=False):
            min_adr = st.number_input(
                "ADR% (Min)",
                min_value=0.0,
                step=0.5,
                format="%.2f",
                key="cf_min_adr",
            )
            min_atr = st.number_input(
                "ATR% (Min)",
                min_value=0.0,
                step=0.5,
                format="%.2f",
                key="cf_min_atr",
            )

        with st.expander("Distance From Highs", expanded=False):
            max_from_52w = st.number_input(
                "% From 52W High (Max distance, e.g. 15 = within 15%)",
                min_value=0.0,
                step=1.0,
                format="%.2f",
                key="cf_max_from_52w",
            )
            max_from_ath = st.number_input(
                "% From All-Time High (Max distance)",
                min_value=0.0,
                step=1.0,
                format="%.2f",
                key="cf_max_from_ath",
            )

        with st.expander("Sector", expanded=False):
            sectors = sorted([s for s in df["Sector"].dropna().unique().tolist() if str(s).strip() != ""])
            sector_choice = st.selectbox("Sector", ["All"] + sectors, key="cf_sector_choice")

        with st.expander("Trend (Moving Averages)", expanded=False):
            p_above_200 = st.checkbox("Price Above 200MA", key="cf_p_above_200")
            p_above_50 = st.checkbox("Price Above 50MA", key="cf_p_above_50")
            p_above_20 = st.checkbox("Price Above 20MA", key="cf_p_above_20")
            p_above_10 = st.checkbox("Price Above 10MA", key="cf_p_above_10")
            trend_template_1 = st.checkbox("Trend Template 1 (P>200 & P>50 & 50>200)", key="cf_trend_template_1")

    max_results = st.slider("Max Results", 25, 2000, 200, 25, key="max_results")


# ============================================================
# SCAN LOGIC
# ============================================================
bench_t = normalize_ticker(BENCHMARK)
df_univ = df[df["Ticker"] != bench_t].copy()

# RS GAP helper (used only in accel/decel)
df_univ["RS GAP"] = (
    pd.to_numeric(df_univ["RS 1M"], errors="coerce")
    - pd.to_numeric(df_univ["RS 1Y"], errors="coerce")
)

rs_cols_all = ["RS 1W", "RS 1M", "RS 3M", "RS 6M", "RS 1Y"]

if mode == "Primary timeframe only":
    df_f = df_univ[df_univ[primary_tf].fillna(0) >= rs_min].copy()
    tie = "RS 1Y" if "RS 1Y" in df_f.columns else primary_tf
    df_f = df_f.sort_values([primary_tf, tie], ascending=[False, False])

elif mode == "All timeframes >= threshold":
    cond = True
    for c in rs_cols_all:
        cond = cond & (df_univ[c].fillna(0) >= rs_min)
    df_f = df_univ[cond].copy()
    tie = "RS 1Y" if "RS 1Y" in df_f.columns else primary_tf
    df_f = df_f.sort_values([primary_tf, tie], ascending=[False, False])

elif mode == "Accelerating":
    cond = (df_univ[primary_tf].fillna(0) >= rs_min)
    cond = cond & (df_univ["RS GAP"].fillna(-999) >= rs_gap)

    if strict_chain:
        cond = cond & (df_univ["RS 1Y"] <= df_univ["RS 6M"])
        cond = cond & (df_univ["RS 6M"] <= df_univ["RS 3M"])
        cond = cond & (df_univ["RS 3M"] <= df_univ["RS 1M"])

    df_f = df_univ[cond].copy()

    if sort_mode == "RS Gap (shift)":
        df_f = df_f.sort_values(["RS GAP", "RS 1M"], ascending=[False, False])
    else:
        tie = "RS 1Y" if "RS 1Y" in df_f.columns else primary_tf
        df_f = df_f.sort_values([primary_tf, tie], ascending=[False, False])

elif mode == "Decelerating":
    cond = (df_univ[primary_tf].fillna(0) >= rs_min)
    cond = cond & ((-df_univ["RS GAP"]).fillna(-999) >= rs_gap)

    if strict_chain:
        cond = cond & (df_univ["RS 1M"] <= df_univ["RS 3M"])
        cond = cond & (df_univ["RS 3M"] <= df_univ["RS 6M"])
        cond = cond & (df_univ["RS 6M"] <= df_univ["RS 1Y"])

    df_f = df_univ[cond].copy()

    if sort_mode == "RS Gap (shift)":
        df_f = df_f.sort_values(["RS GAP", "RS 1Y"], ascending=[True, False])
    else:
        tie = "RS 1Y" if "RS 1Y" in df_f.columns else primary_tf
        df_f = df_f.sort_values([primary_tf, tie], ascending=[False, False])

else:  # CUSTOM (RS-first, then layer filters)
    _init_custom_state()

    min_mktcap = float(st.session_state["cf_min_mktcap"])
    min_float = float(st.session_state["cf_min_float"])

    min_vol1d = float(st.session_state["cf_min_vol1d"])
    min_avgvol30 = float(st.session_state["cf_min_avgvol30"])

    min_volchg_1d = float(st.session_state["cf_min_volchg_1d"])
    min_volchg_1w = float(st.session_state["cf_min_volchg_1w"])
    min_volchg_1m = float(st.session_state["cf_min_volchg_1m"])

    min_rvol_1d = float(st.session_state["cf_min_rvol_1d"])
    min_rvol_1w = float(st.session_state["cf_min_rvol_1w"])
    min_rvol_1m = float(st.session_state["cf_min_rvol_1m"])

    min_eps_q = float(st.session_state["cf_min_eps_q"])
    min_eps_a = float(st.session_state["cf_min_eps_a"])
    min_rev_q = float(st.session_state["cf_min_rev_q"])
    min_rev_a = float(st.session_state["cf_min_rev_a"])
    min_roe = float(st.session_state["cf_min_roe"])
    min_pretax = float(st.session_state["cf_min_pretax"])

    max_from_52w = float(st.session_state["cf_max_from_52w"])
    max_from_ath = float(st.session_state["cf_max_from_ath"])

    min_adr = float(st.session_state["cf_min_adr"])
    min_atr = float(st.session_state["cf_min_atr"])

    sector_choice = str(st.session_state["cf_sector_choice"])

    p_above_200 = bool(st.session_state["cf_p_above_200"])
    p_above_50 = bool(st.session_state["cf_p_above_50"])
    p_above_20 = bool(st.session_state["cf_p_above_20"])
    p_above_10 = bool(st.session_state["cf_p_above_10"])
    trend_template_1 = bool(st.session_state["cf_trend_template_1"])

    cond = (df_univ[primary_tf].fillna(0) >= rs_min)

    if min_mktcap > 0:
        cond = cond & (df_univ["Mkt Cap"].fillna(-1) >= min_mktcap)
    if min_float > 0:
        cond = cond & (df_univ["Float"].fillna(-1) >= min_float)

    if min_vol1d > 0:
        cond = cond & (df_univ["Volume"].fillna(-1) >= min_vol1d)
    if min_avgvol30 > 0:
        cond = cond & (df_univ["Avg Vol 30D"].fillna(-1) >= min_avgvol30)

    if min_volchg_1d > 0:
        cond = cond & (df_univ["Vol Chg 1D"].fillna(-999) >= (min_volchg_1d / 100.0))
    if min_volchg_1w > 0:
        cond = cond & (df_univ["Vol Chg 1W"].fillna(-999) >= (min_volchg_1w / 100.0))
    if min_volchg_1m > 0:
        cond = cond & (df_univ["Vol Chg 1M"].fillna(-999) >= (min_volchg_1m / 100.0))

    if min_rvol_1d > 0:
        cond = cond & (df_univ["Rel Vol 1D"].fillna(-1) >= min_rvol_1d)
    if min_rvol_1w > 0:
        cond = cond & (df_univ["Rel Vol 1W"].fillna(-1) >= min_rvol_1w)
    if min_rvol_1m > 0:
        cond = cond & (df_univ["Rel Vol 1M"].fillna(-1) >= min_rvol_1m)

    if min_rev_q > 0:
        cond = cond & (df_univ["Rev Qtr YoY"].fillna(-999) >= (min_rev_q / 100.0))
    if min_rev_a > 0:
        cond = cond & (df_univ["Rev Ann YoY"].fillna(-999) >= (min_rev_a / 100.0))
    if min_eps_q > 0:
        cond = cond & (df_univ["EPS Qtr YoY"].fillna(-999) >= (min_eps_q / 100.0))
    if min_eps_a > 0:
        cond = cond & (df_univ["EPS Ann YoY"].fillna(-999) >= (min_eps_a / 100.0))

    if min_roe > 0:
        cond = cond & (df_univ["ROE TTM"].fillna(-999) >= (min_roe / 100.0))
    if min_pretax > 0:
        cond = cond & (df_univ["PreTax Mgn TTM"].fillna(-999) >= (min_pretax / 100.0))

    if max_from_52w > 0:
        cond = cond & (df_univ["% From 52W High"].fillna(-999) >= (-max_from_52w / 100.0))
    if max_from_ath > 0:
        cond = cond & (df_univ["% From ATH"].fillna(-999) >= (-max_from_ath / 100.0))

    if min_adr > 0:
        cond = cond & (df_univ["ADR%"].fillna(-999) >= (min_adr / 100.0))
    if min_atr > 0:
        cond = cond & (df_univ["ATR%"].fillna(-999) >= (min_atr / 100.0))

    if sector_choice != "All":
        cond = cond & (df_univ["Sector"].astype(str) == sector_choice)

    if p_above_200:
        cond = cond & (df_univ["P>200"] == True)
    if p_above_50:
        cond = cond & (df_univ["P>50"] == True)
    if p_above_20:
        cond = cond & (df_univ["P>20"] == True)
    if p_above_10:
        cond = cond & (df_univ["P>10"] == True)

    if trend_template_1:
        cond = cond & (df_univ["P>200"] == True)
        cond = cond & (df_univ["P>50"] == True)
        cond = cond & (df_univ["50>200"] == True)

    df_f = df_univ[cond].copy()
    tie = "RS 1Y" if "RS 1Y" in df_f.columns else primary_tf
    df_f = df_f.sort_values([primary_tf, tie], ascending=[False, False])

df_show = df_f.reset_index(drop=True)

# ============================================================
# TICKER LOOKUP DASHBOARD (NEW) - render BEFORE scanner results
# ============================================================
render_ticker_lookup_dashboard(df_univ)

# ============================================================
# RESULTS HEADER
# ============================================================
st.markdown('<div class="section-title">Scanner Results</div>', unsafe_allow_html=True)
st.markdown(
    f'<div class="small-muted">Universe: <b>{len(df_univ):,}</b> • Matches: <b>{len(df_f):,}</b></div>',
    unsafe_allow_html=True
)

# ============================================================
# COLUMNS TO SHOW
# ============================================================
base_cols = [
    "Ticker", "Name", "Price",
    "RS 1W", "RS 1M", "RS 3M", "RS 6M", "RS 1Y",
    "% 1D", "% 1W", "% 1M", "% 3M", "% 6M", "% 1Y",
]

if mode in ["Accelerating", "Decelerating"]:
    show_cols = base_cols.copy()
    show_cols.insert(show_cols.index("RS 1Y") + 1, "RS GAP")

elif mode == "Custom":
    _init_custom_state()

    show_cols = base_cols.copy()
    extras = []

    # Liquidity
    if st.session_state["cf_min_mktcap"] > 0:
        extras.append("Mkt Cap")
    if st.session_state["cf_min_float"] > 0:
        extras.append("Float")

    # Volume
    if st.session_state["cf_min_vol1d"] > 0:
        extras.append("Volume")
    if st.session_state["cf_min_avgvol30"] > 0:
        extras.append("Avg Vol 30D")
    if st.session_state["cf_min_volchg_1d"] > 0:
        extras.append("Vol Chg 1D")
    if st.session_state["cf_min_volchg_1w"] > 0:
        extras.append("Vol Chg 1W")
    if st.session_state["cf_min_volchg_1m"] > 0:
        extras.append("Vol Chg 1M")
    if st.session_state["cf_min_rvol_1d"] > 0:
        extras.append("Rel Vol 1D")
    if st.session_state["cf_min_rvol_1w"] > 0:
        extras.append("Rel Vol 1W")
    if st.session_state["cf_min_rvol_1m"] > 0:
        extras.append("Rel Vol 1M")

    # Fundamentals
    if st.session_state["cf_min_rev_q"] > 0:
        extras.append("Rev Qtr YoY")
    if st.session_state["cf_min_rev_a"] > 0:
        extras.append("Rev Ann YoY")
    if st.session_state["cf_min_eps_q"] > 0:
        extras.append("EPS Qtr YoY")
    if st.session_state["cf_min_eps_a"] > 0:
        extras.append("EPS Ann YoY")
    if st.session_state["cf_min_roe"] > 0:
        extras.append("ROE TTM")
    if st.session_state["cf_min_pretax"] > 0:
        extras.append("PreTax Mgn TTM")

    # Volatility
    if st.session_state["cf_min_adr"] > 0:
        extras.append("ADR%")
    if st.session_state["cf_min_atr"] > 0:
        extras.append("ATR%")

    # Highs distance
    if st.session_state["cf_max_from_52w"] > 0:
        extras.append("% From 52W High")
    if st.session_state["cf_max_from_ath"] > 0:
        extras.append("% From ATH")

    # Sector
    if str(st.session_state["cf_sector_choice"]) != "All":
        extras.append("Sector")

    # Trend / MAs
    if bool(st.session_state["cf_p_above_200"]):
        extras += ["P>200", "SMA200"]
    if bool(st.session_state["cf_p_above_50"]):
        extras += ["P>50", "SMA50"]
    if bool(st.session_state["cf_p_above_20"]):
        extras += ["P>20", "SMA20"]
    if bool(st.session_state["cf_p_above_10"]):
        extras += ["P>10", "SMA10"]
    if bool(st.session_state["cf_trend_template_1"]):
        extras += ["P>200", "P>50", "50>200", "SMA200", "SMA50"]

    # Insert extras after Price
    insert_at = show_cols.index("Price") + 1
    seen = set(show_cols)
    extras_clean = []
    for c in extras:
        if c not in seen and c in df_show.columns:
            extras_clean.append(c)
            seen.add(c)

    for i, c in enumerate(extras_clean):
        show_cols.insert(insert_at + i, c)

else:
    show_cols = base_cols

render_table_html(df_show[show_cols].head(max_results), show_cols, height_px=950)

st.markdown('<div class="hr"></div>', unsafe_allow_html=True)
st.markdown(
    """
**How RS is Calculated:**  
Each stock is compared to **SPY** over a timeframe.  
Then all stocks in your screener universe are ranked against each other and assigned an **RS rating (1–99)**.
"""
)

# NOTE: Fixed your invalid "///" comment so the file runs.


