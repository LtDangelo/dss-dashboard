import streamlit as st
import ccxt
import pandas as pd
import numpy as np
import requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import time

# ---------- CONFIGURATION ----------
CMC_API_KEY = "2a5d0400-dd0f-44d4-88ad-aa216aeea5dc"
EXCLUDED_STABLECOINS = {"USDT", "USDC", "BUSD", "DAI", "TUSD", "FDUSD", "GUSD"}
MAX_THREADS = 15

st.set_page_config(page_title="Top 200 Binance DSS Dashboard", layout="wide")
st.title("📊 DSS Bressert Multi-Timeframe Dashboard (Binance Top 200)")

# ---------- DSS FUNCTIONS ----------
def stochastic(close, high, low, length):
    lowest = low.rolling(length).min()
    highest = high.rolling(length).max()
    return 100 * (close - lowest) / (highest - lowest)

def dss_bressert(df, pds=10, ema_len=9, trigger_len=5):
    stoch1 = stochastic(df['close'], df['high'], df['low'], pds)
    precalc = stoch1.ewm(span=ema_len, adjust=False).mean()
    stoch2 = stochastic(precalc, precalc, precalc, pds)
    xDSS = stoch2.ewm(span=ema_len, adjust=False).mean()
    xTrigger = xDSS.ewm(span=trigger_len, adjust=False).mean()
    return xDSS, xTrigger

# ---------- SYMBOL FETCH ----------
@st.cache_data
def load_binance_symbols():
    exchange = ccxt.binance({
        'enableRateLimit': True,
        'options': {'defaultType': 'spot'}
    })
    return exchange.load_markets()

def get_binance_symbols(limit=200):
    url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"
    headers = {"X-CMC_PRO_API_KEY": CMC_API_KEY}
    params = {"start": "1", "limit": str(limit), "convert": "USD"}
    response = requests.get(url, headers=headers, params=params)
    data = response.json()

    if "data" not in data:
        st.error("❌ Failed to fetch data from CoinMarketCap.")
        return [], []

    cmc_symbols = [coin["symbol"] for coin in data["data"] if coin["symbol"] not in EXCLUDED_STABLECOINS]

    if not cmc_symbols:
        return [], []

    exchange = ccxt.binance({
        'enableRateLimit': True,
        'options': {'defaultType': 'spot'}
    })
    markets = load_binance_symbols()
    available_pairs = set(markets.keys())

    valid_pairs = [f"{sym}/USDT" for sym in cmc_symbols if f"{sym}/USDT" in available_pairs]
    return cmc_symbols, valid_pairs

# ---------- DATA FETCH with Retry ----------
def fetch_ohlcv(exchange, symbol, timeframe, limit=300):
    for attempt in range(3):
        try:
            ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            return df
        except ccxt.NetworkError:
            time.sleep(2)
        except ccxt.BaseError:
            return pd.DataFrame()
    return pd.DataFrame()

# ---------- PROCESS ----------
def process_symbol(exchange, symbol, timeframes):
    row = {'Symbol': symbol}
    directions = []
    for label, tf in timeframes.items():
        df = fetch_ohlcv(exchange, symbol, tf, limit=300)
        if df.empty:
            row[label] = "N/A"
            row[f"{label}_DSS"] = "N/A"
            directions.append("N/A")
        else:
            dss, trigger = dss_bressert(df)
            direction = "Bullish" if dss.iloc[-1] > trigger.iloc[-1] else "Bearish" if dss.iloc[-1] < trigger.iloc[-1] else "Flat"
            row[label] = direction
            last_dss = dss.iloc[-1]
            row[f"{label}_DSS"] = int(round(last_dss)) if pd.notna(last_dss) else "N/A"
            directions.append(direction)

    row['Signal'] = "Long" if all(d == "Bullish" for d in directions) else "Short" if all(d == "Bearish" for d in directions) else "Neutral"
    return row

# ---------- REFRESH BUTTON ----------
if st.button("🔄 Refresh Data"):
    st.rerun()

# ---------- DASHBOARD ----------
timeframes = {'1W': '1w', '1D': '1d'}
cmc_symbols, symbols = get_binance_symbols()

if not symbols:
    st.error("❌ No valid symbols found. Please check your API key.")
    st.stop()

exchange = ccxt.binance({
    'enableRateLimit': True,
    'options': {'defaultType': 'spot'}
})
rows = []
progress_bar = st.empty()

with ThreadPoolExecutor(MAX_THREADS) as executor:
    futures = [executor.submit(process_symbol, exchange, sym, timeframes) for sym in symbols]
    for i, future in enumerate(tqdm(as_completed(futures), total=len(futures), desc="Fetching data")):
        rows.append(future.result())
        progress_bar.progress((i + 1) / len(futures))

# Sort and label
symbol_order = {f"{sym}/USDT": i for i, sym in enumerate(cmc_symbols)}
df_out = pd.DataFrame(rows)
df_out['SortIndex'] = df_out['Symbol'].map(symbol_order)
df_out = df_out.sort_values("SortIndex").drop(columns=["SortIndex"])
df_out.insert(0, "#", range(1, 1 + len(df_out)))

# ---------- DISPLAY ----------
def color_map(val):
    if val == "Bullish":
        return 'background-color: green; width: 12px'
    elif val == "Bearish":
        return 'background-color: red; width: 12px'
    elif val == "Flat":
        return 'background-color: gray; width: 12px'
    elif val == "Long":
        return 'background-color: green; width: 12px'
    elif val == "Short":
        return 'background-color: red; width: 12px'
    elif val == "Neutral":
        return 'background-color: gray; width: 12px'
    return 'width: 12px'

cols = ['#', 'Symbol', '1W', '1W_DSS', '1D', '1D_DSS', 'Signal']

st.dataframe(
    df_out[cols].style.applymap(
        color_map,
        subset=pd.IndexSlice[:, ['1W', '1D', 'Signal']]
    ),
    use_container_width=True,
    hide_index=True,
    height=900
)
