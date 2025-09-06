import os
import time
import numpy as np
import pandas as pd
from dotenv import load_dotenv

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest, GetAssetsRequest
from alpaca.trading.enums import OrderSide, TimeInForce, OrderClass
from alpaca.trading.requests import TakeProfitRequest, StopLossRequest

from alpaca.data.historical.stock import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame

# ---------- config ----------
SYMBOLS = ["AAPL", "MSFT"]         # keep tiny for MVP
BAR_TIMEFRAME = TimeFrame.Minute
LOOKBACK = 20                      # breakout window (minutes)
DOLLAR_RISK = 100                  # risk per trade (paper!)
TP_PCT = 0.004                     # 0.4% take profit
SL_PCT = 0.003                     # 0.3% stop loss
POLL_SEC = 30                      # how often to poll bars
# ----------------------------

load_dotenv()
API_KEY = os.getenv("APCA_API_KEY_ID")
API_SECRET = os.getenv("APCA_API_SECRET_KEY")
BASE_URL = os.getenv("APCA_API_BASE_URL", "https://paper-api.alpaca.markets")

# Trading client (paper by default via paper=True; can also override URL)
trading = TradingClient(api_key=API_KEY, secret_key=API_SECRET, paper=True, url_override=BASE_URL)

# Market data client (stocks)
data_client = StockHistoricalDataClient(API_KEY, API_SECRET)

def get_latest_bars(symbols, limit=LOOKBACK+1):
    req = StockBarsRequest(
        symbol_or_symbols=symbols,
        timeframe=BAR_TIMEFRAME,
        limit=limit
    )
    bars = data_client.get_stock_bars(req).df  # MultiIndex (symbol, timestamp)
    if bars.empty:
        return None
    # reshape to per-symbol frames
    latest = {}
    for sym in symbols:
        if (sym,) in bars.index.get_level_values(0):
            df = bars.xs(sym).copy()
            latest[sym] = df
    return latest

def calc_signal(df):
    """
    Simple breakout: go long if last close > rolling max of previous LOOKBACK bars.
    """
    if len(df) < LOOKBACK + 1:
        return False
    rolling_max = df["close"].iloc[-(LOOKBACK+1):-1].max()
    return df["close"].iloc[-1] > rolling_max

def round_qty(price, dollars):
    if price <= 0:
        return 0
    qty = int(dollars // price)  # whole shares for MVP
    return max(qty, 0)

def already_open(sym):
    positions = trading.get_all_positions()
    return any(p.symbol == sym for p in positions)

def place_bracket_market(sym, qty, price):
    """
    Places a parent market order with attached TP/SL (bracket).
    """
    take_profit = TakeProfitRequest(limit_price=round(price * (1 + TP_PCT), 2))
    stop_loss   = StopLossRequest(stop_price=round(price * (1 - SL_PCT), 2))

    order = MarketOrderRequest(
        symbol=sym,
        qty=qty,
        side=OrderSide.BUY,
        time_in_force=TimeInForce.DAY,
        order_class=OrderClass.BRACKET,
        take_profit=take_profit,
        stop_loss=stop_loss,
    )
    resp = trading.submit_order(order)
    return resp

def ensure_tradable(symbols):
    tradables = set()
    assets = trading.get_all_assets(GetAssetsRequest())
    tradable_map = {a.symbol: a.tradable for a in assets}
    for s in symbols:
        if tradable_map.get(s, False):
            tradables.add(s)
    return sorted(tradables)

def main():
    tradable_syms = ensure_tradable(SYMBOLS)
    if not tradable_syms:
        print("No tradable symbols found. Check your account permissions/universe.")
        return

    print("Starting loop. Ctrl+C to stop.")
    while True:
        try:
            bars_map = get_latest_bars(tradable_syms)
            if not bars_map:
                print("No bars yet...")
                time.sleep(POLL_SEC)
                continue

            for sym, df in bars_map.items():
                last_price = float(df["close"].iloc[-1])
                if already_open(sym):
                    print(f"[{sym}] position already open. Skipping.")
                    continue

                signal = calc_signal(df)
                if signal:
                    qty = round_qty(last_price, DOLLAR_RISK / SL_PCT)  # size so SL ~ DOLLAR_RISK
                    if qty <= 0:
                        print(f"[{sym}] qty=0 at price {last_price:.2f}.")
                        continue
                    print(f"[{sym}] breakout! sending bracket order: qty={qty} @ ~{last_price:.2f}")
                    resp = place_bracket_market(sym, qty, last_price)
                    print(f"  -> order id: {resp.id} status: {resp.status}")
                else:
                    print(f"[{sym}] no signal. last={last_price:.2f}")

            time.sleep(POLL_SEC)

        except KeyboardInterrupt:
            print("\nExiting…")
            break
        except Exception as e:
            print("Error:", e)
            time.sleep(POLL_SEC)

if __name__ == "__main__":
    main()
