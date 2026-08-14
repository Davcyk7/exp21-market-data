"""
Fetch live prices + historical price series for every ticker David tracks,
plus FX rates, and write one JSON file: market_data.json, committed straight
into this same repo by the GitHub Actions workflow in
.github/workflows/refresh.yml.

This is the CLOUD version of the script that used to run on David's PC via
Windows Task Scheduler. Same logic, same output shape — the only difference
is where it runs (GitHub's servers, on a schedule, for free) and where it
writes the output (this repo's root, not a folder on David's PC), so the app
can fetch it from anywhere over the internet instead of needing his PC on.

You should not normally need to run this by hand — GitHub Actions runs it
automatically every 30 minutes. To test it manually: repo's Actions tab →
"Refresh market data" workflow → "Run workflow".
"""

import json
import yfinance as yf
import pandas as pd
from datetime import datetime, timezone

# ticker -> (display name, exchange label, category)
# Add a line here whenever a NEW ticker is added in the app that isn't
# already priced — commit the change, the next scheduled run picks it up.
TICKERS = {
    "0857.HK":   ("PetroChina H",                           "HKEX",     "Equities"),
    "MSFT":      ("Microsoft",                                "NASDAQ",   "Equities"),
    "META":      ("Meta Platforms",                           "NASDAQ",   "Equities"),
    "HXXD.SI":   ("Xiaomi (SGX Depositary Receipt)",         "SGX",      "Equities"),
    "300750.SZ": ("Amperex Tech / CATL (A-share)",           "Shenzhen", "Equities"),
    "SE":        ("Sea Limited",                              "NYSE",     "Equities"),
    "HBBD.SI":   ("Alibaba (SGX Depositary Receipt)",        "SGX",      "Equities"),
    "O39.SI":    ("OCBC Bank",                                "SGX",      "Equities"),
    "N2IU.SI":   ("Mapletree Pan Asia Commercial Trust",     "SGX",      "REITs"),
    "C38U.SI":   ("CapitaLand Integrated Commercial Trust",  "SGX",      "REITs"),
    "GOOGL":     ("Alphabet A",                               "NASDAQ",   "Equities"),
    "AAPL":      ("Apple",                                    "NASDAQ",   "Equities"),
    "NVDA":      ("NVIDIA",                                   "NASDAQ",   "Equities"),
}

# FX pairs needed to convert every currency seen above into SGD
FX_TICKERS = {
    "USD": "USDSGD=X",
    "HKD": "HKDSGD=X",
    "CNY": "CNYSGD=X",
    # SGD needs no conversion
}

# Historical ranges the app's range buttons (1W/1M/6M/1Y/All) switch between.
# Daily-or-coarser resolution only — not intraday — kept deliberately simple
# so it works reliably across US/HK/SG/China trading calendars in one script.
RANGES = {
    "1W":  {"period": "7d",  "interval": "1d"},
    "1M":  {"period": "1mo", "interval": "1d"},
    "6M":  {"period": "6mo", "interval": "1wk"},
    "1Y":  {"period": "1y",  "interval": "1wk"},
    "All": {"period": "5y",  "interval": "1mo"},
}


def fetch_fx_rates():
    rates = {"SGD": 1.0}
    for currency, ticker in FX_TICKERS.items():
        try:
            rate = yf.Ticker(ticker).fast_info.last_price
            rates[currency] = rate
        except Exception as e:
            print(f"  WARNING: could not fetch {ticker} ({currency}->SGD): {e}")
            rates[currency] = None
    return rates


def fetch_series(ticker, period, interval):
    """One ticker's closing-price history for one range. Returns a pandas
    Series indexed by date (tz stripped, normalized to midnight), or None."""
    try:
        df = yf.Ticker(ticker).history(period=period, interval=interval)
        if df is None or df.empty:
            return None
        s = df["Close"]
        idx = s.index
        if getattr(idx, "tz", None) is not None:
            idx = idx.tz_localize(None)
        s.index = idx.normalize()
        return s
    except Exception as e:
        print(f"  history WARNING {ticker} {period}/{interval}: {e}")
        return None


def build_range_history(tickers, period, interval):
    """One range's worth of data across ALL tickers + FX, aligned onto a
    single shared date axis (union of every ticker's trading days, forward-
    filled so exchanges with different calendars/holidays still line up)."""
    price_series = {}
    for t in tickers:
        s = fetch_series(t, period, interval)
        if s is not None:
            price_series[t] = s

    if not price_series:
        return None

    fx_series = {}
    for cur, fx_ticker in FX_TICKERS.items():
        s = fetch_series(fx_ticker, period, interval)
        if s is not None:
            fx_series[cur] = s

    all_dates = sorted(set().union(*[s.index for s in price_series.values()]))
    date_index = pd.DatetimeIndex(all_dates)

    prices_out = {}
    for t, s in price_series.items():
        aligned = s.reindex(date_index, method="ffill").bfill()
        prices_out[t] = [round(float(v), 4) if pd.notna(v) else None for v in aligned]

    fx_out = {"SGD": [1.0] * len(date_index)}
    for cur, s in fx_series.items():
        aligned = s.reindex(date_index, method="ffill").bfill()
        fx_out[cur] = [round(float(v), 4) if pd.notna(v) else None for v in aligned]

    return {
        "dates": [d.strftime("%Y-%m-%d") for d in date_index],
        "prices": prices_out,
        "fx": fx_out,
    }


def main():
    print("Fetching FX rates...")
    fx = fetch_fx_rates()
    for cur, rate in fx.items():
        if rate is not None:
            print(f"  1 {cur} = {rate:.4f} SGD")

    print("\nFetching current prices...")
    prices = {}
    failed = []
    for ticker, (name, exchange, category) in TICKERS.items():
        try:
            tk = yf.Ticker(ticker)
            fi = tk.fast_info
            price = fi.last_price
            currency = fi.currency
            prev_close = fi.previous_close
            day_change_pct = (
                ((price - prev_close) / prev_close * 100) if prev_close else None
            )
            prices[ticker] = {
                "name": name,
                "exchange": exchange,
                "category": category,
                "price": round(price, 4),
                "currency": currency,
                "prev_close": round(prev_close, 4) if prev_close else None,
                "day_change_pct": round(day_change_pct, 2) if day_change_pct is not None else None,
            }
            print(f"  {ticker:<11} OK   {name}  {price} {currency}")
        except Exception as e:
            failed.append(ticker)
            print(f"  {ticker:<11} FAILED  {e}")

    print("\nFetching historical series (this takes longer)...")
    history = {}
    for range_key, cfg in RANGES.items():
        print(f"  {range_key} ({cfg['period']} / {cfg['interval']})...")
        history[range_key] = build_range_history(TICKERS.keys(), cfg["period"], cfg["interval"])

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "base_currency": "SGD",
        "fx_rates_to_sgd": {k: round(v, 4) if v else None for k, v in fx.items()},
        "prices": prices,
        "history": history,
        "failed_tickers": failed,
    }

    # Written to the repo root — the GitHub Actions workflow commits this
    # file back to the repo, and the app fetches it from the repo's raw URL.
    out_path = "market_data.json"
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\nFetched {len(prices)} tickers ({len(failed)} failed).")
    print(f"Written to {out_path}")


if __name__ == "__main__":
    main()
