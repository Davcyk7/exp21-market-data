"""
Fetch live prices + historical price series for every ticker David tracks,
plus FX rates, and write one JSON file: market_data.json, committed straight
into this same repo by the GitHub Actions workflow in
.github/workflows/refresh.yml.

v46 (David's personal-use pivot, back to Yahoo Finance, 2026-09-30): this
REPLACES the Marketstack version of this script (itself a v43 rewrite of
the original yfinance/pandas version). David decided to stop monetizing
Vestly and use it personally only — the exact ToS risk that forced v42's
move off Yahoo (Yahoo's terms restrict commercial/automated use) no longer
applies to a personal-use app, so this reverts to Yahoo Finance via the
`yfinance` package. Same output shape as always (App.js doesn't change at
all because of this) — what changed is where the numbers come from and,
because yfinance has no metered quota to protect, a real simplification of
HOW this script fetches them:

  - Every run now fetches each tracked ticker's FULL relevant history fresh
    (one batched `yfinance.download()` call covering all 13 tickers plus
    the benchmark at once — see `fetch_all_history` below), instead of
    v43's small 7-day window incrementally merged onto whatever the
    previous run had already committed. yfinance is free and unlimited, so
    there's no quota reason to fetch less than the full window every time,
    and a full-refetch design is simpler and more self-healing (a bad
    commit, or a market_data.json that's gone missing entirely, fixes
    itself on the very next run instead of needing weeks to backfill
    incrementally).
  - `merge_and_window` (v43's incremental range-merge core) is gone,
    replaced by the much simpler `window_and_bucket`, which just collapses
    a fresh, complete daily series to one point per bucket and trims it to
    the range's window — no prior-run union step needed, because there's
    no "prior run's window" to union with anymore.
  - The BACKFILL_DAYS escape hatch (and its workflow_dispatch input in
    refresh.yml) is gone too — it existed only to work around the
    incremental design's slow cold-start; a full-refetch design has no
    cold-start problem to work around.
  - All 13 tracked tickers are covered again, including the six
    Hong Kong/Singapore-listed names that were confirmed DEAD on
    Marketstack's feed (see this file's own git history for that
    research) — that gap was specific to Marketstack's account/plan, not a
    real Yahoo Finance limitation; Yahoo has always covered HKEX and SGX.
  - This script still does not compute or store dividend/split data —
    that's App.js's own job (fetchMarketCorporateActions calls Yahoo's
    chart endpoint directly, on demand, per position), unrelated to this
    cloud pipeline either way.

  FX rates still come from Frankfurter (api.frankfurter.dev, ECB-backed,
  free, no API key) — this half of the pipeline was never Marketstack- or
  Yahoo-specific and is completely unchanged by this reversion.

Requires the `yfinance` package (see requirements.txt) but no API key at
all — unlike Marketstack, Yahoo's unofficial endpoints (which is what
yfinance itself calls under the hood) need no account or credential.

You should not normally need to run this by hand — GitHub Actions runs it
automatically on a schedule (see refresh.yml). To test it manually: repo's
Actions tab -> "Refresh market data" workflow -> "Run workflow".
"""

import json
import math
import urllib.parse
import urllib.request
import time
from datetime import datetime, timedelta, timezone

import pandas as pd
import yfinance as yf

FRANKFURTER_BASE = "https://api.frankfurter.dev/v1"
OUT_PATH = "market_data.json"

# ticker (the app's own identifier, App.js position.ticker/market.prices key,
# AND the exact string this script hands to yfinance — Yahoo's own ticker
# format already carries the right exchange suffix, e.g. ".HK"/".SI"/".SZ",
# so unlike the Marketstack era there's no separate translated-symbol field
# needed at all here). name/exchange/category/currency: display metadata
# only, shown in the app. All 13 are real, live Yahoo Finance coverage —
# the six HKEX/SGX names that were confirmed dead on Marketstack's feed
# (see git history) are ordinary tickers again under Yahoo.
TICKERS = {
    "0857.HK":   {"name": "PetroChina H",                           "exchange": "HKEX",     "category": "Equities", "currency": "HKD"},
    "MSFT":      {"name": "Microsoft",                              "exchange": "NASDAQ",   "category": "Equities", "currency": "USD"},
    "META":      {"name": "Meta Platforms",                         "exchange": "NASDAQ",   "category": "Equities", "currency": "USD"},
    "HXXD.SI":   {"name": "Xiaomi (SGX Depositary Receipt)",        "exchange": "SGX",      "category": "Equities", "currency": "SGD"},
    "300750.SZ": {"name": "Amperex Tech / CATL (A-share)",          "exchange": "Shenzhen", "category": "Equities", "currency": "CNY"},
    "SE":        {"name": "Sea Limited",                            "exchange": "NYSE",     "category": "Equities", "currency": "USD"},
    "HBBD.SI":   {"name": "Alibaba (SGX Depositary Receipt)",       "exchange": "SGX",      "category": "Equities", "currency": "SGD"},
    "O39.SI":    {"name": "OCBC Bank",                              "exchange": "SGX",      "category": "Equities", "currency": "SGD"},
    "N2IU.SI":   {"name": "Mapletree Pan Asia Commercial Trust",    "exchange": "SGX",      "category": "REITs",    "currency": "SGD"},
    "C38U.SI":   {"name": "CapitaLand Integrated Commercial Trust", "exchange": "SGX",      "category": "REITs",    "currency": "SGD"},
    "GOOGL":     {"name": "Alphabet A",                             "exchange": "NASDAQ",   "category": "Equities", "currency": "USD"},
    "AAPL":      {"name": "Apple",                                  "exchange": "NASDAQ",   "category": "Equities", "currency": "USD"},
    "NVDA":      {"name": "NVIDIA",                                 "exchange": "NASDAQ",   "category": "Equities", "currency": "USD"},
}

# World-index ticker for the app's "against a benchmark" comparison on
# Insights (see App.js's BENCHMARK_TICKER). Fetched alongside your actual
# holdings, on the exact same date axis per range — not a holding, so it's
# deliberately kept out of TICKERS/the top-level `prices` output (it won't
# show up in your allocation or holdings list), only its historical series
# is used. Same ticker string on Yahoo as everywhere else, no translation.
BENCHMARK_TICKER = "VT"

# Currencies that ever need converting to the app's base currency. SGD
# needs no conversion (it IS the base) but is still stored explicitly in
# every range's `fx` array below, matching what App.js's fxConvert/
# buildValueSeries already expect.
BASE_CURRENCY = "SGD"

# GBP/EUR/CAD/BRL are fetched UNCONDITIONALLY here, not just derived from
# TICKERS below. TICKERS is David's own fixed, manually-curated list —
# deriving FX_CURRENCIES from it alone would mean these four currencies
# only start getting real rates the day David's own TICKERS table happens
# to include one, which does nothing for any position — David's or any
# future user's — added through the app's own on-demand Add Position flow
# (a completely separate code path from this cloud script, see App.js's
# CURRENCIES comment).
ALWAYS_FETCHED_FX_CURRENCIES = {"GBP", "EUR", "CAD", "BRL"}
FX_CURRENCIES = sorted(
    ({t["currency"] for t in TICKERS.values()} | ALWAYS_FETCHED_FX_CURRENCIES) - {BASE_CURRENCY}
)

# Per-range bucketing + how many days back each range's final series is
# trimmed to keep — same day-window/bucket shape as App.js's own
# CLIENT_RANGES (the on-demand chart for ad-hoc tickers), kept consistent
# on purpose so "1Y" means the same thing everywhere in this project.
# bucket=None means "keep every daily point" (no collapsing).
RANGE_CONFIG = {
    "1W":  {"bucket": None,    "window_days": 10},
    "1M":  {"bucket": None,    "window_days": 40},
    "6M":  {"bucket": "week",  "window_days": 200},
    "1Y":  {"bucket": "week",  "window_days": 380},
    "All": {"bucket": "month", "window_days": 1850},
}

# v46: how many trailing calendar days of DAILY bars to fetch fresh, every
# run. Unlike v43's FETCH_WINDOW_DAYS (deliberately tiny, 7 days, to stay
# under Marketstack's metered quota), this now just needs to cover the
# LONGEST range's own window (RANGE_CONFIG["All"], 1850 days) plus a small
# buffer — because every run re-fetches full history from scratch (see the
# module docstring), there's no incremental merge needing only a short
# recent slice anymore. yfinance has no per-request cost that makes a
# ~5-year window across 14 tickers in one batched call expensive.
HISTORY_FETCH_DAYS = max(cfg["window_days"] for cfg in RANGE_CONFIG.values()) + 30


# --------------------------------------------------------------------- #
# Yahoo Finance, via yfinance
# --------------------------------------------------------------------- #

def fetch_all_history(tickers, date_from, date_to):
    """One batched `yfinance.download()` call across every tracked ticker
    (plus the benchmark) at once, instead of one request per ticker —
    mirrors the old Marketstack version's single comma-separated /eod
    call, but for a genuinely free/unlimited provider there's no quota
    reason to batch this carefully; it's just faster and simpler than
    looping. Returns {ticker: {date_str: close}}, closes rounded to 4
    decimal places same as before. A ticker yfinance has no data for at
    all (delisted, typo, a genuine gap) is simply absent from the result
    rather than raising — callers already treat a missing/empty entry as
    "no fresh data this run" the same way a Marketstack gap used to look."""
    if not tickers:
        return {}
    raw = yf.download(
        tickers=tickers,
        start=date_from,
        end=date_to,
        interval="1d",
        group_by="ticker",
        auto_adjust=False,
        progress=False,
        threads=True,
    )
    out = {}
    if raw is None or raw.empty:
        return out
    for ticker in tickers:
        try:
            if isinstance(raw.columns, pd.MultiIndex):
                if ticker not in raw.columns.get_level_values(0):
                    continue
                closes = raw[ticker]["Close"]
            else:
                # Only reachable if `tickers` had exactly one element —
                # yfinance drops the ticker level of the column MultiIndex
                # entirely in that case.
                closes = raw["Close"]
        except KeyError:
            continue
        daily = {}
        for ts, close in closes.items():
            if close is None or (isinstance(close, float) and math.isnan(close)):
                continue
            daily[ts.strftime("%Y-%m-%d")] = round(float(close), 4)
        if daily:
            out[ticker] = daily
    return out


# --------------------------------------------------------------------- #
# HTTP helper for Frankfurter — plain urllib, no requests dependency
# needed for a simple GET-JSON API.
# --------------------------------------------------------------------- #

def http_get_json(url, timeout=30, retries=2):
    last_err = None
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "vestly-market-refresh/1.0"})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception as e:  # noqa: BLE001 — genuinely want to catch+retry anything here
            last_err = e
            if attempt < retries:
                time.sleep(1.5 * (attempt + 1))
    raise RuntimeError(f"GET {url.split('?')[0]} failed after {retries + 1} attempts: {last_err}")


# --------------------------------------------------------------------- #
# Frankfurter (FX) — free, no key, unrelated to the market-data provider
# either way (Yahoo now, Marketstack before, yfinance before that).
# fx_rates_to_sgd's own convention (App.js's fxConvert) is "units of SGD
# per 1 unit of X", so every rate Frankfurter gives back (which is
# naturally "units of X per 1 unit of base") gets inverted below.
# --------------------------------------------------------------------- #

def frankfurter_latest(base, symbols):
    if not symbols:
        return {}
    params = urllib.parse.urlencode({"base": base, "symbols": ",".join(symbols)})
    payload = http_get_json(f"{FRANKFURTER_BASE}/latest?{params}")
    rates = payload.get("rates", {}) or {}
    out = {}
    for cur, rate in rates.items():
        if rate:
            out[cur] = round(1.0 / rate, 6)
    return out


def frankfurter_series(base, symbols, date_from, date_to):
    """{currency: {date_str: sgd_per_unit}} over [date_from, date_to]."""
    if not symbols:
        return {}
    params = urllib.parse.urlencode({"base": base, "symbols": ",".join(symbols)})
    payload = http_get_json(f"{FRANKFURTER_BASE}/{date_from}..{date_to}?{params}")
    by_date = payload.get("rates", {}) or {}
    out = {cur: {} for cur in symbols}
    for date_str, day_rates in by_date.items():
        for cur, rate in (day_rates or {}).items():
            if rate and cur in out:
                out[cur][date_str] = round(1.0 / rate, 6)
    return out


# --------------------------------------------------------------------- #
# Bucketing — unchanged, reusable, provider-agnostic (same logic under
# Marketstack, and before that under the original yfinance version).
# --------------------------------------------------------------------- #

def bucket_key_week(date_obj):
    monday = date_obj - timedelta(days=date_obj.weekday())
    return monday.isoformat()


def bucket_key_month(date_obj):
    return date_obj.strftime("%Y-%m")


BUCKET_KEY_FNS = {"week": bucket_key_week, "month": bucket_key_month}


def bucket_series(daily):
    """{date_str: val} already collapsed to one point per bucket -> itself,
    unchanged. Used for bucket=None ranges, kept for symmetry/clarity at
    call sites rather than branching there."""
    return dict(daily)


def collapse_to_buckets(daily, bucket_kind):
    """{date_str: val} -> one point per bucket, keeping the LATEST trading
    date's value within each bucket (same convention as a pandas weekly/
    monthly resample, and App.js's own downsampleBars). The point's KEY in
    the returned dict is that real trading date, not the bucket's own
    label — so the app's x-axis still shows genuine trading days, exactly
    like the shape it already expects."""
    if bucket_kind is None:
        return bucket_series(daily)
    key_fn = BUCKET_KEY_FNS[bucket_kind]
    buckets = {}
    for date_str in sorted(daily.keys()):
        bk = key_fn(datetime.strptime(date_str, "%Y-%m-%d").date())
        buckets[bk] = (date_str, daily[date_str])  # later dates overwrite -> last-in-bucket wins
    return {rep_date: val for rep_date, val in buckets.values()}


def window_and_bucket(daily_points, bucket_kind, window_days):
    """v46 replacement for v43's merge_and_window: yfinance has no metered
    quota to protect, so this script now fetches each ticker's FULL
    relevant history fresh every run (see fetch_all_history) instead of
    incrementally merging a small window onto whatever the previous run
    committed. This is just the second half of the old function — collapse
    to one point per bucket, then trim to the trailing `window_days` — with
    no prior-run union step, since `daily_points` here is already the
    complete, freshly-fetched series, not a short recent window. Returns {}
    for an empty input."""
    if not daily_points:
        return {}
    collapsed = collapse_to_buckets(daily_points, bucket_kind)
    if not collapsed:
        return {}
    cutoff = (datetime.strptime(max(collapsed.keys()), "%Y-%m-%d").date() - timedelta(days=window_days)).isoformat()
    return {d: v for d, v in collapsed.items() if d >= cutoff}


# --------------------------------------------------------------------- #
# Assembling one range's full output (shared date axis, forward/back-fill
# per ticker — same behaviour the original yfinance/pandas version had via
# reindex(ffill).bfill(), reimplemented here without pandas for this step).
# --------------------------------------------------------------------- #

def align_series(per_key_points, date_axis):
    """{key: {date: val}} + a shared sorted date axis -> {key: [val,...]}
    forward-filled from each key's own last known value, then back-filled
    for any axis dates before that key's own first known value — matches
    the app's expectation that every array in one range is the same length
    as `dates`, with no holes."""
    out = {}
    for key, points in per_key_points.items():
        if not points:
            continue
        series = []
        last = None
        for d in date_axis:
            if d in points:
                last = points[d]
            series.append(last)
        # back-fill any leading Nones with the first real value seen
        first_real = next((v for v in series if v is not None), None)
        if first_real is not None:
            series = [v if v is not None else first_real for v in series]
        out[key] = series
    return out


def build_range(range_key, cfg, fresh_prices_daily, fresh_fx_daily):
    """v46: simplified — no more `prior_history` argument, since there's no
    incremental merge left to do (see window_and_bucket). Each range is
    built fresh, every run, straight from this run's own complete fetch."""
    bucketed_prices = {}
    for key, daily in fresh_prices_daily.items():
        bucketed = window_and_bucket(daily, cfg["bucket"], cfg["window_days"])
        if bucketed:
            bucketed_prices[key] = bucketed

    bucketed_fx = {}
    for cur in FX_CURRENCIES:
        bucketed = window_and_bucket(fresh_fx_daily.get(cur, {}), cfg["bucket"], cfg["window_days"])
        if bucketed:
            bucketed_fx[cur] = bucketed

    if not bucketed_prices:
        return None

    date_axis = sorted(set().union(*[set(v.keys()) for v in bucketed_prices.values()]))
    prices_out = align_series(bucketed_prices, date_axis)
    fx_out = align_series(bucketed_fx, date_axis)
    fx_out["SGD"] = [1.0] * len(date_axis)

    return {"dates": date_axis, "prices": prices_out, "fx": fx_out}


# --------------------------------------------------------------------- #
# main
# --------------------------------------------------------------------- #

def main():
    try:
        with open(OUT_PATH, "r") as f:
            previous = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        previous = {}
    prior_prices = previous.get("prices") or {}

    today = datetime.now(timezone.utc).date()
    date_from = (today - timedelta(days=HISTORY_FETCH_DAYS)).isoformat()
    date_to = today.isoformat()

    tickers_to_fetch = list(TICKERS.keys()) + [BENCHMARK_TICKER]

    print(f"Fetching {len(tickers_to_fetch)} tickers' full history from Yahoo Finance ({date_from}..{date_to})...")
    failed = []
    fresh_by_ticker = {}
    try:
        fresh_by_ticker = fetch_all_history(tickers_to_fetch, date_from, date_to)
    except Exception as e:
        print(f"  Yahoo Finance fetch FAILED entirely this run: {e}")
        print("  Falling back to last run's data untouched for every ticker.")

    for ticker in TICKERS:
        if ticker not in fresh_by_ticker:
            failed.append(ticker)
            print(f"  {ticker:<11} no fresh data this run — keeping last known price/history.")

    print("Fetching FX rates (Frankfurter, free, unrelated to the price provider)...")
    try:
        fx_now = frankfurter_latest(BASE_CURRENCY, FX_CURRENCIES)
    except Exception as e:
        print(f"  Frankfurter latest-rate fetch failed: {e} — reusing last known rates.")
        fx_now = {}
    fx_rates_to_sgd = {"SGD": 1.0}
    for cur in FX_CURRENCIES:
        fx_rates_to_sgd[cur] = fx_now.get(cur) or (previous.get("fx_rates_to_sgd") or {}).get(cur)

    try:
        fx_daily = frankfurter_series(BASE_CURRENCY, FX_CURRENCIES, date_from, date_to)
    except Exception as e:
        print(f"  Frankfurter historical-series fetch failed: {e} — this run's history will lean on price data only.")
        fx_daily = {}

    # Current prices (top-level `prices`, used for today's holdings values)
    # — derived from the SAME fresh daily fetch, not from the bucketed
    # history, so it's always as current as this run's own fetch allows.
    # A ticker with no fresh data this run still keeps whatever price this
    # script last committed — stale beats missing — but (unlike v43's
    # history ranges) there's no equivalent fallback needed for history
    # itself anymore, since a full refetch either has the data or doesn't.
    prices_out = {}
    for ticker, info in TICKERS.items():
        daily = fresh_by_ticker.get(ticker)
        if daily:
            dates_sorted = sorted(daily.keys())
            price = daily[dates_sorted[-1]]
            prev_close = daily[dates_sorted[-2]] if len(dates_sorted) >= 2 else (prior_prices.get(ticker) or {}).get("price")
            day_change_pct = round(((price - prev_close) / prev_close) * 100, 2) if prev_close else None
            prices_out[ticker] = {
                "name": info["name"], "exchange": info["exchange"], "category": info["category"],
                "price": price, "currency": info["currency"],
                "prev_close": prev_close, "day_change_pct": day_change_pct,
            }
            print(f"  {ticker:<11} OK   {info['name']}  {price} {info['currency']}")
        elif ticker in prior_prices:
            prices_out[ticker] = prior_prices[ticker]  # stale beats missing

    print("Bucketing into 1W/1M/6M/1Y/All history...")
    history = {}
    for range_key, cfg in RANGE_CONFIG.items():
        built = build_range(range_key, cfg, fresh_by_ticker, fx_daily)
        if built:
            history[range_key] = built
            print(f"  {range_key:<4} {len(built['dates'])} dates, {len(built['prices'])} tickers")

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "base_currency": BASE_CURRENCY,
        "fx_rates_to_sgd": {k: (round(v, 4) if v else None) for k, v in fx_rates_to_sgd.items()},
        "prices": prices_out,
        "history": history,
        "failed_tickers": failed,
    }

    with open(OUT_PATH, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\nFetched {len(prices_out)} tickers ({len(failed)} failed this run, carried over from last known).")
    print(f"Written to {OUT_PATH}")


if __name__ == "__main__":
    main()
