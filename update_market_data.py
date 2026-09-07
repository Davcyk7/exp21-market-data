"""
Fetch live prices + historical price series for every ticker David tracks,
plus FX rates, and write one JSON file: market_data.json, committed straight
into this same repo by the GitHub Actions workflow in
.github/workflows/refresh.yml.

v43 (leaving Yahoo Finance): this REPLACES the yfinance/pandas version of
this script. Same output shape (the app itself, App.js, doesn't change at
all because of this) — what changed is where the numbers come from and how
often this runs. Two things forced this rewrite together, not separately:

  1. Yahoo Finance's endpoint was never a real, licensed data source (see
     the main Vestly repo's "Leaving Yahoo Finance" README section) — this
     script now calls Marketstack (https://marketstack.com), a real,
     commercially-licensed provider, instead.
  2. Marketstack's Basic plan is METERED: 10,000 requests/month, not
     unlimited like yfinance was. A naive port of the old script (full
     5-year daily history, refetched from scratch every 15 minutes, one
     ticker at a time) would blow that budget in well under a day. So this
     version is also a genuine redesign, not just a find-and-replace:
       - ONE batched request per run covers every tracked ticker at once
         (Marketstack's /v1/eod takes a comma-separated symbols list) —
         never one request per ticker.
       - Each run only fetches a short recent window (FETCH_WINDOW_DAYS,
         below) instead of full history — the 1Y/6M/All ranges are
         maintained incrementally, by merging that fresh window onto
         whatever this SAME script already committed last run (read back
         from market_data.json, which this script also writes — the repo
         itself is the only persistent state between runs; see
         `merge_and_window` below). A full history refetch would need
         dozens of paginated requests every single run; incremental merge
         needs exactly one.
       - This script no longer computes or stores dividend/split data —
         that's now the app's own job (App.js's fetchMarketCorporateActions
         calls the Cloudflare Worker's /marketstack/eod endpoint directly,
         on demand, per position) so it doesn't cost this shared, metered
         budget at all.
     See the main Vestly repo's README "Marketstack quota budget" section
     for the actual monthly math this design is built around.

  FX rates come from a SEPARATE, free, no-API-key source — Frankfurter
  (api.frankfurter.dev, ECB-backed) — deliberately decoupled from
  Marketstack so currency conversion never touches the metered quota at
  all, however often this runs.

Requires the MARKETSTACK_API_KEY environment variable to be set (in
GitHub Actions, a repository secret — see the main repo's README for the
one-time "add a repository secret" steps). Get your own key free at
marketstack.com (the free tier is enough to confirm this script runs; the
real monthly volume needs the Basic plan — same key, no code change).

You should not normally need to run this by hand — GitHub Actions runs it
automatically on a schedule (see refresh.yml). To test it manually: repo's
Actions tab → "Refresh market data" workflow → "Run workflow".
"""

import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

MARKETSTACK_BASE = "https://api.marketstack.com/v1"
FRANKFURTER_BASE = "https://api.frankfurter.dev/v1"
OUT_PATH = "market_data.json"

# ticker (the app's own identifier, App.js position.ticker/market.prices key)
#   -> marketstack: the real Marketstack symbol to fetch (MIC-suffixed —
#      see cloudflare-worker/worker.js's MIC_CURRENCY table in the main
#      repo for the same suffix convention), or None if Marketstack simply
#      doesn't carry this listing at all (confirmed live for the two SGX
#      depositary receipts below — see the main repo's "Leaving Yahoo
#      Finance" research). A None entry is skipped entirely by the fetch
#      (costs nothing) and left out of `prices`/`history` in the output,
#      same as any other ticker the cloud script doesn't cover — the app's
#      own on-demand live-fetch fallback is what a real fix for these two
#      needs (swapping to their primary HKEX listings — a separate,
#      deliberately-not-automatic change, since it also changes what
#      currency/DR-ratio the position is priced in; see the main repo's
#      README once that lands).
# name/exchange/category: display metadata only, shown in the app.
TICKERS = {
    "0857.HK":   {"marketstack": "0857.XHKG",   "name": "PetroChina H",                          "exchange": "HKEX",     "category": "Equities", "currency": "HKD"},
    "MSFT":      {"marketstack": "MSFT",         "name": "Microsoft",                             "exchange": "NASDAQ",   "category": "Equities", "currency": "USD"},
    "META":      {"marketstack": "META",         "name": "Meta Platforms",                        "exchange": "NASDAQ",   "category": "Equities", "currency": "USD"},
    "HXXD.SI":   {"marketstack": None,           "name": "Xiaomi (SGX Depositary Receipt)",       "exchange": "SGX",      "category": "Equities", "currency": "SGD"},
    "300750.SZ": {"marketstack": "300750.XSHE",  "name": "Amperex Tech / CATL (A-share)",         "exchange": "Shenzhen", "category": "Equities", "currency": "CNY"},
    "SE":        {"marketstack": "SE",           "name": "Sea Limited",                           "exchange": "NYSE",     "category": "Equities", "currency": "USD"},
    "HBBD.SI":   {"marketstack": None,           "name": "Alibaba (SGX Depositary Receipt)",      "exchange": "SGX",      "category": "Equities", "currency": "SGD"},
    "O39.SI":    {"marketstack": "O39.XSES",     "name": "OCBC Bank",                             "exchange": "SGX",      "category": "Equities", "currency": "SGD"},
    "N2IU.SI":   {"marketstack": "N2IU.XSES",    "name": "Mapletree Pan Asia Commercial Trust",   "exchange": "SGX",      "category": "REITs",    "currency": "SGD"},
    "C38U.SI":   {"marketstack": "C38U.XSES",    "name": "CapitaLand Integrated Commercial Trust","exchange": "SGX",      "category": "REITs",    "currency": "SGD"},
    "GOOGL":     {"marketstack": "GOOGL",        "name": "Alphabet A",                            "exchange": "NASDAQ",   "category": "Equities", "currency": "USD"},
    "AAPL":      {"marketstack": "AAPL",         "name": "Apple",                                 "exchange": "NASDAQ",   "category": "Equities", "currency": "USD"},
    "NVDA":      {"marketstack": "NVDA",         "name": "NVIDIA",                                "exchange": "NASDAQ",   "category": "Equities", "currency": "USD"},
}

# World-index ticker for the app's "against a benchmark" comparison on
# Insights (see App.js's BENCHMARK_TICKER). Fetched alongside your actual
# holdings, on the exact same date axis per range — not a holding, so it's
# deliberately kept out of TICKERS/prices (it won't show up in your
# allocation or holdings list), only its historical series is used.
BENCHMARK_TICKER = "VT"
BENCHMARK_MARKETSTACK = "VT"

# Currencies that ever need converting to the app's base currency. SGD
# needs no conversion (it IS the base) but is still stored explicitly in
# every range's `fx` array below, matching what App.js's fxConvert/
# buildValueSeries already expect.
BASE_CURRENCY = "SGD"
FX_CURRENCIES = sorted({t["currency"] for t in TICKERS.values()} - {BASE_CURRENCY})

# How many trailing calendar days of DAILY bars to fetch fresh, every run.
# This is deliberately small — see the module docstring's point 2. 7 days
# covers a full trading week (so a single missed run, or a 2-3 day
# exchange holiday, still gets fully backfilled by the NEXT run) while
# keeping each run's request comfortably inside a single Marketstack page
# even at the smallest page size this project has ever observed live
# (100 rows — see the main repo's "Marketstack quota budget" section):
# ~12 symbols x 7 days = ~84 rows, well under 100. If Marketstack ever
# returns more rows than one page for this window, `marketstack_eod`
# below still paginates correctly — it just costs more than 1 request
# that run, not a hard failure.
FETCH_WINDOW_DAYS = 7

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


# --------------------------------------------------------------------- #
# HTTP helpers — plain urllib, no requests dependency needed for two
# simple GET-JSON APIs, keeping requirements.txt empty (stdlib only).
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
# Marketstack
# --------------------------------------------------------------------- #

def marketstack_eod(symbols, date_from, date_to, api_key):
    """One (usually) request across every symbol at once, via Marketstack's
    comma-separated `symbols` param. Paginates defensively if the account's
    real page size ever returns more than one page for this window — see
    FETCH_WINDOW_DAYS' own comment for why that's not expected to happen in
    normal operation. Returns a flat list of {symbol, date, close} dicts
    (dividend/split_factor are also present on each row but deliberately
    unused here — see the module docstring's point 2)."""
    if not symbols:
        return []
    rows = []
    offset = 0
    seen_pages = 0
    while True:
        params = {
            "access_key": api_key,
            "symbols": ",".join(symbols),
            "date_from": date_from,
            "date_to": date_to,
            "limit": 1000,
            "offset": offset,
        }
        url = f"{MARKETSTACK_BASE}/eod?{urllib.parse.urlencode(params)}"
        payload = http_get_json(url)
        if "error" in payload:
            err = payload["error"]
            raise RuntimeError(f"Marketstack error: {err.get('code')} — {err.get('message')}")
        data = payload.get("data", [])
        rows.extend(data)
        seen_pages += 1
        pagination = payload.get("pagination", {})
        total = pagination.get("total", len(rows))
        count = pagination.get("count", len(data))
        offset += count if count else len(data)
        if not data or offset >= total or seen_pages > 50:  # 50 is a hard safety cap, not an expected case
            break
    return rows


def normalize_eod_rows(rows):
    """[{symbol, date, close}, ...] -> {symbol: {date_str: close}}. Marketstack
    dates come back as e.g. '2026-09-05T00:00:00+0000' — trimmed to the
    plain YYYY-MM-DD the app's history.dates entries already use."""
    out = {}
    for row in rows:
        symbol = row.get("symbol")
        raw_date = row.get("date")
        close = row.get("close")
        if not symbol or not raw_date or close is None:
            continue
        date_str = str(raw_date)[:10]
        out.setdefault(symbol, {})[date_str] = round(float(close), 4)
    return out


# --------------------------------------------------------------------- #
# Frankfurter (FX) — free, no key, decoupled from the Marketstack budget
# entirely (see module docstring). fx_rates_to_sgd's own convention
# (App.js's fxConvert) is "units of SGD per 1 unit of X", so every rate
# Frankfurter gives back (which is naturally "units of X per 1 unit of
# base") gets inverted below.
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
# Incremental range merge — the core of staying within the Marketstack
# budget. See the module docstring's point 2 and RANGE_CONFIG's comment.
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


def merge_and_window(prior_points, fresh_points, bucket_kind, window_days):
    """The one function that makes every range incremental. `prior_points`
    is what THIS SAME range held last run (read back from the committed
    market_data.json); `fresh_points` is this run's newly-fetched daily
    window. Unions them (fresh wins on an exact-date collision — a Marketstack
    revision to a recent close), collapses to one point per bucket, then
    trims to the trailing `window_days`. Buckets entirely outside the fresh
    window are untouched (nothing to override them with) — that's what
    keeps 6M/1Y/All "incrementally maintained" instead of needing a full
    refetch every run. Returns {} if there's nothing at all yet (a brand
    new ticker, or every fetch attempt so far has failed)."""
    union = dict(prior_points)
    union.update(fresh_points)
    if not union:
        return {}
    collapsed = collapse_to_buckets(union, bucket_kind)
    cutoff = (datetime.strptime(max(collapsed.keys()), "%Y-%m-%d").date() - timedelta(days=window_days)).isoformat()
    return {d: v for d, v in collapsed.items() if d >= cutoff}


# --------------------------------------------------------------------- #
# Assembling one range's full output (shared date axis, forward/back-fill
# per ticker — same behaviour the old yfinance/pandas version had via
# reindex(ffill).bfill(), reimplemented here without pandas).
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


def build_range(range_key, cfg, prior_history, fresh_prices_daily, fresh_fx_daily):
    prior = (prior_history or {}).get(range_key) or {}
    prior_dates = prior.get("dates") or []
    prior_prices = prior.get("prices") or {}
    prior_fx = prior.get("fx") or {}

    def prior_points_for(series_by_key, key):
        arr = series_by_key.get(key)
        if not arr or not prior_dates:
            return {}
        return {d: v for d, v in zip(prior_dates, arr) if v is not None}

    merged_prices = {}
    for key, daily in fresh_prices_daily.items():
        merged = merge_and_window(prior_points_for(prior_prices, key), daily, cfg["bucket"], cfg["window_days"])
        if merged:
            merged_prices[key] = merged
    # Tickers with no fresh data this run (not covered by Marketstack, or a
    # failed fetch) still keep whatever this range already had, un-merged —
    # stale beats missing, same philosophy as the rest of this pipeline.
    for key, arr in prior_prices.items():
        if key not in merged_prices and key not in fresh_prices_daily:
            merged = merge_and_window(prior_points_for(prior_prices, key), {}, cfg["bucket"], cfg["window_days"])
            if merged:
                merged_prices[key] = merged

    merged_fx = {}
    for cur in FX_CURRENCIES:
        merged = merge_and_window(prior_points_for(prior_fx, cur), fresh_fx_daily.get(cur, {}), cfg["bucket"], cfg["window_days"])
        if merged:
            merged_fx[cur] = merged

    if not merged_prices:
        return None

    date_axis = sorted(set().union(*[set(v.keys()) for v in merged_prices.values()]))
    prices_out = align_series(merged_prices, date_axis)
    fx_out = align_series(merged_fx, date_axis)
    fx_out["SGD"] = [1.0] * len(date_axis)

    return {"dates": date_axis, "prices": prices_out, "fx": fx_out}


# --------------------------------------------------------------------- #
# main
# --------------------------------------------------------------------- #

def main():
    api_key = os.environ.get("MARKETSTACK_API_KEY")
    if not api_key:
        print("MARKETSTACK_API_KEY is not set — see this script's own docstring. Aborting without touching market_data.json.")
        sys.exit(1)

    try:
        with open(OUT_PATH, "r") as f:
            previous = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        previous = {}
    prior_history = previous.get("history") or {}
    prior_prices = previous.get("prices") or {}

    today = datetime.now(timezone.utc).date()
    date_from = (today - timedelta(days=FETCH_WINDOW_DAYS)).isoformat()
    date_to = today.isoformat()

    ms_symbol_to_ticker = {info["marketstack"]: ticker for ticker, info in TICKERS.items() if info["marketstack"]}
    ms_symbol_to_ticker[BENCHMARK_MARKETSTACK] = BENCHMARK_TICKER
    symbols = list(ms_symbol_to_ticker.keys())

    print(f"Fetching {len(symbols)} symbols' last {FETCH_WINDOW_DAYS} days from Marketstack ({date_from}..{date_to})...")
    failed = []
    fresh_by_symbol = {}
    try:
        rows = marketstack_eod(symbols, date_from, date_to, api_key)
        fresh_by_symbol = normalize_eod_rows(rows)
    except Exception as e:
        print(f"  Marketstack fetch FAILED entirely this run: {e}")
        print("  Falling back to last run's data untouched for every ticker.")

    fresh_by_ticker = {}
    for ms_symbol, ticker in ms_symbol_to_ticker.items():
        daily = fresh_by_symbol.get(ms_symbol)
        if daily:
            fresh_by_ticker[ticker] = daily
        elif ticker != BENCHMARK_TICKER:
            failed.append(ticker)
            print(f"  {ticker:<11} no fresh data this run — keeping last known price/history.")

    print("Fetching FX rates (Frankfurter, free — doesn't touch the Marketstack budget)...")
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
        print(f"  Frankfurter historical-series fetch failed: {e} — this run's history merge will lean on prior data only.")
        fx_daily = {}

    # Current prices (top-level `prices`, used for today's holdings values)
    # — derived from the SAME fresh daily window, not from the bucketed
    # history, so it's always as current as this run's own fetch allows.
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
        elif info["marketstack"] is None:
            print(f"  {ticker:<11} not covered by Marketstack — skipped (see TICKERS' own comment).")

    print("Merging into 1W/1M/6M/1Y/All history...")
    history = {}
    for range_key, cfg in RANGE_CONFIG.items():
        built = build_range(range_key, cfg, prior_history, fresh_by_ticker, fx_daily)
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
