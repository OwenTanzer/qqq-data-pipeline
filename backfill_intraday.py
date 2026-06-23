#!/usr/bin/env python3
"""
backfill_intraday.py — Upload QQQ intraday price bars for all historical expiry days.

For each chain date already in the R2 manifest, uploads a 5-min OHLCV CSV to
prices/qqq_intraday_YYYYMMDD.csv for the corresponding expiry day (+1D).

Data sources (tried in order per date):
  1. yfinance          — last ~59 days, free, no key needed
  2. Twelve Data       — older dates, free tier (800 calls/day), uses TWELVEDATA_API_KEY
  3. MarketData.app    — fallback if Twelve Data key absent, uses MARKETDATA_API_KEY

Required env vars:
  R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET_NAME

Optional env vars:
  TWELVEDATA_API_KEY   — preferred free source for dates beyond yfinance's window
  MARKETDATA_API_KEY   — fallback if Twelve Data key not set (costs credits)

Usage:
  python backfill_intraday.py              # upload all missing, auto-select source
  python backfill_intraday.py --dry-run    # show plan without uploading
  python backfill_intraday.py --yf-only    # yfinance only, skip all paid/keyed sources
"""

import argparse
import io
import json
import os
import time
from datetime import date, datetime, timedelta, timezone

import boto3
import pandas as pd
import pandas_market_calendars as mcal
import pytz
import requests
import yfinance as yf
from botocore.client import Config

# ── config ────────────────────────────────────────────────────────────────────

TICKER              = "QQQ"
YF_CUTOFF_DAYS      = 59
REQUEST_DELAY       = 0.3
CANDLES_BASE        = f"https://api.marketdata.app/v1/stocks/candles/10/{TICKER}/"
TWELVEDATA_BASE     = "https://api.twelvedata.com/time_series"
TWELVEDATA_RATE_SEC = 8   # free tier: 8 calls/min → 1 per 7.5s; use 8s to be safe

_ET = pytz.timezone("America/New_York")

# ── calendar ──────────────────────────────────────────────────────────────────

_NYSE      = mcal.get_calendar("NYSE")
_valid_days: set[date] = set()


def _load_calendar(start: date, end: date) -> None:
    days = _NYSE.valid_days(start_date=start.isoformat(), end_date=end.isoformat())
    _valid_days.update(d.date() for d in days)


def next_trading_day(d: date) -> date:
    nd = d + timedelta(days=1)
    while nd not in _valid_days:
        nd += timedelta(days=1)
    return nd


# ── R2 ────────────────────────────────────────────────────────────────────────

def _require(name: str) -> str:
    val = os.environ.get(name, "").strip()
    if not val:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return val


def make_r2():
    return boto3.client(
        "s3",
        endpoint_url=f"https://{_require('R2_ACCOUNT_ID')}.r2.cloudflarestorage.com",
        aws_access_key_id=_require("R2_ACCESS_KEY_ID"),
        aws_secret_access_key=_require("R2_SECRET_ACCESS_KEY"),
        config=Config(signature_version="s3v4"),
        region_name="auto",
    )


def r2_key_exists(r2, bucket: str, key: str) -> bool:
    try:
        r2.head_object(Bucket=bucket, Key=key)
        return True
    except Exception:
        return False


def upload_df(r2, bucket: str, df: pd.DataFrame, key: str) -> None:
    buf = io.BytesIO()
    df.to_csv(buf, index=False)
    buf.seek(0)
    r2.put_object(Bucket=bucket, Key=key, Body=buf.read(), ContentType="text/csv")


def load_manifest(r2, bucket: str) -> list[date]:
    obj  = r2.get_object(Bucket=bucket, Key="manifest.json")
    data = json.loads(obj["Body"].read().decode())
    return [date.fromisoformat(d) for d in data.get("dates", [])]


# ── fetchers ──────────────────────────────────────────────────────────────────

def _to_standard(df: pd.DataFrame) -> pd.DataFrame | None:
    """Normalize a yfinance OHLCV dataframe to the standard five-column output."""
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] for c in df.columns]
    if df.index.tz is None:
        df.index = df.index.tz_localize("UTC")
    df.index = df.index.tz_convert(_ET)
    df = df.between_time("09:30", "15:59")
    if df.empty:
        return None
    df = df.reset_index()
    df.columns = [str(c).lower() for c in df.columns]
    dt_col = next((c for c in df.columns if "datetime" in c), None)
    if dt_col and dt_col != "datetime":
        df = df.rename(columns={dt_col: "datetime"})
    df["datetime"] = df["datetime"].astype(str)
    return df[["datetime", "open", "high", "low", "close", "volume"]]


def fetch_yfinance(d: date) -> pd.DataFrame | None:
    for interval in ("5m", "2m", "1m"):
        try:
            raw = yf.download(
                TICKER,
                start=d.isoformat(),
                end=(d + timedelta(days=1)).isoformat(),
                interval=interval,
                auto_adjust=True,
                progress=False,
            )
            if not raw.empty:
                result = _to_standard(raw)
                if result is not None:
                    return result
        except Exception:
            continue
    return None


def fetch_marketdata(d: date, api_key: str) -> pd.DataFrame | None:
    resp = requests.get(
        CANDLES_BASE,
        params={"date": d.isoformat()},
        headers={"Authorization": f"Bearer {api_key}"},
        timeout=30,
    )
    if resp.status_code in (401, 403):
        raise PermissionError(f"MarketData.app auth failed (HTTP {resp.status_code})")
    if resp.status_code == 402:
        raise RuntimeError("QUOTA_EXHAUSTED")
    if resp.status_code == 404:
        return None
    resp.raise_for_status()

    payload = resp.json()
    if payload.get("s") != "ok":
        return None

    timestamps = payload.get("t", [])
    if not timestamps:
        return None

    rows = []
    for i, ts in enumerate(timestamps):
        dt = datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(_ET)
        if dt.hour < 9 or (dt.hour == 9 and dt.minute < 30) or dt.hour >= 16:
            continue
        rows.append({
            "datetime": dt.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "open":     payload["o"][i],
            "high":     payload["h"][i],
            "low":      payload["l"][i],
            "close":    payload["c"][i],
            "volume":   payload["v"][i],
        })
    return pd.DataFrame(rows) if rows else None


def fetch_twelvedata(d: date, api_key: str) -> pd.DataFrame | None:
    resp = requests.get(
        TWELVEDATA_BASE,
        params={
            "symbol":     TICKER,
            "interval":   "5min",
            "start_date": f"{d} 09:30:00",
            "end_date":   f"{d} 16:00:00",
            "outputsize": 100,
            "order":      "ASC",
            "apikey":     api_key,
        },
        timeout=30,
    )
    resp.raise_for_status()
    payload = resp.json()

    if payload.get("status") == "error":
        msg = payload.get("message", "")
        if any(w in msg.lower() for w in ("limit", "quota", "credits", "plan")):
            raise RuntimeError(f"QUOTA_EXHAUSTED: {msg}")
        raise RuntimeError(f"Twelve Data error: {msg}")

    values = payload.get("values", [])
    if not values:
        return None

    rows = [{
        "datetime": v["datetime"],
        "open":     float(v["open"]),
        "high":     float(v["high"]),
        "low":      float(v["low"]),
        "close":    float(v["close"]),
        "volume":   int(float(v.get("volume", 0))),
    } for v in values]

    return pd.DataFrame(rows)


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Backfill QQQ intraday price bars to R2")
    parser.add_argument("--dry-run", action="store_true", help="Show plan without uploading")
    parser.add_argument("--yf-only", action="store_true", help="Skip MarketData.app fallback")
    args = parser.parse_args()

    today      = date.today()
    yf_cutoff  = today - timedelta(days=YF_CUTOFF_DAYS)
    td_key     = os.environ.get("TWELVEDATA_API_KEY", "").strip()
    md_key     = os.environ.get("MARKETDATA_API_KEY", "").strip()

    r2     = make_r2()
    bucket = _require("R2_BUCKET_NAME")

    chain_dates = load_manifest(r2, bucket)
    if not chain_dates:
        print("Manifest is empty — nothing to backfill.")
        return

    _load_calendar(
        min(chain_dates) - timedelta(days=10),
        max(chain_dates) + timedelta(days=120),
    )

    # Build work list
    todo: list[tuple[date, date, str, str]] = []   # (chain_day, exp_day, r2_key, source)
    already_done = 0
    future_skip  = 0

    for d in sorted(chain_dates):
        exp_day   = next_trading_day(d)
        price_key = f"prices/qqq_intraday_{exp_day.strftime('%Y%m%d')}.csv"

        if exp_day >= today:
            future_skip += 1
            continue
        if r2_key_exists(r2, bucket, price_key):
            already_done += 1
            continue

        if exp_day >= yf_cutoff:
            source = "yfinance"
        elif args.yf_only:
            source = "skip"
        elif td_key:
            source = "twelvedata"
        elif md_key:
            source = "marketdata"
        else:
            source = "skip"

        todo.append((d, exp_day, price_key, source))

    n_skip_nosrc = sum(1 for *_, src in todo if src == "skip")

    print(f"QQQ intraday backfill")
    print(f"  Chain dates in manifest : {len(chain_dates)}")
    print(f"  Already in R2           : {already_done}")
    print(f"  Expiry not yet passed   : {future_skip}")
    print(f"  To fetch via yfinance   : {sum(1 for *_, s in todo if s == 'yfinance')}")
    print(f"  To fetch via twelvedata : {sum(1 for *_, s in todo if s == 'twelvedata')}")
    print(f"  To fetch via marketdata : {sum(1 for *_, s in todo if s == 'marketdata')}")
    if n_skip_nosrc:
        print(f"  Will skip (no source)   : {n_skip_nosrc}  "
              f"(set TWELVEDATA_API_KEY to cover dates before {yf_cutoff})")

    if args.dry_run or not todo:
        if todo:
            sample = [t for t in todo if t[3] != "skip"][:5]
            if sample:
                print(f"\n  Sample (first 5 fetchable):")
                for d, exp_day, key, src in sample:
                    print(f"    chain {d}  ->  exp {exp_day}  [{src}]  ->  {key}")
        return

    print()
    uploaded = skipped = errors = 0

    for i, (d, exp_day, price_key, source) in enumerate(todo, 1):
        label = f"[{i}/{len(todo)}] exp {exp_day}"

        if source == "skip":
            print(f"  {label}  SKIP (no source for dates before {yf_cutoff})")
            skipped += 1
            continue

        df = None

        if source == "yfinance":
            df = fetch_yfinance(exp_day)
            if df is not None:
                print(f"  {label}  yfinance    {len(df):>2} bars  ->  {price_key}")
            else:
                print(f"  {label}  yfinance returned no data")

        elif source == "twelvedata":
            try:
                df = fetch_twelvedata(exp_day, td_key)
                if df is not None:
                    print(f"  {label}  twelvedata  {len(df):>2} bars  ->  {price_key}")
                else:
                    print(f"  {label}  twelvedata returned no data")
                time.sleep(TWELVEDATA_RATE_SEC)
            except RuntimeError as e:
                if "QUOTA_EXHAUSTED" in str(e):
                    print(f"\nTwelve Data quota hit after {uploaded} upload(s). Re-run tomorrow.")
                    break
                print(f"  {label}  ERROR: {e}")
                errors += 1
                continue

        elif source == "marketdata":
            try:
                df = fetch_marketdata(exp_day, md_key)
                if df is not None:
                    print(f"  {label}  marketdata  {len(df):>2} bars  ->  {price_key}")
                else:
                    print(f"  {label}  marketdata returned no data")
                time.sleep(REQUEST_DELAY)
            except PermissionError as e:
                print(f"\nAuth error: {e}")
                raise SystemExit(1)
            except RuntimeError as e:
                if "QUOTA_EXHAUSTED" in str(e):
                    print(f"\nCredit quota exhausted after {uploaded} upload(s). Re-run to continue.")
                    break
                print(f"  {label}  ERROR: {e}")
                errors += 1
                continue

        if df is None:
            skipped += 1
            continue

        upload_df(r2, bucket, df, price_key)
        uploaded += 1

    print(f"\nDone.  uploaded={uploaded}  skipped={skipped}  errors={errors}")


if __name__ == "__main__":
    main()
