#!/usr/bin/env python3
"""
pipeline.py — Daily QQQ 0DTE options chain pull and Cloudflare R2 upload.

Runs as a Railway cron job after market close. Fetches any trading days
within the lookback window that are absent from the R2 manifest, uploads
them as CSVs, and updates manifest.json.

Required environment variables (set in Railway):
    MARKETDATA_API_KEY      MarketData.app bearer token
    R2_ACCOUNT_ID           Cloudflare account ID
    R2_ACCESS_KEY_ID        R2 API token key ID
    R2_SECRET_ACCESS_KEY    R2 API token secret
    R2_BUCKET_NAME          R2 bucket name
"""

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

# ── config ─────────────────────────────────────────────────────────────────────

TICKER        = "QQQ"
BASE_URL      = f"https://api.marketdata.app/v1/options/chain/{TICKER}/"
STRIKE_WINDOW = 33
REQUEST_DELAY = 0.3
LOOKBACK_DAYS = 7   # re-check this many calendar days back to catch any gaps

# ── environment ────────────────────────────────────────────────────────────────

def _require(name: str) -> str:
    val = os.environ.get(name, "").strip()
    if not val:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return val

# ── NYSE calendar ──────────────────────────────────────────────────────────────

_NYSE = mcal.get_calendar("NYSE")
_valid_days: set[date] = set()


def _load_calendar(start: date, end: date) -> None:
    days = _NYSE.valid_days(start_date=start.isoformat(), end_date=end.isoformat())
    _valid_days.update(d.date() for d in days)


def is_trading_day(d: date) -> bool:
    return d in _valid_days


def next_trading_day(d: date) -> date:
    nd = d + timedelta(days=1)
    while not is_trading_day(nd):
        nd += timedelta(days=1)
    return nd


def prior_trading_day(d: date) -> date:
    nd = d
    while not is_trading_day(nd):
        nd -= timedelta(days=1)
    return nd


def nominal_friday(d: date) -> date:
    return d + timedelta(days=(4 - d.weekday()) % 7)


def last_calendar_day_of_month(year: int, month: int) -> date:
    if month == 12:
        return date(year + 1, 1, 1) - timedelta(days=1)
    return date(year, month + 1, 1) - timedelta(days=1)


def target_expirations(as_of: date) -> list[tuple[str, date]]:
    this_fri = prior_trading_day(nominal_friday(as_of))
    next_fri = prior_trading_day(nominal_friday(as_of) + timedelta(days=7))
    nm = as_of.month % 12 + 1
    ny = as_of.year + (1 if as_of.month == 12 else 0)
    candidates = [
        ("0DTE", as_of),
        ("+1D",  next_trading_day(as_of)),
        ("EoW",  this_fri),
        ("EoNW", next_fri),
        ("EoM",  prior_trading_day(last_calendar_day_of_month(as_of.year, as_of.month))),
        ("EoNM", prior_trading_day(last_calendar_day_of_month(ny, nm))),
    ]
    seen: set[date] = set()
    result = []
    for label, d in candidates:
        if d not in seen:
            seen.add(d)
            result.append((label, d))
    return result


def trading_days_in_range(start: date, end: date) -> list[date]:
    return sorted(d for d in _valid_days if start <= d <= end)


# ── R2 ─────────────────────────────────────────────────────────────────────────

def make_r2(account_id: str, key_id: str, secret: str):
    return boto3.client(
        "s3",
        endpoint_url=f"https://{account_id}.r2.cloudflarestorage.com",
        aws_access_key_id=key_id,
        aws_secret_access_key=secret,
        config=Config(signature_version="s3v4"),
        region_name="auto",
    )


def load_manifest(r2, bucket: str) -> dict:
    try:
        obj = r2.get_object(Bucket=bucket, Key="manifest.json")
        return json.loads(obj["Body"].read().decode())
    except Exception:
        return {"dates": []}


def save_manifest(r2, bucket: str, manifest: dict) -> None:
    manifest["updated_at"] = datetime.now(timezone.utc).isoformat()
    r2.put_object(
        Bucket=bucket,
        Key="manifest.json",
        Body=json.dumps(manifest, indent=2, sort_keys=True).encode(),
        ContentType="application/json",
    )


def upload_df(r2, bucket: str, df: pd.DataFrame, key: str) -> None:
    buf = io.BytesIO()
    df.to_csv(buf, index=False)
    buf.seek(0)
    r2.put_object(Bucket=bucket, Key=key, Body=buf.read(), ContentType="text/csv")


def _r2_key_exists(r2, bucket: str, key: str) -> bool:
    try:
        r2.head_object(Bucket=bucket, Key=key)
        return True
    except Exception:
        return False


_ET = pytz.timezone("America/New_York")


def fetch_intraday(d: date) -> pd.DataFrame | None:
    """Fetch 10-min (fallback 5-min) OHLCV bars for QQQ on date d via yfinance.
    Filters to regular market hours (09:30–15:59 ET). Returns None if unavailable."""
    for interval in ("10m", "5m"):
        try:
            df = yf.download(
                "QQQ",
                start=d.isoformat(),
                end=(d + timedelta(days=1)).isoformat(),
                interval=interval,
                auto_adjust=True,
                progress=False,
            )
            if not df.empty:
                break
        except Exception:
            continue
    else:
        return None

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


# ── MarketData.app ─────────────────────────────────────────────────────────────

class SessionNotClosedError(Exception):
    pass


def _api_call(params: dict, api_key: str) -> dict:
    resp = requests.get(
        BASE_URL,
        params=params,
        headers={"Authorization": f"Bearer {api_key}"},
        timeout=30,
    )
    if resp.status_code in (401, 403):
        raise PermissionError(f"HTTP {resp.status_code} — check MARKETDATA_API_KEY")
    if resp.status_code == 402:
        errmsg = (resp.json().get("errmsg", "") if resp.content else "")
        if "session" in errmsg.lower() and "closed" in errmsg.lower():
            raise SessionNotClosedError(errmsg)
        raise RuntimeError(f"QUOTA_EXHAUSTED — {errmsg}")
    if resp.status_code == 404:
        return {}
    resp.raise_for_status()
    payload = resp.json()
    if payload.get("s") == "no_data":
        return {}
    if payload.get("s") != "ok":
        raise RuntimeError(f"API error: {payload}")
    return payload


def get_spot(d: date, probe_expiry: date, api_key: str) -> float | None:
    for delta_band in (".45-.55", ".35-.65"):
        payload = _api_call(
            {"date": d.isoformat(), "expiration": probe_expiry.isoformat(),
             "side": "call", "delta": delta_band},
            api_key,
        )
        prices = payload.get("underlyingPrice")
        if prices:
            return prices[0]
    payload = _api_call(
        {"date": d.isoformat(), "expiration": probe_expiry.isoformat(),
         "side": "call", "strikeLimit": 2},
        api_key,
    )
    prices = payload.get("underlyingPrice")
    return prices[0] if prices else None


def fetch_day(d: date, api_key: str) -> list[dict]:
    targets = target_expirations(d)
    spot    = get_spot(d, targets[0][1], api_key)
    if spot is None:
        raise RuntimeError(f"Could not determine ATM price for {d}")
    low, high = spot - STRIKE_WINDOW, spot + STRIKE_WINDOW
    rows: list[dict] = []
    for _label, expiry in targets:
        payload = _api_call(
            {"date": d.isoformat(), "expiration": expiry.isoformat(),
             "strike": f"{low:.0f}-{high:.0f}"},
            api_key,
        )
        if payload:
            n = len(payload.get("optionSymbol", []))
            for i in range(n):
                exp_ts = payload["expiration"][i]
                exp_str = datetime.fromtimestamp(exp_ts, tz=timezone.utc).date().isoformat()
                rows.append({
                    "TradeDate":       d.isoformat(),
                    "Expiration":      exp_str,
                    "Strike":          payload["strike"][i],
                    "Type":            payload["side"][i],
                    "OptionSymbol":    payload["optionSymbol"][i],
                    "DTE":             payload["dte"][i],
                    "OpenInterest":    payload["openInterest"][i],
                    "Volume":          payload["volume"][i],
                    "Bid":             payload["bid"][i],
                    "Mid":             payload["mid"][i],
                    "Ask":             payload["ask"][i],
                    "Last":            payload["last"][i],
                    "IV":              payload["iv"][i],
                    "Delta":           payload["delta"][i],
                    "Gamma":           payload["gamma"][i],
                    "Theta":           payload["theta"][i],
                    "Vega":            payload["vega"][i],
                    "UnderlyingPrice": payload["underlyingPrice"][i],
                })
        time.sleep(REQUEST_DELAY)
    return rows


# ── main ───────────────────────────────────────────────────────────────────────

def main():
    api_key    = _require("MARKETDATA_API_KEY")
    account_id = _require("R2_ACCOUNT_ID")
    key_id     = _require("R2_ACCESS_KEY_ID")
    secret     = _require("R2_SECRET_ACCESS_KEY")
    bucket     = _require("R2_BUCKET_NAME")

    today = date.today()
    start = today - timedelta(days=LOOKBACK_DAYS)
    _load_calendar(start - timedelta(days=10), today + timedelta(days=120))

    r2       = make_r2(account_id, key_id, secret)
    manifest = load_manifest(r2, bucket)
    existing = set(manifest.get("dates", []))

    todo = [d for d in trading_days_in_range(start, today)
            if d.isoformat() not in existing]

    if not todo:
        print(f"Nothing to fetch — all trading days in the {LOOKBACK_DAYS}-day window are already in R2.")
        return

    print(f"Fetching {len(todo)} missing day(s): {todo[0]} .. {todo[-1]}")
    newly_added: list[str] = []

    for d in todo:
        print(f"  {d} ... ", end="", flush=True)
        try:
            rows = fetch_day(d, api_key)
            if not rows:
                print("no data returned — skipping")
                continue
            df  = pd.DataFrame(rows)
            key = f"raw/qqq_chain_{d.strftime('%Y%m%d')}.csv"
            upload_df(r2, bucket, df, key)
            newly_added.append(d.isoformat())
            print(f"{len(rows)} contracts  →  {key}")

            exp_day = next_trading_day(d)
            if exp_day <= today:
                price_key = f"prices/qqq_intraday_{exp_day.strftime('%Y%m%d')}.csv"
                if _r2_key_exists(r2, bucket, price_key):
                    print(f"    intraday {exp_day}: already in R2")
                else:
                    price_df = fetch_intraday(exp_day)
                    if price_df is not None:
                        upload_df(r2, bucket, price_df, price_key)
                        print(f"    intraday {exp_day}: {len(price_df)} bars  →  {price_key}")
                    else:
                        print(f"    intraday {exp_day}: no data from yfinance")
        except SessionNotClosedError as e:
            print(f"session not yet closed — skipping  ({e})")
        except RuntimeError as e:
            if str(e).startswith("QUOTA_EXHAUSTED"):
                print(f"\nCredit quota exhausted after {len(newly_added)} day(s).")
                break
            print(f"ERROR: {e} — skipping")
        except PermissionError as e:
            print(f"\nAuth error: {e}")
            raise SystemExit(1)

    if newly_added:
        manifest["dates"] = sorted(existing | set(newly_added))
        save_manifest(r2, bucket, manifest)
        print(f"\nDone — {len(newly_added)} new day(s) added to manifest.")
    else:
        print("\nNo new days uploaded.")


if __name__ == "__main__":
    main()
