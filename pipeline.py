#!/usr/bin/env python3
"""
pipeline.py — Daily QQQ 0DTE options chain pull and Cloudflare R2 upload.

Runs as a Railway cron job after market close. Fetches today's option chain
via DXLink (tastytrade streamer), uploads the CSV, and updates manifest.json.
Also fetches intraday price bars for today's expiry day via yfinance.

Required environment variables (set in Railway):
    TASTY_LOGIN             Tastytrade username / email
    TASTY_PASSWORD          Tastytrade password  (not needed if remember-token exists in R2)
    TASTY_TOTP_SECRET       TOTP secret (base32) for 2FA -- optional if remember-token exists
    R2_ACCOUNT_ID           Cloudflare account ID
    R2_ACCESS_KEY_ID        R2 API token key ID
    R2_SECRET_ACCESS_KEY    R2 API token secret
    R2_BUCKET_NAME          R2 bucket name
"""

import io
import json
import math
import os
import threading
import time
from datetime import date, datetime, timedelta, timezone
from typing import Optional

import boto3
import pandas as pd
import pandas_market_calendars as mcal
import pytz
import requests
import websocket
import yfinance as yf
from botocore.client import Config

# -- config --------------------------------------------------------------------

TICKER        = "QQQ"
STRIKE_WINDOW = 33
TASTY_BASE    = "https://api.tastyworks.com"
DXLINK_WAIT   = 90   # max seconds to wait for OI data to populate

_ET = pytz.timezone("America/New_York")

# -- environment ---------------------------------------------------------------

def _require(name: str) -> str:
    val = os.environ.get(name, "").strip()
    if not val:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return val

# -- NYSE calendar -------------------------------------------------------------

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


# -- R2 ------------------------------------------------------------------------

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


# -- intraday prices (yfinance) ------------------------------------------------

def fetch_intraday(d: date) -> pd.DataFrame | None:
    """Fetch 5-min OHLCV bars for QQQ on date d via yfinance."""
    for interval in ("5m", "2m", "1m"):
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


# -- tastytrade auth -----------------------------------------------------------

R2_REMEMBER_TOKEN_KEY = "auth/remember_token.json"


def _load_remember_token(r2, bucket: str) -> Optional[str]:
    try:
        body = r2.get_object(Bucket=bucket, Key=R2_REMEMBER_TOKEN_KEY)["Body"].read()
        return json.loads(body)["remember_token"]
    except Exception:
        pass
    return os.environ.get("TASTY_REMEMBER_TOKEN")


def _save_remember_token(r2, bucket: str, token: str) -> None:
    r2.put_object(
        Bucket=bucket,
        Key=R2_REMEMBER_TOKEN_KEY,
        Body=json.dumps({"remember_token": token,
                         "updated_at": datetime.now(timezone.utc).isoformat()}).encode(),
        ContentType="application/json",
    )
    print("  remember-token rotated and saved to R2")


def _complete_device_challenge(login: str, password: str, challenge_token: str) -> requests.Response:
    import pyotp
    requests.post(
        f"{TASTY_BASE}/device-challenge",
        headers={"Content-Type": "application/json",
                 "X-Tastyworks-Challenge-Token": challenge_token},
        timeout=10,
    )
    otp = pyotp.TOTP(os.environ["TASTY_TOTP_SECRET"]).now()
    return requests.post(
        f"{TASTY_BASE}/sessions",
        json={"login": login, "password": password, "remember-me": True},
        headers={
            "Content-Type": "application/json",
            "X-Tastyworks-Challenge-Token": challenge_token,
            "X-Tastyworks-OTP": otp,
        },
        timeout=15,
    )


def tasty_auth(login: str, r2, bucket: str) -> dict:
    remember_token = _load_remember_token(r2, bucket)
    if remember_token:
        print("  tasty_auth -- trying remember-token")
        resp = requests.post(
            f"{TASTY_BASE}/sessions",
            json={"login": login, "remember-token": remember_token, "remember-me": True},
            headers={"Content-Type": "application/json"},
            timeout=15,
        )
        if resp.status_code == 201:
            data      = resp.json()["data"]
            new_token = data.get("remember-token")
            if new_token:
                _save_remember_token(r2, bucket, new_token)
            resp2 = requests.get(
                f"{TASTY_BASE}/api-quote-tokens",
                headers={"Authorization": data["session-token"]},
                timeout=10,
            )
            resp2.raise_for_status()
            d = resp2.json()["data"]
            return {
                "session_token":  data["session-token"],
                "streamer_token": d["token"],
                "streamer_url":   (d.get("dxlink-url") or d.get("websocket-url") or
                                   "wss://tasty-openapi-ws.dxfeed.com/realtime"),
            }
        print(f"  remember-token rejected ({resp.status_code}), falling back to password")

    password = _require("TASTY_PASSWORD")
    resp = requests.post(
        f"{TASTY_BASE}/sessions",
        json={"login": login, "password": password, "remember-me": True},
        headers={"Content-Type": "application/json"},
        timeout=15,
    )
    if resp.status_code == 403:
        challenge_token = resp.headers.get("X-Tastyworks-Challenge-Token")
        if not challenge_token:
            resp.raise_for_status()
        print("  device challenge -- completing with TOTP")
        resp = _complete_device_challenge(login, password, challenge_token)

    resp.raise_for_status()
    data          = resp.json()["data"]
    session_token = data["session-token"]
    new_token     = data.get("remember-token")
    if new_token:
        _save_remember_token(r2, bucket, new_token)

    resp2 = requests.get(
        f"{TASTY_BASE}/api-quote-tokens",
        headers={"Authorization": session_token},
        timeout=10,
    )
    resp2.raise_for_status()
    d = resp2.json()["data"]
    return {
        "session_token":  session_token,
        "streamer_token": d["token"],
        "streamer_url":   (d.get("dxlink-url") or d.get("websocket-url") or
                           "wss://tasty-openapi-ws.dxfeed.com/realtime"),
    }


# -- option chain structure ----------------------------------------------------

def _strike_str(strike: float) -> str:
    if strike == int(strike):
        return str(int(strike))
    return f"{strike * 100:.0f}".rstrip("0")


def _dxlink_symbol(occ_symbol: str) -> str:
    occ = occ_symbol.replace(" ", "")
    i = 0
    while i < len(occ) and not occ[i].isdigit():
        i += 1
    underlying = occ[:i]
    date_part  = occ[i:i+6]
    side       = occ[i+6]
    strike     = int(occ[i+7:]) / 1000.0
    return f".{underlying}{date_part}{side}{_strike_str(strike)}"


def _build_symbol(strike: float, exp_date: str, option_type: str) -> str:
    yy, mm, dd = exp_date[2:4], exp_date[5:7], exp_date[8:10]
    side = "C" if option_type.lower() == "call" else "P"
    return f".{TICKER}{yy}{mm}{dd}{side}{_strike_str(strike)}"


def load_chain_for_pipeline(session_token: str, today: date,
                             targets: list[tuple[str, date]], spot: float) -> list[dict]:
    """
    Load multi-expiry chain from tastytrade REST API.
    Returns strike rows for all target expirations within +/-STRIKE_WINDOW of spot.
    """
    resp = requests.get(
        f"{TASTY_BASE}/option-chains/{TICKER}/nested",
        headers={"Authorization": session_token},
        timeout=30,
    )
    resp.raise_for_status()

    items = resp.json().get("data", {}).get("items", [])
    if not items:
        raise RuntimeError("empty option chain response from tastytrade")

    target_dates = {exp.isoformat() for _, exp in targets}
    low, high    = spot - STRIKE_WINDOW, spot + STRIKE_WINDOW
    expirations  = items[0].get("expirations", [])

    out = []
    for exp in expirations:
        exp_date = exp.get("expiration-date", "")
        if exp_date not in target_dates:
            continue
        for s in exp.get("strikes", []):
            strike = float(s.get("strike-price", 0))
            if not (low <= strike <= high):
                continue
            c = s.get("call", {})
            p = s.get("put",  {})
            if isinstance(c, str):
                call_occ = c.replace(" ", "")
                call_sym = _dxlink_symbol(call_occ) if call_occ else _build_symbol(strike, exp_date, "call")
            else:
                call_occ = c.get("symbol", "")
                call_sym = (c.get("streamer-symbol") or
                            (_dxlink_symbol(call_occ) if call_occ else _build_symbol(strike, exp_date, "call")))
            if isinstance(p, str):
                put_occ = p.replace(" ", "")
                put_sym = _dxlink_symbol(put_occ) if put_occ else _build_symbol(strike, exp_date, "put")
            else:
                put_occ  = p.get("symbol", "")
                put_sym  = (p.get("streamer-symbol") or
                            (_dxlink_symbol(put_occ) if put_occ else _build_symbol(strike, exp_date, "put")))
            out.append({
                "strike":   strike,
                "exp_date": exp_date,
                "call_sym": call_sym,
                "put_sym":  put_sym,
                "call_occ": call_occ,
                "put_occ":  put_occ,
            })

    n_exps = len({r["exp_date"] for r in out})
    print(f"  chain loaded: {n_exps} expiries, {len(out)} strike rows")
    return out


# -- DXLink one-shot feed ------------------------------------------------------

class DXLinkSnapshot:
    """Simplified one-shot DXLink client for EOD chain snapshot. No reconnect."""

    _DXLINK_VERSION = "0.1-js/1.0.0"

    def __init__(self, url: str, token: str):
        self._url        = url
        self._token      = token
        self._state: dict[str, dict] = {}
        self._lock       = threading.Lock()
        self._ws: Optional[websocket.WebSocketApp] = None
        self._ready      = threading.Event()
        self._subs: list[dict] = []
        self._subscribed = False

    def set_subscriptions(self, option_symbols: list[str],
                          price_symbols: Optional[list[str]] = None) -> None:
        self._subs = []
        for sym in option_symbols:
            for et in ("Quote", "Summary", "Trade", "Greeks"):
                self._subs.append({"type": et, "symbol": sym})
        for sym in (price_symbols or []):
            for et in ("Quote", "Trade", "Summary"):
                self._subs.append({"type": et, "symbol": sym})

    def start(self) -> None:
        self._ws = websocket.WebSocketApp(
            self._url,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
        )
        self._thread = threading.Thread(target=self._ws.run_forever, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        if self._ws:
            self._ws.close()

    def wait_ready(self, timeout: float = 30.0) -> bool:
        return self._ready.wait(timeout=timeout)

    def wait_for_oi(self, symbols: list[str], timeout: float = DXLINK_WAIT) -> bool:
        """Return True once at least half of the subscribed symbols have OI data."""
        threshold = max(1, len(symbols) // 2)
        deadline  = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self._lock:
                oi_count = sum(1 for s in symbols
                               if self._state.get(s, {}).get("oi") is not None)
            if oi_count >= threshold:
                return True
            time.sleep(2)
        return False

    def get_state(self) -> dict[str, dict]:
        with self._lock:
            return {k: dict(v) for k, v in self._state.items()}

    def _send(self, msg: dict) -> None:
        if self._ws:
            self._ws.send(json.dumps(msg))

    def _on_open(self, ws) -> None:
        print("  DXLink connected")
        self._send({
            "type": "SETUP", "channel": 0,
            "version": self._DXLINK_VERSION,
            "keepaliveTimeout": 60,
            "acceptKeepaliveTimeout": 60,
        })

    def _on_message(self, ws, raw: str) -> None:
        try:
            msg = json.loads(raw)
        except Exception:
            return
        mtype = msg.get("type")

        if mtype == "SETUP":
            self._send({"type": "AUTH", "channel": 0, "token": self._token})

        elif mtype == "AUTH_STATE":
            if msg.get("state") == "AUTHORIZED":
                print("  DXLink authorized -- requesting channel")
                self._send({
                    "type": "CHANNEL_REQUEST", "channel": 1,
                    "service": "FEED",
                    "parameters": {"contract": "AUTO"},
                })
            else:
                print(f"  DXLink auth failed: {msg}")

        elif mtype == "CHANNEL_OPENED":
            self._send({
                "type": "FEED_SETUP", "channel": 1,
                "acceptDataFormat": "FULL",
                "acceptEventFields": {
                    "Quote":   ["eventType", "eventSymbol", "bidPrice", "askPrice"],
                    "Summary": ["eventType", "eventSymbol", "openInterest",
                                "prevDayClosePrice", "dayOpenPrice"],
                    "Trade":   ["eventType", "eventSymbol", "dayVolume", "price"],
                    "Greeks":  ["eventType", "eventSymbol",
                                "volatility", "delta", "gamma", "theta", "vega"],
                },
            })

        elif mtype == "FEED_CONFIG":
            if self._subscribed:
                return
            self._subscribed = True
            print(f"  DXLink subscribing to {len(self._subs)} event/symbol pairs")
            batch_size = 200
            for i in range(0, len(self._subs), batch_size):
                self._send({
                    "type": "FEED_SUBSCRIPTION", "channel": 1,
                    "reset": i == 0,
                    "add": self._subs[i:i + batch_size],
                })
            self._ready.set()

        elif mtype == "FEED_DATA":
            self._ingest(msg.get("data", []))

        elif mtype == "KEEPALIVE":
            self._send({"type": "KEEPALIVE", "channel": 0})

        elif mtype == "ERROR":
            print(f"  DXLink server error: {msg}")

    @staticmethod
    def _to_int(val) -> Optional[int]:
        try:
            f = float(val)
            return None if math.isnan(f) else int(f)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _to_float(val) -> Optional[float]:
        try:
            f = float(val)
            return None if math.isnan(f) else f
        except (TypeError, ValueError):
            return None

    def _ingest(self, data) -> None:
        if not isinstance(data, list):
            return
        for event in data:
            if not isinstance(event, dict):
                continue
            et  = event.get("eventType")
            sym = event.get("eventSymbol")
            if not sym:
                continue
            with self._lock:
                s = self._state.setdefault(sym, {})
                if et == "Quote":
                    b = self._to_float(event.get("bidPrice"))
                    a = self._to_float(event.get("askPrice"))
                    if b is not None: s["bid"] = b
                    if a is not None: s["ask"] = a
                elif et == "Summary":
                    oi = self._to_int(event.get("openInterest"))
                    if oi is not None: s["oi"] = oi
                elif et == "Trade":
                    vol = self._to_int(event.get("dayVolume"))
                    if vol is not None: s["volume"] = vol
                    px = self._to_float(event.get("price"))
                    if px is not None: s["last"] = px
                elif et == "Greeks":
                    for field in ("volatility", "delta", "gamma", "theta", "vega"):
                        v = self._to_float(event.get(field))
                        if v is not None: s[field] = v

    def _on_error(self, ws, error) -> None:
        print(f"  DXLink error: {error}")

    def _on_close(self, ws, code, msg) -> None:
        print(f"  DXLink closed (code={code})")
        self._ready.clear()


# -- fetch today's chain -------------------------------------------------------

def fetch_via_dxlink(today: date, auth: dict) -> list[dict]:
    """
    Connect to DXLink, subscribe to today's 6-tier option chain, wait for
    OI data to populate, take a snapshot, and return rows in pipeline CSV format.
    """
    targets = target_expirations(today)

    # Quick spot estimate for strike window (yfinance fast_info avoids a full download)
    try:
        spot = float(yf.Ticker(TICKER).fast_info.last_price)
        if math.isnan(spot):
            raise ValueError
    except Exception:
        spot = float(yf.download(TICKER, period="1d", progress=False)["Close"].iloc[-1])
    print(f"  spot ~ ${spot:.2f}")

    chain_rows = load_chain_for_pipeline(auth["session_token"], today, targets, spot)
    if not chain_rows:
        raise RuntimeError("no chain rows returned from tastytrade chain API")

    call_syms = [r["call_sym"] for r in chain_rows]
    put_syms  = [r["put_sym"]  for r in chain_rows]
    all_opt_syms = list(dict.fromkeys(call_syms + put_syms))

    feed = DXLinkSnapshot(auth["streamer_url"], auth["streamer_token"])
    feed.set_subscriptions(all_opt_syms, price_symbols=[TICKER])
    feed.start()

    if not feed.wait_ready(timeout=30):
        feed.stop()
        raise RuntimeError("DXLink did not become ready within 30s")

    print(f"  waiting up to {DXLINK_WAIT}s for OI data ...")
    feed.wait_for_oi(all_opt_syms)

    state = feed.get_state()
    feed.stop()

    # Use the official close price as UnderlyingPrice so ATM is anchored to the close.
    # DXLink bid/ask goes stale after 4pm; fast_info.last_price matches the official close.
    underlying_price = spot  # spot was already fetched from fast_info.last_price above

    rows: list[dict] = []
    for r in chain_rows:
        exp_date = r["exp_date"]
        dte      = (date.fromisoformat(exp_date) - today).days
        for side, sym, occ in [("call", r["call_sym"], r["call_occ"]),
                                ("put",  r["put_sym"],  r["put_occ"])]:
            s   = state.get(sym, {})
            bid = s.get("bid")
            ask = s.get("ask")
            mid = round((bid + ask) / 2, 4) if bid is not None and ask is not None else None
            rows.append({
                "TradeDate":       today.isoformat(),
                "Expiration":      exp_date,
                "Strike":          r["strike"],
                "Type":            side,
                "OptionSymbol":    occ or sym,
                "DTE":             dte,
                "OpenInterest":    s.get("oi", 0),
                "Volume":          s.get("volume", 0),
                "Bid":             bid,
                "Mid":             mid,
                "Ask":             ask,
                "Last":            s.get("last"),
                "IV":              s.get("volatility"),
                "Delta":           s.get("delta"),
                "Gamma":           s.get("gamma"),
                "Theta":           s.get("theta"),
                "Vega":            s.get("vega"),
                "UnderlyingPrice": underlying_price,
            })

    oi_hits = sum(1 for row in rows if row["OpenInterest"])
    print(f"  snapshot: {len(rows)} contracts, {oi_hits} with OI data")
    return rows


# -- main ----------------------------------------------------------------------

def main():
    login      = _require("TASTY_LOGIN")
    account_id = _require("R2_ACCOUNT_ID")
    key_id     = _require("R2_ACCESS_KEY_ID")
    secret     = _require("R2_SECRET_ACCESS_KEY")
    bucket     = _require("R2_BUCKET_NAME")

    today = date.today()
    _load_calendar(today - timedelta(days=10), today + timedelta(days=120))

    if not is_trading_day(today):
        print(f"{today} is not a trading day -- nothing to do.")
        return

    r2       = make_r2(account_id, key_id, secret)
    manifest = load_manifest(r2, bucket)
    existing = set(manifest.get("dates", []))

    # Chain collection
    if today.isoformat() in existing:
        print(f"Chain for {today} already in R2 -- skipping chain fetch.")
    else:
        print(f"Fetching chain for {today} via DXLink ...")
        auth = tasty_auth(login, r2, bucket)
        rows = fetch_via_dxlink(today, auth)
        if not rows:
            print("No data returned -- aborting.")
            return
        df  = pd.DataFrame(rows)
        key = f"raw/qqq_chain_{today.strftime('%Y%m%d')}.csv"
        upload_df(r2, bucket, df, key)
        manifest["dates"] = sorted(existing | {today.isoformat()})
        save_manifest(r2, bucket, manifest)
        print(f"  {len(rows)} contracts -> {key}")

    # Intraday prices for today.
    # Today's price series is the expiry-day panel for yesterday's chain (captured D-1, expiring D).
    # Tomorrow's prices don't exist yet and are intentionally left blank in the viewer.
    price_key = f"prices/qqq_intraday_{today.strftime('%Y%m%d')}.csv"
    if _r2_key_exists(r2, bucket, price_key):
        print(f"  intraday {today}: already in R2")
    else:
        price_df = fetch_intraday(today)
        if price_df is not None:
            upload_df(r2, bucket, price_df, price_key)
            print(f"  intraday {today}: {len(price_df)} bars -> {price_key}")
        else:
            print(f"  intraday {today}: no data from yfinance")


if __name__ == "__main__":
    main()
