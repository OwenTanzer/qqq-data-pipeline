#!/usr/bin/env python3
"""
seed.py — One-time bootstrap: upload existing historical CSVs and OIranges to R2.

Run locally (not on Railway) after creating your R2 bucket. Point --dir at
QQQ_options/historical/resources/ and it will upload everything and write
an initial manifest.json.

Usage:
    R2_ACCOUNT_ID=... R2_ACCESS_KEY_ID=... R2_SECRET_ACCESS_KEY=... R2_BUCKET_NAME=... \
        python seed.py --dir /path/to/QQQ_options/historical/resources

Or set variables in a .env file and export them first.
"""

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import boto3
import pandas as pd
from botocore.client import Config


def make_r2(account_id: str, key_id: str, secret: str):
    return boto3.client(
        "s3",
        endpoint_url=f"https://{account_id}.r2.cloudflarestorage.com",
        aws_access_key_id=key_id,
        aws_secret_access_key=secret,
        config=Config(signature_version="s3v4"),
        region_name="auto",
    )


def upload(r2, bucket: str, local: Path, key: str, content_type: str = "text/csv") -> None:
    r2.upload_file(
        str(local), bucket, key,
        ExtraArgs={"ContentType": content_type},
    )
    print(f"  ✓  {key}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", required=True,
                        help="Path to QQQ_options/historical/resources/")
    args = parser.parse_args()

    resources = Path(args.dir).expanduser().resolve()
    raw_dir   = resources / "raw"
    opex_dir  = raw_dir / "opex"
    derived   = resources / "derived"

    account_id = os.environ["R2_ACCOUNT_ID"]
    key_id     = os.environ["R2_ACCESS_KEY_ID"]
    secret     = os.environ["R2_SECRET_ACCESS_KEY"]
    bucket     = os.environ["R2_BUCKET_NAME"]

    r2 = make_r2(account_id, key_id, secret)
    dates: set[str] = set()

    # ── raw daily CSVs ─────────────────────────────────────────────────────────
    raw_csvs = sorted(raw_dir.glob("qqq_chain_*.csv"))
    print(f"Uploading {len(raw_csvs)} daily chain CSVs...")
    for p in raw_csvs:
        upload(r2, bucket, p, f"raw/{p.name}")
        ds = p.stem.split("_")[-1]
        dates.add(f"{ds[:4]}-{ds[4:6]}-{ds[6:]}")

    # ── opex CSVs ──────────────────────────────────────────────────────────────
    if opex_dir.exists():
        opex_csvs = sorted(opex_dir.glob("qqq_chain_*.csv"))
        print(f"Uploading {len(opex_csvs)} opex CSVs...")
        for p in opex_csvs:
            upload(r2, bucket, p, f"raw/opex/{p.name}")
            ds = p.stem.split("_")[-1]
            dates.add(f"{ds[:4]}-{ds[4:6]}-{ds[6:]}")

    # ── OIranges ───────────────────────────────────────────────────────────────
    ranges_src = None
    for name in ("OIranges_new.csv", "OIranges.csv"):
        p = derived / name
        if p.exists():
            df = pd.read_csv(p)
            if "0DTE_Regular" in df.get("Tier", pd.Series()).values:
                ranges_src = p
                break
    if ranges_src:
        print(f"Uploading OIranges from {ranges_src.name}...")
        upload(r2, bucket, ranges_src, "derived/OIranges.csv")
    else:
        print("Warning: no viewer-compatible OIranges.csv found — skipping.")

    # ── manifest ───────────────────────────────────────────────────────────────
    manifest = {
        "dates":      sorted(dates),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    r2.put_object(
        Bucket=bucket,
        Key="manifest.json",
        Body=json.dumps(manifest, indent=2).encode(),
        ContentType="application/json",
    )
    print(f"\nmanifest.json written — {len(dates)} dates.")
    print("Bucket seeded successfully.")


if __name__ == "__main__":
    main()
