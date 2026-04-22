"""
pipeline.py — Unified Payment Ingestion Pipeline
Run: python pipeline.py
"""
from __future__ import annotations
import csv, json, logging
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd
from schema import event_to_dict
from transformers import transform_cards, transform_transfers, transform_bills

RAW_DIR    = Path("data/raw")
OUTPUT_DIR = Path("data/output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

def run_pipeline():
    all_events, all_errors = [], []
    sources = [
        ("cards.csv",         transform_cards,     "Cards"),
        ("transfers.csv",     transform_transfers, "Transfers"),
        ("bill_payments.csv", transform_bills,     "Bill Payments"),
    ]
    for filename, fn, label in sources:
        path = RAW_DIR / filename
        if not path.exists():
            log.warning(f"[{label}] Not found: {path}")
            continue
        df = pd.read_csv(path)
        log.info(f"[{label}] Loaded {len(df)} rows")
        events, errors = fn(df)
        log.info(f"[{label}] OK={len(events)}  ERR={len(errors)}")
        all_events.extend(events)
        all_errors.extend(errors)

    if all_events:
        rows = [event_to_dict(e) for e in all_events]
        json_path = OUTPUT_DIR / "payment_events.json"
        with open(json_path, "w") as f:
            json.dump(rows, f, indent=2, default=str)
        log.info(f"Wrote {len(rows)} events -> {json_path}")

        # Write CSV too (easy to inspect)
        csv_path = OUTPUT_DIR / "payment_events.csv"
        df_out = pd.DataFrame(rows)
        df_out.to_csv(csv_path, index=False)
        log.info(f"Wrote CSV -> {csv_path}")

    if all_errors:
        err_path = OUTPUT_DIR / "errors.json"
        with open(err_path, "w") as f:
            json.dump(all_errors, f, indent=2, default=str)
        log.warning(f"{len(all_errors)} error(s) -> {err_path}")

    by_type = {}
    for e in all_events:
        by_type[e.payment_type] = by_type.get(e.payment_type, 0) + 1

    print("\n" + "="*50)
    print(f"  Pipeline complete — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("="*50)
    print(f"  Total events : {len(all_events)}")
    print(f"  Total errors : {len(all_errors)}")
    for t, n in by_type.items():
        print(f"    {t:<12}: {n}")
    print("="*50 + "\n")

if __name__ == "__main__":
    run_pipeline()
