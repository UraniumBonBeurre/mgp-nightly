#!/usr/bin/env python3
"""
scripts/awin_sync.py — Import quotidien des ventes et des clics Awin (mesure, docs/PLAN-REORIENTATION.md §0.4)
=============================================================================================================

1. Transactions des 35 derniers jours (deux fenêtres de 31 jours max) → table conversions.
   clickRef "c{id}" → click_event_id (clic enregistré par /go/{offerId}).
2. Clics comptés par Awin, jour par jour, sur les 3 derniers jours → awin_clicks_daily (recoupement).

Bibliothèque standard uniquement. Limite Awin : 20 appels/min → pause de 3,2 s entre appels.

Usage :
    python3 scripts/awin_sync.py
"""

import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from settings import AWIN_API_TOKEN, AWIN_PUBLISHER_ID, SUPABASE_KEY, SUPABASE_URL  # noqa: E402

REGIONS = ("FR", "GB", "US")
_last = 0.0


def awin(path: str):
    global _last
    wait = 3.2 - (time.monotonic() - _last)
    if wait > 0:
        time.sleep(wait)
    _last = time.monotonic()
    req = urllib.request.Request(f"https://api.awin.com{path}", headers={"Authorization": f"Bearer {AWIN_API_TOKEN}"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read() or b"[]")


def upsert(table: str, rows: list[dict], conflict: str):
    if not rows:
        return
    req = urllib.request.Request(
        f"{SUPABASE_URL}/rest/v1/{table}?on_conflict={conflict}",
        data=json.dumps(rows, default=str).encode(),
        method="POST",
        headers={"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}", "Content-Type": "application/json",
                 "Prefer": "resolution=merge-duplicates,return=minimal"},
    )
    with urllib.request.urlopen(req, timeout=60):
        pass


def click_event_id(ref: str | None) -> int | None:
    m = re.fullmatch(r"c(\d{1,15})", (ref or "").strip())
    return int(m.group(1)) if m else None


def sync_transactions(today: date) -> int:
    rows = []
    for start, end in ((today - timedelta(days=35), today - timedelta(days=5)), (today - timedelta(days=4), today)):
        txs = awin(f"/publishers/{AWIN_PUBLISHER_ID}/transactions/?startDate={start}T00:00:00"
                   f"&endDate={end}T23:59:59&timezone=UTC&dateType=transaction")
        for t in txs or []:
            ref = (t.get("clickRefs") or {}).get("clickRef") or t.get("clickRef")
            rows.append({
                "transaction_id": t["id"],
                "transaction_date": t.get("transactionDate"),
                "advertiser_id": t.get("advertiserId"),
                "advertiser_name": t.get("advertiserName"),
                "sale_amount": (t.get("saleAmount") or {}).get("amount"),
                "commission": (t.get("commissionAmount") or {}).get("amount"),
                "currency": (t.get("commissionAmount") or {}).get("currency"),
                "status": t.get("commissionStatus"),
                "click_ref": ref,
                "click_event_id": click_event_id(ref),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            })
    upsert("conversions", rows, "transaction_id")
    return len(rows)


def sync_clicks(today: date) -> int:
    rows = {}
    for back in range(3):
        d = today - timedelta(days=back)
        for region in REGIONS:
            for r in awin(f"/publishers/{AWIN_PUBLISHER_ID}/reports/advertiser?startDate={d}&endDate={d}"
                          f"&region={region}&timezone=UTC") or []:
                key = (str(d), r.get("advertiserId"))
                row = rows.setdefault(key, {"day": str(d), "advertiser_id": r.get("advertiserId"),
                                            "advertiser_name": r.get("advertiserName"), "clicks": 0})
                row["clicks"] += r.get("clicks") or 0
    upsert("awin_clicks_daily", list(rows.values()), "day,advertiser_id")
    return sum(r["clicks"] for r in rows.values())


def main():
    if not (AWIN_API_TOKEN and AWIN_PUBLISHER_ID and SUPABASE_URL and SUPABASE_KEY):
        sys.exit("AWIN_API_TOKEN, AWIN_PUBLISHER_ID, NEXT_PUBLIC_SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY requis")
    today = datetime.now(timezone.utc).date()
    try:
        n_tx = sync_transactions(today)
        n_clicks = sync_clicks(today)
    except urllib.error.HTTPError as e:
        sys.exit(f"Awin/Supabase HTTP {e.code} : {e.read().decode()[:300]}")
    print(f"Awin : {n_tx} transactions (35 j) importées, {n_clicks} clics comptés par Awin (3 j)")


if __name__ == "__main__":
    main()
