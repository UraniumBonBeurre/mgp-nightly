#!/usr/bin/env python3
"""
scripts/awin_promotions.py — Codes promo et promotions des programmes rejoints, chaque nuit
==========================================================================================

API Awin « promotions » (POST /publisher/{id}/promotions, 200 par page, 20 appels par minute au plus) :
promotions actives des programmes rejoints → table promotions ; celles qui ne sont plus actives (expirées,
retirées par l'annonceur, programme quitté) sont supprimées. Le site les affiche sous l'offre de la boutique
(fiches) et sur /{site}/codes-promo, avec un lien /go/promo/{id} (clic compté).

Usage : python3 scripts/awin_promotions.py
"""

import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from settings import AWIN_API_TOKEN, AWIN_PUBLISHER_ID, SUPABASE_KEY, SUPABASE_URL  # noqa: E402

PAGE = 200


def sb(method: str, path: str, body=None):
    req = urllib.request.Request(f"{SUPABASE_URL}/rest/v1/{path}", method=method,
                                 data=json.dumps(body).encode() if body is not None else None,
                                 headers={"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}",
                                          "Content-Type": "application/json",
                                          "Prefer": "resolution=merge-duplicates,return=minimal"})
    for attempt in range(4):                     # 19/11 : erreur 500 passagère juste après le classement (base chargée)
        try:
            with urllib.request.urlopen(req, timeout=120) as r:
                r.read()
            return
        except urllib.error.HTTPError as e:
            if e.code < 500 or attempt == 3:
                raise
            time.sleep(15 * (attempt + 1))


def fetch_all() -> list[dict]:
    out, page = [], 1
    while True:
        body = {"filters": {"membership": "joined", "status": "active"}, "pagination": {"page": page, "pageSize": PAGE}}
        req = urllib.request.Request(f"https://api.awin.com/publisher/{AWIN_PUBLISHER_ID}/promotions",
                                     data=json.dumps(body).encode(), method="POST",
                                     headers={"Authorization": f"Bearer {AWIN_API_TOKEN}", "Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=60) as r:
            d = json.loads(r.read())
        out += d.get("data") or []
        if page * PAGE >= (d.get("pagination") or {}).get("total", 0):
            return out
        page += 1
        time.sleep(3.5)                                   # sous la limite de 20 appels par minute


def to_row(p: dict, now: str) -> dict:
    regions = p.get("regions") or {}
    countries = None if regions.get("all") else sorted({r.get("countryCode") for r in regions.get("list") or [] if r.get("countryCode")}) or None
    code = ((p.get("voucher") or {}).get("code") or "").strip() or None
    return {
        "promotion_id": p["promotionId"], "awin_mid": p["advertiser"]["id"], "advertiser": p["advertiser"]["name"],
        "type": "voucher" if code else "promotion", "code": code, "title": (p.get("title") or "").strip()[:300],
        "terms": (p.get("terms") or "").strip()[:600] or None, "url": p.get("url") or None, "countries": countries,
        "starts_at": p.get("startDate"), "ends_at": p.get("endDate"), "updated_at": now,
    }


def main():
    if not (AWIN_API_TOKEN and AWIN_PUBLISHER_ID):
        sys.exit("AWIN_API_TOKEN et AWIN_PUBLISHER_ID requis")
    now = datetime.now(timezone.utc).isoformat()
    rows = [to_row(p, now) for p in fetch_all() if p.get("title")]
    for i in range(0, len(rows), 500):
        sb("POST", "promotions?on_conflict=promotion_id", rows[i:i + 500])
    # Plus actives (absentes de la réponse) ou expirées : supprimées.
    sb("DELETE", f"promotions?updated_at=lt.{urllib.parse.quote(now)}")
    sb("DELETE", f"promotions?ends_at=lt.{urllib.parse.quote(now)}")
    print(f"Promotions : {len(rows)} actives ({sum(1 for r in rows if r['code'])} codes) chez "
          f"{len({r['awin_mid'] for r in rows})} boutiques")


if __name__ == "__main__":
    main()
