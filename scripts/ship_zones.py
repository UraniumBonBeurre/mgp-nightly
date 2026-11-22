#!/usr/bin/env python3
"""
scripts/ship_zones.py — Pays de livraison des boutiques (table merchant_ships)
=============================================================================

Une boutique peut livrer bien au-delà du marché de sa devise (22/11 : 80 des 178 boutiques Shopify livrent les
6 pays du site). catalog_sync.py s'en sert : boutique en euros affichée aussi sur les autres marchés en euros
qu'elle livre ; autres devises : page « bons plans livrés depuis l'étranger » (Worker).

1. Boutiques Shopify : liste exacte réglée par la boutique, lue sur {site}/meta.json (« ships_to_countries »).
2. Autres boutiques : pages de livraison lues à la main le 22/11 (MANUAL). Sans information : marché d'origine seul.
Une boutique Shopify injoignable une nuit garde sa ligne de la veille.

Usage : python3 scripts/ship_zones.py [--dry-run]
"""

import argparse
import json
import ssl
import sys
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from classify_rayons import sb  # noqa: E402  (charge aussi settings / .env.local)
from settings import AWIN_API_TOKEN, AWIN_PUBLISHER_ID  # noqa: E402

UA = "Mozilla/5.0 (compatible; MyGoodPick-ShippingCheck/1.0; +https://mygoodpick.net)"
EU_ALL = "FR DE IT ES"
# Pages de livraison lues le 22/11 (boutiques hors Shopify). Pays du site seulement (FR DE IT ES GB US).
MANUAL = {
    7735: "FR", 12665: "FR", 6901: "FR",                       # Darty, Fnac, Rue du Commerce : France seulement
    127695: EU_ALL,                # Omara Jewelry : 21 pays de l'UE, livraison gratuite
    15079: "DE FR IT",             # Blitec : Belgique, Pays-Bas, France, Italie, Autriche…
    129661: "DE IT ES",            # Luregod : « ausgewählte europäische Länder » (Italia, España)
    30693: EU_ALL,                 # 8wines DACH : livraison gratuite vers la France dès 200 €
    70855: EU_ALL + " GB",         # House of Rugby : grille de tarifs par pays (Europe)
    131715: EU_ALL,                # Scento : 26 pays européens
    59747: EU_ALL + " US",         # Gemax (saegeblatt-shop.de) : « Lieferland »
    120289: EU_ALL + " GB",        # SK-LED : zones 1 à 3
    13991: EU_ALL,                 # 100percentpure : liste des pays UE
    17412: EU_ALL + " GB",         # Profichemie : grille des pays
    56633: EU_ALL,                 # Bellerei : « alle EU-Länder, ausgenommen Malta und Zypern »
    17531: EU_ALL,                 # Monbento : Europe
    115393: EU_ALL + " GB US",     # ANITA & ZAHA
    112314: "ES FR",               # Nasopia : Espagne et France
    129031: EU_ALL + " GB US",     # LIKA : « worldwide »
    131739: EU_ALL + " GB US",     # eSIM For U : produit numérique
    91177: EU_ALL + " GB US",      # Silverbene : « We ship worldwide »
    102989: "DE", 17471: "DE", 69970: "DE", 125128: "DE", 106791: "DE", 77278: "DE", 108408: "DE", 127385: "DE",
    63006: "ES", 121176: "IT", 129537: "IT", 131023: "DE", 131019: "FR DE", 496: "GB",
}
ctx = ssl.create_default_context()


def programmes() -> list[dict]:
    req = urllib.request.Request(f"https://api.awin.com/publishers/{AWIN_PUBLISHER_ID}/programmes?relationship=joined",
                                 headers={"Authorization": f"Bearer {AWIN_API_TOKEN}"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read())


def shopify_countries(display_url: str) -> list[str] | None:
    url = display_url.strip()
    if not url:
        return None
    if not url.startswith("http"):
        url = "https://" + url
    base = "/".join(url.split("/")[:3])
    try:
        req = urllib.request.Request(base + "/meta.json", headers={"User-Agent": UA, "Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=20, context=ctx) as r:
            data = json.loads(r.read(300_000))
    except Exception:  # noqa: BLE001 — pas Shopify, ou injoignable : rien à dire
        return None
    c = data.get("ships_to_countries") if isinstance(data, dict) else None
    return sorted({x for x in c if isinstance(x, str)}) if isinstance(c, list) else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    progs = programmes()
    with ThreadPoolExecutor(max_workers=16) as pool:
        found = list(pool.map(lambda p: shopify_countries(p.get("displayUrl") or ""), progs))
    rows = []
    for p, countries in zip(progs, found):
        if countries:
            rows.append({"awin_mid": p["id"], "name": p["name"], "countries": countries, "source": "shopify"})
        elif p["id"] in MANUAL:
            rows.append({"awin_mid": p["id"], "name": p["name"], "countries": MANUAL[p["id"]].split(), "source": "manual"})
    site = {"FR", "DE", "IT", "ES", "GB", "US"}
    print(f"Pays de livraison : {len(progs)} programmes, {sum(r['source'] == 'shopify' for r in rows)} Shopify, "
          f"{sum(r['source'] == 'manual' for r in rows)} relevés à la main ; "
          f"{sum(1 for r in rows if site <= set(r['countries']) or '*' in r['countries'])} livrent les 6 pays du site")
    if args.dry_run or not rows:
        return
    now = datetime.now(timezone.utc).isoformat()
    for r in rows:
        r["checked_at"] = now
    for i in range(0, len(rows), 200):
        sb("POST", "merchant_ships?on_conflict=awin_mid", rows[i:i + 200])


if __name__ == "__main__":
    main()
