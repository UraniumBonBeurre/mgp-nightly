#!/usr/bin/env python3
"""
scripts/awin_commissions.py — Taux de commission des boutiques, relus chaque nuit (23/11)
=========================================================================================

1. Groupes de commission de chaque programme rejoint (API Awin commissiongroups, 20 appels/min) → merchant_commissions.
2. Taux qui s'applique à chaque rayon du site, par boutique → merchant_rayon_rates. Une boutique avec un seul taux
   l'applique partout ; sinon Gemini Flash Lite choisit le groupe de chaque rayon (Darty « Téléphonie et Mobilité »
   pour smartphones…), seulement quand les groupes de la boutique ont changé.
3. Changements de taux d'une nuit à l'autre → commission_changes et message Telegram.

Lus par le Worker : commission estimée de chaque clic (tableau de bord, onglet Prévisions).

Usage : python3 scripts/awin_commissions.py [--no-gemini]
"""

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent))
from settings import AWIN_API_TOKEN, AWIN_PUBLISHER_ID  # noqa: E402
from classify_rayons import ask_gemini, sb  # noqa: E402

# Rayons du site (table rayons) : description donnée à Gemini pour choisir le groupe de commission
RAYONS = {
    "tv-video": "TV, video projectors, home cinema", "ordinateurs": "laptops and desktop computers",
    "composants-pc": "PC components (SSD, graphics cards, RAM)", "ecrans": "computer monitors",
    "peripheriques": "keyboards, mice, webcams, PC accessories", "impression": "printers and ink",
    "reseau-stockage": "networking, routers, external storage, NAS", "smartphones": "smartphones and mobile phones, tablets",
    "audio": "headphones, speakers, hi-fi", "photo": "cameras, lenses, drones", "gaming": "video game consoles and games",
    "objets-connectes": "smartwatches, connected devices, smart home", "gros-electromenager": "large appliances (washing machines, fridges)",
    "petit-electromenager": "small kitchen and household appliances", "beaute-sante": "beauty, personal care, health",
    "maison": "home, furniture, decoration, bedding, tableware", "jouets": "toys", "sport": "sports and outdoors",
    "bricolage-jardin": "DIY tools and garden", "bureau-papeterie": "office supplies and stationery", "logiciels": "software",
    "mode": "fashion, clothing, shoes", "animaux": "pet supplies", "autres": "other products",
}
# Groupes qui ne concernent pas une vente de produit (cartes de fidélité, abonnements, taux nul technique)
NOT_PRODUCT = ("CARTE", "RENOUVEL", "FIX", "NO_RATE", "ABONNEMENT", "SUBSCRIPTION", "NEWSLETTER", "LEAD")


def awin(path: str, state={"last": 0.0}):
    wait = 3.2 - (time.monotonic() - state["last"])
    if wait > 0:
        time.sleep(wait)
    state["last"] = time.monotonic()
    req = urllib.request.Request(f"https://api.awin.com{path}", headers={"Authorization": f"Bearer {AWIN_API_TOKEN}"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read() or b"{}")


def to_groups(mid: int, raw: dict) -> list[dict]:
    out = []
    for g in raw.get("commissionGroups") or []:
        kind = (g.get("type") or "").lower()
        out.append({"awin_mid": mid, "group_code": str(g.get("groupCode") or g.get("groupId")), "group_name": g.get("groupName"),
                    "kind": kind, "pct": g.get("percentage") if kind == "percentage" else None,
                    "amount": g.get("amount") if kind == "fix" else None, "currency": g.get("currency")})
    return out


def label(g: dict) -> str:
    return f"{g['pct']:g} %" if g["kind"] == "percentage" and g["pct"] is not None else f"{g['amount'] or 0:g} {g['currency'] or ''}".strip()


def product_groups(groups: list[dict]) -> list[dict]:
    """Groupes applicables à un produit : en pourcentage, hors cartes de fidélité et taux techniques."""
    return [g for g in groups if g["kind"] == "percentage" and (g["pct"] or 0) > 0
            and not any(w in (g["group_code"] + " " + (g["group_name"] or "")).upper() for w in NOT_PRODUCT)]


def signature(groups: list[dict]) -> str:
    return "|".join(sorted(f"{g['group_code']}={label(g)}" for g in product_groups(groups)))


def default_group(groups: list[dict]) -> dict | None:
    pg = product_groups(groups)
    return next((g for g in pg if g["group_code"].upper() == "DEFAULT"), None) or (min(pg, key=lambda g: g["pct"]) if pg else None)


def changes(old: list[dict], new: list[dict]) -> list[tuple[str, str, str]]:
    """(groupe, ancien taux, nouveau taux) pour les groupes de produits modifiés, ajoutés ou retirés."""
    o = {g["group_code"]: g for g in product_groups(old)}
    n = {g["group_code"]: g for g in product_groups(new)}
    out = []
    for code in sorted(set(o) | set(n)):
        a, b = o.get(code), n.get(code)
        if a and b and label(a) == label(b):
            continue
        out.append(((b or a)["group_name"] or code, label(a) if a else "—", label(b) if b else "retiré"))
    return out


def map_rayons(mid: int, name: str, groups: list[dict], use_gemini: bool) -> tuple[dict[str, dict], bool]:
    """Rayon du site → groupe de commission de la boutique ; second élément : correspondance définitive (un seul taux,
    ou choisie par Gemini). Sans Gemini, taux par défaut partout, refait au relevé suivant."""
    pg = product_groups(groups)
    if not pg:
        return {}, True
    d = default_group(groups)
    if len({label(g) for g in pg}) == 1:
        return {r: d for r in RAYONS}, True
    if not use_gemini:
        return {r: d for r in RAYONS}, False
    codes = [g["group_code"] for g in pg]
    listing = "\n".join(f"{g['group_code']}\t{g['group_name']}" for g in pg)
    items = "\n".join(f"{i}\t{desc}" for i, desc in enumerate(RAYONS.values()))
    prompt = (f"The online shop {name} pays different affiliate commission rates per product category. Its commission "
              f"groups (code<TAB>name):\n{listing}\n\nFor each product department below (id<TAB>description), choose the "
              f"code of the commission group that applies to such products sold by this shop. If none fits, choose the "
              f"default/other group.\n{items}")
    picked = ask_gemini(prompt, codes)
    by_code = {g["group_code"]: g for g in pg}
    return {r: by_code.get(picked.get(i), d) for i, r in enumerate(RAYONS)}, True


def telegram(text: str):
    tok, chat = os.environ.get("TELEGRAM_BOT_TOKEN"), os.environ.get("TELEGRAM_CHAT_ID")
    if not (tok and chat):
        return
    data = urllib.parse.urlencode({"chat_id": chat, "text": text}).encode()
    try:
        urllib.request.urlopen(f"https://api.telegram.org/bot{tok}/sendMessage", data=data, timeout=30)
    except urllib.error.URLError:
        pass


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--no-gemini", action="store_true")
    args = ap.parse_args()
    use_gemini = not args.no_gemini and bool(os.environ.get("GOOGLE_AI_API_KEY"))
    feeds = sb("GET", "feeds?select=awin_mid,merchant_name") or []
    names = {}
    for f in feeds:
        names.setdefault(f["awin_mid"], f["merchant_name"])
    old_rows = sb("GET", "merchant_commissions?select=*&limit=10000") or []
    first_run = not old_rows
    old = {}
    for g in old_rows:
        old.setdefault(g["awin_mid"], []).append(g)
    sigs = {(r["awin_mid"], r["rayon"]): r["signature"] for r in sb("GET", "merchant_rayon_rates?select=awin_mid,rayon,signature&limit=20000") or []}
    alerts, n_groups, n_mapped, failed = [], 0, 0, 0
    for mid in sorted(names):
        try:
            groups = to_groups(mid, awin(f"/publishers/{AWIN_PUBLISHER_ID}/commissiongroups?advertiserId={mid}"))
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as e:
            failed += 1
            print(f"  {names[mid]} : lecture impossible ({str(e)[:60]})")
            continue
        if not groups:
            continue
        n_groups += len(groups)
        diff = [] if first_run or mid not in old else changes(old[mid], groups)
        for grp, a, b in diff:
            sb("POST", "commission_changes", {"awin_mid": mid, "merchant": names[mid], "group_name": grp, "old_value": a, "new_value": b})
            alerts.append(f"• {names[mid]} — {grp} : {a} → {b}")
        sb("DELETE", f"merchant_commissions?awin_mid=eq.{mid}")
        sb("POST", "merchant_commissions", groups)
        sig = signature(groups)
        if all(sigs.get((mid, r)) == sig for r in RAYONS):
            continue                                          # mêmes groupes : correspondance des rayons inchangée
        try:
            mapping, exact = map_rayons(mid, names[mid], groups, use_gemini)
        except RuntimeError as e:                             # quota Gemini : taux par défaut, Gemini au relevé suivant
            print(f"  Gemini indisponible ({str(e)[:80]}) : taux par défaut pour les rayons des boutiques suivantes")
            use_gemini = False
            mapping, exact = map_rayons(mid, names[mid], groups, use_gemini)
        rows = [{"awin_mid": mid, "rayon": r, "group_code": g["group_code"] if g else None, "pct": g["pct"] if g else None,
                 "signature": sig if exact else ""} for r, g in mapping.items()]
        if rows:
            sb("POST", "merchant_rayon_rates?on_conflict=awin_mid,rayon", rows)
            n_mapped += 1
    print(f"Commissions : {n_groups} groupes pour {len(names) - failed} boutiques ({failed} illisibles), "
          f"rayons associés pour {n_mapped} boutiques, {len(alerts)} changements de taux")
    if alerts:
        telegram("💱 MyGoodPick : taux de commission modifiés cette nuit\n\n" + "\n".join(alerts[:30])
                 + (f"\n… et {len(alerts) - 30} autres" if len(alerts) > 30 else ""))
    if failed > len(names) / 2:
        sys.exit(f"API Awin : {failed} boutiques sur {len(names)} illisibles")


if __name__ == "__main__":
    main()
