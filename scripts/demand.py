#!/usr/bin/env python3
"""
scripts/demand.py — Demande mesurée, chaque nuit avant le relevé (jobs/nightly.yml)
=================================================================================

Ce que les visiteurs, Google et les assistants IA cherchent vraiment, par marché : rayon, sous-rayon, marque,
et fiabilité de chaque marchand. Résultat dans dashboard_cache (« demand »), lu par catalog_sync.py pour choisir
et classer le catalogue (product_value) et affiché dans l'onglet « Pilotage » du tableau de bord.

Signaux des WINDOW_DAYS derniers jours, les plus récents comptant le plus (demi-vie HALF_LIFE_DAYS), pondérés
selon leur force (SIGNAL_WEIGHT) : recherches sur le site, pages vues (fiches et rayons), clics vers un marchand,
ventes, pages lues par un assistant IA pour quelqu'un, pages et recherches de Google (Search Console).

Chaque signal est comparé à ce qu'on attendrait sans préférence (part du rayon dans le catalogue × poids de départ
RAYON_DEMAND), puis lissé : « lift » = (observé + K) / (attendu + K). Avec peu de données, le lift reste proche de 1
et les poids de départ gardent la main ; plus le trafic grandit, plus la mesure décide. Les marchés s'appuient
d'abord sur la tendance de tous les marchés (lift global), puis sur leurs propres données.

Usage :
    python3 scripts/demand.py            # calcule et écrit
    python3 scripts/demand.py --dry-run  # affiche sans écrire
"""

import argparse
import json
import math
import re
import sys
import urllib.parse
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))
from classify_rayons import sb  # noqa: E402  (charge aussi settings / .env.local)
from catalog_sync import MARKET_WEIGHT, RAYON_DEMAND, brand_key  # noqa: E402  (poids de départ, partagés avec la sélection)

WINDOW_DAYS = 28
HALF_LIFE_DAYS = 7
SIGNAL_WEIGHT = {"search": 3.0, "rayon_view": 1.5, "product_view": 1.0, "outbound": 4.0, "sale": 15.0,
                 "ai_read": 1.5, "gsc_click": 2.0, "gsc_impression": 0.1, "gsc_query": 0.1}
K_RAYON, K_SOUS, K_BRAND = 40.0, 20.0, 40.0           # force du lissage (en « signaux équivalents » ; un visiteur en émet plusieurs)
CLIP_RAYON, CLIP_SOUS, CLIP_BRAND = (0.4, 3.0), (0.5, 2.0), (0.7, 2.0)
MIN_EVENTS_MARKET = 30                                 # en dessous, audience par marché fixe (MARKET_WEIGHT)
VIEW_MARKET = {"fr": "fr", "en": "fr", "uk": "uk", "us": "us", "de": "de", "it": "it", "es": "es"}
# Noms de gamme → marque (recherches « macbook », « galaxy s24 », « ps5 »…)
ALIASES = {
    "iphone": "apple", "ipad": "apple", "macbook": "apple", "imac": "apple", "airpods": "apple", "mac": "apple",
    "galaxy": "samsung", "pixel": "google", "playstation": "sony", "ps5": "sony", "ps4": "sony", "bravia": "sony",
    "xbox": "microsoft", "surface": "microsoft", "switch": "nintendo", "kindle": "amazon", "thinkpad": "lenovo",
    "legion": "lenovo", "ideapad": "lenovo", "vivobook": "asus", "zenbook": "asus", "rog": "asus", "omen": "hp",
    "pavilion": "hp", "alienware": "dell", "xps": "dell", "redmi": "xiaomi", "poco": "xiaomi", "dualsense": "sony",
}
_PRODUCT = re.compile(r"^/([a-z]{2})/prix/(?:.*-)?(\d{8,14}|x[0-9a-f]{13})/?$")
_RAYON = re.compile(r"^/([a-z]{2})/rayon/([a-z0-9-]+)(?:/([a-z0-9-]+))?/?$")


norm = brand_key          # recherches et marques normalisées comme dans catalog_sync (mêmes clés)


def decay(ts: str, now: datetime) -> float:
    age = (now - datetime.fromisoformat(ts.replace("Z", "+00:00"))).total_seconds() / 86400
    return 0.5 ** (max(0.0, age) / HALF_LIFE_DAYS)


def parse_path(path: str) -> tuple | None:
    """("product", marché, clé) ou ("rayon", marché, rayon, sous-rayon) ; adresse complète ou chemin."""
    p = urllib.parse.urlparse(path).path if path.startswith("http") else path.split("?")[0]
    if m := _PRODUCT.match(p):
        return ("product", VIEW_MARKET.get(m[1]), m[2]) if m[1] in VIEW_MARKET else None
    if m := _RAYON.match(p):
        return ("rayon", VIEW_MARKET.get(m[1]), m[2], m[3]) if m[1] in VIEW_MARKET else None
    return None


def query_brands(q: str, brands: set[str]) -> set[str]:
    """Marques citées dans une recherche : nom de marque en mots entiers, ou nom de gamme (ALIASES)."""
    words = norm(q).split()
    out = {ALIASES[w] for w in words if w in ALIASES}
    text = f" {' '.join(words)} "
    out |= {b for b in brands if len(b) >= 2 and f" {b} " in text}
    return out


def lift(observed: float, expected: float, k: float, prior: float = 1.0) -> float:
    return (observed + k * prior) / (expected + k)


def clip(v: float, bounds: tuple[float, float]) -> float:
    return max(bounds[0], min(bounds[1], v))


def compute(events: list[dict], counts: dict) -> dict:
    """events : {"w": poids déjà pondéré et amorti, "market" (None = tous marchés), "rayon", "sous", "brands"} ;
    counts : {"rayon": {m: {r: n}}, "sous": {m: {"r/s": n}}, "brand": {m: {b: n}}} (produits au catalogue).
    Renvoie les multiplicateurs par marché (rayon : poids complet ; sous-rayon et marque : autour de 1)."""
    E = defaultdict(lambda: defaultdict(float))            # E[m][r], m = "*" pour tous marchés
    ES = defaultdict(lambda: defaultdict(float))           # ES[m]["r/s"]
    EB = defaultdict(lambda: defaultdict(float))           # EB[m][marque]
    tot = defaultdict(float)
    for e in events:
        m = e.get("market")
        for mk in ({m, "*"} if m else {"*"}):
            tot[mk] += e["w"]
            if e.get("rayon"):
                E[mk][e["rayon"]] += e["w"]
                if e.get("sous"):
                    ES[mk][f"{e['rayon']}/{e['sous']}"] += e["w"]
            for b in e.get("brands") or ():
                EB[mk][b] += e["w"] / len(e["brands"])
    n_r = counts.get("rayon", {})
    glob_n = defaultdict(float)
    for m, d in n_r.items():
        for r, n in d.items():
            glob_n[r] += n

    def rayon_lifts(mk: str, n: dict, base: dict | None) -> dict:
        weighted = {r: RAYON_DEMAND.get(r, 0.5) * c for r, c in n.items() if c}
        total_w = sum(weighted.values()) or 1
        known = sum(E[mk].values())
        return {r: lift(E[mk].get(r, 0), known * weighted.get(r, 0) / total_w, K_RAYON, (base or {}).get(r, 1.0))
                for r in set(weighted) | set(E[mk])}
    glob = rayon_lifts("*", glob_n, None)
    out = {"rayon": {}, "sous": {}, "brand": {}, "lift": {"*": {r: round(v, 3) for r, v in glob.items()}}}
    nb_glob = defaultdict(float)
    for m, d in counts.get("brand", {}).items():
        for b, c in d.items():
            nb_glob[b] += c

    def brand_lifts(mk: str, nb: dict, base: dict | None) -> dict:
        total = sum(nb.values()) or 1
        known = sum(EB[mk].values())
        return {b: lift(EB[mk].get(b, 0), known * nb.get(b, 0) / total, K_BRAND, (base or {}).get(b, 1.0))
                for b in set(EB[mk]) | {b for b in (base or {})}}
    bglob = brand_lifts("*", nb_glob, None)
    for m in sorted(set(n_r) | {k for k in E if k != "*"}):
        lifts = rayon_lifts(m, n_r.get(m, {}), glob)
        out["lift"][m] = {r: round(v, 3) for r, v in lifts.items()}
        out["rayon"][m] = {r: round(RAYON_DEMAND.get(r, 0.5) * clip(v, CLIP_RAYON), 4) for r, v in lifts.items()}
        # Sous-rayons : autour du rayon (attendu = signaux du rayon × part du sous-rayon dans le rayon)
        ns = counts.get("sous", {}).get(m, {})
        sous = {}
        for key, c in ns.items():
            r = key.split("/")[0]
            in_r = sum(v for k2, v in ns.items() if k2.startswith(f"{r}/")) or 1
            known_r = sum(v for k2, v in ES[m].items() if k2.startswith(f"{r}/"))
            v = clip(lift(ES[m].get(key, 0), known_r * c / in_r, K_SOUS), CLIP_SOUS)
            if abs(v - 1) > 0.02:
                sous[key] = round(v, 3)
        out["sous"][m] = sous
        bl = brand_lifts(m, counts.get("brand", {}).get(m, {}), bglob)
        out["brand"][m] = {b: round(clip(v, CLIP_BRAND), 3) for b, v in bl.items() if abs(clip(v, CLIP_BRAND) - 1) > 0.02}
    # Audience par marché : part des signaux (racine : un petit marché garde sa chance)
    per_m = {m: v for m, v in tot.items() if m != "*"}
    if sum(per_m.values()) >= MIN_EVENTS_MARKET:
        top = max(per_m.values())
        out["market"] = {m: round(0.6 + 0.4 * math.sqrt(per_m.get(m, 0) / top), 3) for m in MARKET_WEIGHT}
    else:
        out["market"] = dict(MARKET_WEIGHT)
    out["signals"] = {m: round(v, 1) for m, v in tot.items()}
    return out


def merchant_quality(kpi: list[dict], clicks: dict, sales: dict) -> dict:
    """Fiabilité d'un marchand (autour de 1) : part des ventes validées (indicateurs Awin), taux de conversion
    comparé aux autres marchands, et ventes réelles par clic venues du site (lissées : 2 % attendus)."""
    conv = sorted(float(k["conversion_rate"]) for k in kpi if k.get("conversion_rate"))
    med = conv[len(conv) // 2] if conv else None
    out = {}
    for k in kpi:
        a = clip(float(k["approval_pct"]) / 85, (0.6, 1.1)) if k.get("approval_pct") else 1.0
        c = clip((float(k["conversion_rate"]) / med) ** 0.3, (0.7, 1.3)) if k.get("conversion_rate") and med else 1.0
        out[int(k["awin_mid"])] = a * c
    for mid in set(clicks) | set(sales):
        own = clip((sales.get(mid, 0) + 1) / (0.02 * clicks.get(mid, 0) + 1), (0.5, 2.0))
        out[mid] = out.get(mid, 1.0) * own
    return {str(m): round(v, 3) for m, v in out.items() if abs(v - 1) > 0.01}


def get_all(path: str, page: int = 1000) -> list[dict]:
    rows, off = [], 0
    while True:
        chunk = sb("GET", f"{path}&limit={page}&offset={off}") or []
        rows += chunk
        if len(chunk) < page:
            return rows
        off += page


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    now = datetime.now(timezone.utc)
    since = (now.timestamp() - WINDOW_DAYS * 86400)
    since_iso = datetime.fromtimestamp(since, timezone.utc).isoformat()
    since_day = since_iso[:10]
    q = urllib.parse.quote
    site = get_all(f"site_events?select=ts,type,market,path,ean,merchant_key,query&excluded=is.false&ts=gte.{q(since_iso)}&order=id")
    ai = get_all(f"ai_user_fetches?select=ts,path&ts=gte.{q(since_iso)}&order=id")
    gpages = get_all(f"gsc_page_daily?select=day,page,impressions,clicks&day=gte.{since_day}&order=day,page")
    gqueries = get_all(f"gsc_query_daily?select=day,query,impressions,clicks&day=gte.{since_day}&order=day,query")
    feeds = get_all("feeds?select=merchant_key,merchant_name,awin_mid,market&order=feed_id")
    conv = get_all(f"conversions?select=transaction_date,advertiser_id,status&transaction_date=gte.{q(since_iso)}&order=transaction_id")
    kpi = get_all("merchant_kpi?select=awin_mid,conversion_rate,approval_pct&order=awin_mid")
    cache = sb("GET", "dashboard_cache?select=value&key=eq.catalog_mix") or []
    mix = (cache[0]["value"] if cache else {}) or {}
    counts = {"rayon": defaultdict(dict), "sous": defaultdict(dict), "brand": mix.get("brand", {})}
    for r in get_all("rayon_stats?select=market,rayon,products&order=market,rayon"):
        counts["rayon"][r["market"]][r["rayon"]] = r["products"]
    for r in get_all("sous_rayon_stats?select=market,rayon,sous_rayon,products&order=market,rayon,sous_rayon"):
        counts["sous"][r["market"]][f"{r['rayon']}/{r['sous_rayon']}"] = r["products"]
    brands = {b for d in counts["brand"].values() for b in d}

    # Produits cités par les signaux : rayon, sous-rayon, marque, marchand
    wanted = defaultdict(set)
    for path in [e["path"] for e in site if e.get("path")] + [a["path"] for a in ai] + [g["page"] for g in gpages]:
        if (p := parse_path(path)) and p[0] == "product" and p[1]:
            wanted[p[1]].add(p[2])
    for e in site:
        if e["type"] == "outbound" and e.get("ean") and e.get("market"):
            wanted[VIEW_MARKET.get(e["market"], e["market"])].add(e["ean"])
    prod = {}
    for m, keys in wanted.items():
        keys = sorted(keys)
        for i in range(0, len(keys), 150):
            for r in sb("GET", f"products_ean?select=market,ean,rayon,sous_rayon,brand,best_merchant&market=eq.{m}"
                               f"&ean=in.({','.join(keys[i:i + 150])})") or []:
                prod[(m, r["ean"])] = r
    mid_by_name = {(f["market"], f["merchant_name"]): f["awin_mid"] for f in feeds}
    mid_by_key = {f["merchant_key"]: f["awin_mid"] for f in feeds}

    events, clicks, top_queries = [], defaultdict(float), defaultdict(float)
    intent_cache: dict[str, list] = {}

    def add(kind: str, ts: str | None, market: str | None, weight: float = 1.0, product=None, rayon=None, sous=None, brs=None):
        w = SIGNAL_WEIGHT[kind] * weight * (decay(ts, now) if ts else 1.0)
        if product:
            rayon, sous = product.get("rayon"), product.get("sous_rayon")
            brs = {norm(product["brand"])} if product.get("brand") else set()
        if w > 0:
            events.append({"w": w, "market": market, "rayon": rayon if rayon != "autres" else None, "sous": sous,
                           "brands": sorted(brs or ())})

    def intent(qn: str) -> list:
        if qn not in intent_cache:
            try:
                intent_cache[qn] = sb("POST", "rpc/search_intent_rayons", {"p_q": qn}) or []
            except Exception:  # noqa: BLE001 — sans rayon attendu, la recherche compte pour la marque seulement
                intent_cache[qn] = []
        return intent_cache[qn]

    def add_query(text: str, ts: str | None, market: str | None, kind: str, weight: float = 1.0):
        qn = norm(text)
        if len(qn) < 2:
            return
        rs = intent(qn)
        brs = query_brands(qn, brands)
        top_queries[qn] += SIGNAL_WEIGHT[kind] * weight
        for r in rs or [None]:
            add(kind, ts, market, weight / max(1, len(rs)), rayon=r, brs=brs)

    for e in site:
        m = VIEW_MARKET.get(e.get("market") or "", None)
        if e["type"] == "search" and e.get("query"):
            add_query(e["query"], e["ts"], m, "search")
        elif e["type"] == "outbound":
            p = prod.get((m, e.get("ean")))
            add("outbound", e["ts"], m, product=p) if p else None
            if (mid := mid_by_key.get(e.get("merchant_key") or "")):
                clicks[mid] += 1
        elif e["type"] == "pageview" and e.get("path") and (p := parse_path(e["path"])):
            if p[0] == "product" and (r := prod.get((p[1], p[2]))):
                add("product_view", e["ts"], p[1], product=r)
            elif p[0] == "rayon":
                add("rayon_view", e["ts"], p[1], rayon=p[2], sous=p[3])
    for a in ai:
        if (p := parse_path(a["path"])) and p[0] == "product" and (r := prod.get((p[1], p[2]))):
            add("ai_read", a["ts"], p[1], product=r)
    for g in gpages:
        if (p := parse_path(g["page"])):
            ts = f"{g['day']}T12:00:00+00:00"
            r = prod.get((p[1], p[2])) if p[0] == "product" else None
            kw = {"product": r} if r else ({"rayon": p[2], "sous": p[3]} if p[0] == "rayon" else None)
            if kw is not None:
                add("gsc_impression", ts, p[1], g["impressions"], **kw)
                add("gsc_click", ts, p[1], g["clicks"], **kw)
    for g in gqueries:                       # recherches Google : marché inconnu (tous marchés)
        add_query(g["query"], f"{g['day']}T12:00:00+00:00", None, "gsc_query", g["impressions"] + 10 * g["clicks"])
    sales = defaultdict(float)
    for c in conv:
        if c.get("status") != "declined" and c.get("advertiser_id"):
            sales[int(c["advertiser_id"])] += 1

    out = compute(events, counts)
    out["merchant"] = merchant_quality(kpi, clicks, sales)
    out["prior"] = dict(RAYON_DEMAND)
    names = {}
    for f in feeds:
        names.setdefault(str(f["awin_mid"]), f["merchant_name"])
    out["merchant_names"] = {mid: names.get(mid, mid) for mid in out["merchant"]}
    out["generated_at"] = now.isoformat()
    out["window_days"], out["half_life_days"] = WINDOW_DAYS, HALF_LIFE_DAYS
    out["queries"] = [[k, round(v, 1)] for k, v in sorted(top_queries.items(), key=lambda x: -x[1])[:40]]
    out["events"] = len(events)
    summary = {m: sorted(((r, w) for r, w in d.items()), key=lambda x: -x[1])[:6] for m, d in out["rayon"].items()}
    print(f"Demande : {len(events)} signaux ; marchés {out['market']} ; rayons en tête {json.dumps(summary, ensure_ascii=False)[:600]}")
    print(f"Marques : { {m: len(d) for m, d in out['brand'].items()} } ; marchands ajustés : {len(out['merchant'])}")
    if not args.dry_run:
        sb("POST", "dashboard_cache?on_conflict=key", [{"key": "demand", "value": out, "updated_at": now.isoformat()}])


if __name__ == "__main__":
    main()
