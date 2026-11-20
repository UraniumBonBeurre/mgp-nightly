#!/usr/bin/env python3
"""
catalog_sync.py — Relevé quotidien, stockage hybride Supabase + R2 (docs/PLAN-REORIENTATION.md §0.4 sexies)
==========================================================================================================

1. Pour chaque flux du périmètre (track_prices.build_perimeter) : télécharge le flux, le compare à son
   état précédent (R2 catalog/state/{feed}.json.gz), met à jour prix, prix barré, stock et historique.
2. Regroupe toutes les offres actives par produit (marché + EAN ou clé synthétique).
3. Réécrit dans R2 les fragments de fiches touchés (catalog/shards/{market}/{xxx}.json, gzip).
4. Met à jour dans Supabase l'index de navigation products_ean, pour les seuls produits modifiés.

Usage :
    .venv/bin/python catalog_sync.py                 # relevé complet
    .venv/bin/python catalog_sync.py --bootstrap     # 1re fois : historique repris de price_points
    .venv/bin/python catalog_sync.py --dry-run
"""

import argparse
import gzip
import hashlib
import json
import re
import sys
import time
import unicodedata
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import boto3
from botocore.config import Config

import track_prices as tp
from settings import R2_ACCESS_KEY_ID, R2_ACCOUNT_ID, R2_BUCKET_NAME, R2_SECRET_ACCESS_KEY

SHARD_HEX = 3                     # 16³ = 4 096 fragments par marché
HISTORY_MAX = 120                 # changements de prix gardés par offre
STALE_DAYS = 45                   # flux plus ancien : pas de bon plan
PERPETUAL_SHARE = 0.8             # boutique « toujours en promo » : pas de bon plan
PREFIX = "catalog"
# Au-delà, plus aucun nouveau produit dans l'index (limite gratuite : 500 Mo). Arrêt d'urgence seulement : la
# taille est tenue par CATALOG_MAX, et la place des produits retirés est réutilisée sans que le fichier rétrécisse
# (440 bloquait tout ajout dès 446 Mo, même après 143 000 retraits).
DB_GUARD_MB = 475
# Nouveaux produits ajoutés à l'index par relevé (comparables d'abord). Les ajouts massifs (100 000 lignes)
# ont saturé l'instance gratuite trois fois le 16/11 ; le reste entre aux relevés suivants (un produit absent
# de l'index est toujours repris).
NEW_ROWS_MAX = 15_000
CHANGED_ROWS_MAX = 40_000         # lignes existantes modifiées par relevé ; le reste au relevé suivant (rien n'est perdu :
                                  # l'index est comparé à chaque relevé, indépendamment de l'état des flux)
# Taille du catalogue (18/11, base pleine à 446 Mo pour 276 000 produits, ≈ 1,55 Ko chacun) : nombre de produits
# fixe, quel que soit le nombre de programmes rejoints. Les comparables d'abord, puis le reste partagé à parts
# égales entre les boutiques (select_catalog) : chaque nouveau programme réduit un peu la part des plus grosses.
CATALOG_MAX = 240_000
COMPARABLE_MAX = 150_000
SHOP_MAX = 10_000                 # par boutique et par marché, comparables compris (≈ 4 % du catalogue)

s3 = boto3.client(
    "s3", endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
    aws_access_key_id=R2_ACCESS_KEY_ID, aws_secret_access_key=R2_SECRET_ACCESS_KEY,
    region_name="auto", config=Config(retries={"max_attempts": 5}),
)


# ── Fonctions pures (testées dans tests/test_catalog_sync.py) ────────────────

def shard_of(key: str) -> str:
    return hashlib.sha1(key.encode()).hexdigest()[:SHARD_HEX]


def num(d) -> float | None:
    return None if d is None else float(d)


def update_state(state: dict, incoming: dict, today: str) -> tuple[dict, set]:
    """Applique le flux du jour à l'état précédent du flux.

    state    : aw_id → {"k", "p", "o", "s", "c", "h": [[date, prix], …], "x": date de retrait ou None}
    incoming : aw_id → offre (track_prices.row_to_offer)
    Retourne (nouvel état, clés produit touchées).
    """
    touched, new = set(), {}
    for aw_id, o in incoming.items():
        p, op = num(o["price"]), num(o["old_price"])
        cur = state.get(aw_id)
        hist = list(cur["h"]) if cur else []
        if not hist or hist[-1][1] != p:
            hist.append([today, p])
        entry = {"k": o["ean"], "p": p, "o": op, "s": o["in_stock"], "c": o["condition"], "h": hist[-HISTORY_MAX:], "x": None}
        if cur is None or (cur["p"], cur["o"], cur["s"], cur["c"], cur["k"], cur.get("x")) != (p, op, o["in_stock"], o["condition"], o["ean"], None):
            touched.add(o["ean"])
            if cur and cur["k"] != o["ean"]:
                touched.add(cur["k"])
        new[aw_id] = entry
    for aw_id, cur in state.items():
        if aw_id not in incoming:
            if cur.get("x") is None:
                touched.add(cur["k"])
            new[aw_id] = {**cur, "s": False, "x": cur.get("x") or today}
    # Oubli des offres retirées depuis plus de 30 jours (l'état ne grossit pas sans fin).
    limit = (datetime.fromisoformat(today) - timedelta(days=30)).date().isoformat()
    new = {k: v for k, v in new.items() if not v["x"] or v["x"] >= limit}
    return new, touched


# Colonnes comparées à l'index existant : une ligne n'est renvoyée que si l'une d'elles a changé.
SIG_NUM = ("min_price", "max_price", "old_price", "refurbished_min_price", "prev_price")
SIG_COLS = ("title", "brand", "category", "image_url", "offers_count", "merchants_count", "best_merchant", "deal_ok",
            "price_changed_at", "drop_pct", "lowest_since") + SIG_NUM


def row_sig(r: dict) -> int:
    """Empreinte d'une ligne d'index (prix arrondis au centime : 35.9, « 35.90 » et 35.90 sont égaux)."""
    return hash(tuple(round(float(r[c]), 2) if c in SIG_NUM and r.get(c) is not None else r.get(c) for c in SIG_COLS))


def fair_cap(sizes: list[int], room: int) -> int:
    """Plus grand plafond c tel que sum(min(n, c)) <= room : les petites boutiques gardent tout, les grosses
    sont ramenées au même plafond."""
    lo, hi = 0, max(sizes, default=0)
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if sum(min(n, mid) for n in sizes) <= room:
            lo = mid
        else:
            hi = mid - 1
    return lo


# Variantes d'un même article dans une boutique (18/11 : teintes « Shade 1/2/4/5 », couleurs, tailles).
_COLOURS = set("""black white red blue green yellow orange pink purple violet grey gray brown beige navy gold silver
rose copper bronze cream ivory khaki olive teal turquoise burgundy maroon multicolor multicolour natural clear
transparent dark light noir blanc rouge bleu vert jaune gris marron brun dore argent cuivre bordeaux kaki creme
ivoire fonce clair schwarz weiss rot blau gruen grun gelb rosa lila violett grau braun silber kupfer dunkel hell
anthrazit anthracite""".split())
_SIZES = {"xxs", "xs", "s", "m", "l", "xl", "xxl", "xxxl", "2xl", "3xl", "4xl", "5xl"}
# Mot de variante : le nombre qui le suit (« Shade 4 », « Taille 38 ») est aussi ignoré.
_VARIANT_WORDS = {"shade", "teinte", "farbe", "colour", "color", "couleur", "size", "taille", "grosse", "groesse",
                  "gr", "nr", "no", "ton", "tone", "variante", "variant", "eu", "uk", "us"}


def variant_key(title: str) -> str:
    """Titre sans couleur, taille ni numéro de teinte : deux variantes d'un article ont la même clé, deux modèles
    (« iPhone 14 » / « iPhone 15 », « 1 TB » / « 2 TB ») restent distincts."""
    t = unicodedata.normalize("NFKD", (title or "").lower().replace("ß", "ss"))
    toks = re.sub(r"[^a-z0-9]+", " ", "".join(c for c in t if not unicodedata.combining(c))).split()
    out, skip_num = [], False
    for tok in toks:
        if tok in _VARIANT_WORDS:
            skip_num = True
            continue
        if (skip_num and tok.isdigit()) or tok in _COLOURS or tok in _SIZES:
            continue
        skip_num = False
        out.append(tok)
    return " ".join(out)


def select_catalog(products: dict, owner: dict, existing, budget: int = CATALOG_MAX,
                   comparable_max: int = COMPARABLE_MAX, shop_max: int = SHOP_MAX) -> tuple[set, int]:
    """Produits gardés dans le catalogue : (marché, clé), et le plafond par boutique appliqué.

    products : marché → clé → produit (build_product) ; owner : (marché, clé) → boutique affichée (la moins
    chère) ; existing : produits déjà dans l'index (gardés à valeur égale : pas de va-et-vient).
    Aucune boutique ne dépasse shop_max produits, comparables compris (PCHUB : 29 000 comparables le 18/11).
    0. Un seul produit par titre et par marché : les variantes (couleurs, tailles) publiées avec des EAN
       distincts apparaissaient en dizaines de fiches identiques (Kuishi, 18/11 ; 34 000 lignes en double).
       La variante gardée est la plus comparée, puis la mieux classée.
    1. Comparables (≥ 2 boutiques), les plus comparés d'abord, jusqu'à comparable_max.
    2. Le reste du budget partagé à parts égales entre boutiques (fair_cap), chacune gardant ses meilleurs
       produits : photo, bon plan ou baisse, déjà présent, prix ≥ PRICE_FLOOR, puis tirage stable."""
    def rank(mk):
        r = products[mk[0]][mk[1]]["row"]
        return (not r.get("image_url"), not (r.get("deal_ok") or r.get("drop_pct")), mk not in existing,
                (r.get("min_price") or 0) < tp.PRICE_FLOOR, hashlib.sha1(f"{mk[0]}:{mk[1]}".encode()).hexdigest())

    def order(mk):
        return (-(products[mk[0]][mk[1]]["row"].get("merchants_count") or 0), rank(mk))
    best: dict[tuple, tuple] = {}
    for m, prods in products.items():
        for k, p in prods.items():
            t = (m, " ".join(re.sub(r"[^a-z0-9]+", " ", (p["row"].get("title") or "").lower()).split()) or k)
            if t not in best or order((m, k)) < order(best[t]):
                best[t] = (m, k)
    comparables, groups = [], defaultdict(list)
    seen_variant, seen_photo = set(), set()
    for m, k in sorted(best.values(), key=order):
        row = products[m][k]["row"]
        if (row.get("merchants_count") or 0) >= 2:
            comparables.append((m, k))
            continue
        # Produits d'une seule boutique : une fiche par variante (couleur, taille, teinte) et par photo.
        shop = owner[(m, k)]
        vk, photo = (m, shop, variant_key(row.get("title")) or k), (m, shop, row.get("image_url") or k)
        if vk in seen_variant or photo in seen_photo:
            continue
        seen_variant.add(vk)
        seen_photo.add(photo)
        groups[(m, shop)].append((m, k))
    comparables.sort(key=order)
    kept, per_shop = set(), defaultdict(int)
    for m, k in comparables:
        if len(kept) >= comparable_max:
            break
        shop = (m, owner.get((m, k)))
        if per_shop[shop] < shop_max:
            kept.add((m, k))
            per_shop[shop] += 1
    room = {s: max(0, shop_max - per_shop[s]) for s in groups}
    cap = fair_cap([min(len(g), room[s]) for s, g in groups.items()], budget - len(kept))
    for s, g in groups.items():
        kept.update(sorted(g, key=rank)[:min(cap, room[s])])
    return kept, cap


OUTLIER_RATIO = 3
GUARD_DAYS = 3                   # flux effondré : offres de la veille gardées au plus 3 jours, puis réduction acceptée
DROP_MIN_PCT, DROP_MAX_PCT = 5, 70    # baisse constatée affichée : au-delà de 70 %, erreur de flux probable


def price_trend(price: float, history: list) -> dict:
    """Évolution constatée du prix d'une offre, d'après nos relevés (et non le prix barré de la boutique).

    history : [[date, prix], …] (un point par changement). Retourne les champs de l'index :
    prev_price / price_changed_at / drop_pct (dernière baisse constatée) et lowest_since (date du début du
    suivi si le prix actuel est le plus bas jamais relevé, après au moins un changement)."""
    pts: list[tuple[str, float]] = []
    for d, p in history or []:
        if p is not None and (not pts or abs(float(p) - pts[-1][1]) >= 0.005):
            pts.append((d, float(p)))
    out = {"prev_price": None, "price_changed_at": None, "drop_pct": None, "lowest_since": None}
    if len(pts) < 2 or abs(pts[-1][1] - price) >= 0.005:
        return out
    prev = pts[-2][1]
    out["price_changed_at"] = pts[-1][0]
    if prev > price > 0:
        pct = round(100 * (1 - price / prev))
        if DROP_MIN_PCT <= pct <= DROP_MAX_PCT:
            out["prev_price"], out["drop_pct"] = prev, pct
    if price <= min(p for _, p in pts):
        out["lowest_since"] = pts[0][0]
    return out


def build_product(key: str, offers: list[dict], today: str) -> dict | None:
    """Agrège les offres actives d'un produit : fiche (R2) et ligne d'index (Supabase)."""
    active = [o for o in offers if o["s"] and not o["x"]]
    # Même boutique, même état, deux flux (gammes qui se recoupent) : une seule offre, la moins chère.
    active = list({(o["m"], o["c"]): o for o in sorted(active, key=lambda o: o["p"], reverse=True)}.values())
    new = sorted((o for o in active if o["c"] == "new"), key=lambda o: (o["p"], o["m"]))
    if not new:
        return None
    # Offre aberrante (plus de OUTLIER_RATIO fois la moins chère) : en général un code EAN partagé par deux
    # produits différents ; elle fausserait l'écart affiché (« économisez 100 000 $ »).
    if len(new) > 1:
        cap = new[0]["p"] * OUTLIER_RATIO
        new = [o for o in new if o["p"] <= cap]
        active = [o for o in active if o["c"] != "new" or o["p"] <= cap]
    best = new[0]
    fresh = best["f"] is None or best["f"] >= (datetime.fromisoformat(today) - timedelta(days=STALE_DAYS)).date().isoformat()
    refurb = [o["p"] for o in active if o["c"] == "refurbished"]
    disc = round(100 * (1 - best["p"] / best["o"])) if best["o"] and best["o"] > best["p"] else None
    row = {
        "ean": key, "title": best["t"], "brand": best["b"], "category": best["cat"], "image_url": best["i"],
        "offers_count": len(new), "merchants_count": len({o["m"] for o in new}),
        "min_price": best["p"], "max_price": max(o["p"] for o in new), "old_price": best["o"] if disc else None,
        "refurbished_min_price": min(refurb) if refurb else None, "best_merchant": best["n"],
        "deal_ok": bool(disc and fresh and not best.get("perpetual")),
        **price_trend(best["p"], best.get("h")),
    }
    doc = {
        "k": key, "t": best["t"], "b": best["b"], "i": best["i"], "cat": best["cat"],
        "d": next((o["d"] for o in sorted(active, key=lambda o: o["p"]) if o.get("d")), None),
        "o": [{"id": f"{o['m']}-{o['c'][0]}", "m": o["m"], "n": o["n"], "a": o["a"], "p": o["p"], "op": o["o"],
               "c": o["c"], "u": o["u"], "f": o["f"], "h": o["h"]} for o in sorted(active, key=lambda o: o["p"])],
    }
    return {"row": row, "doc": doc}


def offers_from_doc(doc: dict, merchant_key: str, condition: str) -> list[dict]:
    """Offres d'une boutique reprises d'une fiche R2 (flux en échec : on garde celles de la veille)."""
    # Mêmes filtres qu'à l'import (track_prices.row_to_offer) : les fiches anciennes peuvent contenir des prix
    # « bidon » et des titres encodés (offres de PCHUB reprises pendant la protection d'un flux effondré).
    return [{"m": o["m"], "n": o["n"], "a": o["a"], "p": o["p"], "o": o["op"], "s": True, "c": o["c"], "h": o["h"],
             "x": None, "u": o["u"], "f": o["f"], "t": tp.unescape_text(doc["t"] or ""),
             "b": tp.unescape_text(doc["b"]) if doc.get("b") else doc.get("b"), "i": doc["i"], "cat": doc["cat"],
             "d": doc.get("d")}
            for o in doc["o"] if o["m"] == merchant_key and o["c"] == condition and o["p"] < float(tp.PRICE_MAX)]


def carry_over(failed: list, offers_by_key: dict, existing: dict) -> int:
    """Flux en échec (réseau, Awin) : leurs offres de la veille sont reprises des fragments R2, pour qu'une
    panne passagère ne retire ni les produits de l'index ni leurs offres des fragments réécrits."""
    from concurrent.futures import ThreadPoolExecutor
    markets_of = defaultdict(list)
    for market, key in existing:
        markets_of[key].append(market)
    wanted = defaultdict(list)              # (market, shard) → [(key, feed)]
    for feed in failed:
        for v in (r2_get_json(f"{PREFIX}/state/{feed.feed_id}.json.gz") or {}).values():
            if not v.get("x"):
                for market in markets_of.get(v["k"], []):
                    wanted[(market, shard_of(v["k"]))].append((v["k"], feed))
    with ThreadPoolExecutor(max_workers=16) as pool:
        shards = dict(zip(wanted, pool.map(lambda ms: r2_get_json(f"{PREFIX}/shards/{ms[0]}/{ms[1]}.json.gz") or {},
                                           wanted)))
    n = 0
    for (market, sid), items in wanted.items():
        for key, feed in items:
            doc = shards[(market, sid)].get(key)
            have = {(o["m"], o["c"]) for o in offers_by_key.get((market, key), [])}
            if doc and (feed.merchant_key, feed.condition) not in have:
                offs = offers_from_doc(doc, feed.merchant_key, feed.condition)
                offers_by_key[(market, key)].extend(offs)
                n += len(offs)
    return n


def slugify_title(title: str) -> str:
    import unicodedata
    s = unicodedata.normalize("NFKD", title).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]+", "-", s).strip("-")[:80]


def compile_rules(rules: list[dict]):
    """Motifs PostgreSQL (\\m \\M = début/fin de mot) → Python."""
    out = []
    for r in sorted(rules, key=lambda r: r["priority"]):
        pat = r["pattern"].replace(r"\m", r"\b").replace(r"\M", r"\b")
        out.append((re.compile(pat), r["rayon"]))
    return out


def rayon_for(text: str | None, rules) -> str | None:
    import unicodedata
    t = unicodedata.normalize("NFKD", text or "").encode("ascii", "ignore").decode().lower()
    for rx, rayon in rules:
        if rx.search(t):
            return rayon
    return None


# ── E/S R2 ──────────────────────────────────────────────────────────────────

def r2_get_json(key: str):
    try:
        obj = s3.get_object(Bucket=R2_BUCKET_NAME, Key=key)
    except s3.exceptions.NoSuchKey:
        return None
    raw = obj["Body"].read()
    try:
        raw = gzip.decompress(raw)
    except OSError:
        pass
    return json.loads(raw)


def r2_put_json(key: str, data, cache_seconds: int = 3600):
    body = gzip.compress(json.dumps(data, separators=(",", ":"), ensure_ascii=False).encode(), compresslevel=6)
    s3.put_object(Bucket=R2_BUCKET_NAME, Key=key, Body=body, ContentType="application/json",
                  ContentEncoding="gzip", CacheControl=f"public, max-age={cache_seconds}")


# ── Programme ───────────────────────────────────────────────────────────────

def bootstrap_history() -> dict[tuple[int, str], list]:
    """1re exécution : historique repris de la table price_points, (feed_id, aw_id) → [[date, prix]]."""
    offers, last = {}, 0
    while True:
        rows = tp._sb("GET", f"offers?select=id,feed_id,aw_product_id&id=gt.{last}&order=id&limit=1000")
        for r in rows:
            offers[r["id"]] = (r["feed_id"], r["aw_product_id"])
        if len(rows) < 1000:
            break
        last = rows[-1]["id"]
    hist, top = defaultdict(list), max(offers, default=0)
    for lo in range(0, top + 1, 400):                      # tranches d'identifiants d'offres
        offset = 0
        while True:
            rows = tp._sb("GET", f"price_points?select=offer_id,observed_on,price&offer_id=gte.{lo}&offer_id=lt.{lo + 400}"
                                 f"&order=offer_id,observed_on&limit=1000&offset={offset}")
            for r in rows:
                if r["offer_id"] in offers:
                    hist[offers[r["offer_id"]]].append([r["observed_on"], float(r["price"])])
            if len(rows) < 1000:
                break
            offset += 1000
    print(f"bootstrap : historique de {len(hist)} offres repris de price_points")
    return hist


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--bootstrap", action="store_true")
    ap.add_argument("--feed", type=int)
    ap.add_argument("--new-max", type=int, default=NEW_ROWS_MAX, help="nouveaux produits par relevé (rattrapage surveillé)")
    ap.add_argument("--reindex", action="store_true", help="renvoie tout l'index Supabase (sans réécrire les fragments)")
    ap.add_argument("--all-shards", action="store_true", help="réécrit tous les fragments R2 (index inchangé)")
    ap.add_argument("--full", action="store_true", help="réécrit tous les fragments et tout l'index (après un changement de règle)")
    args = ap.parse_args()
    today = datetime.now(timezone.utc).date().isoformat()
    run_start = datetime.now(timezone.utc).isoformat()
    history = bootstrap_history() if args.bootstrap else {}

    feeds = [f for f in tp.build_perimeter() if args.feed in (None, f.feed_id)]
    print(f"Relevé {today} — {len(feeds)} flux")
    offers_by_key: dict[tuple[str, str], list[dict]] = defaultdict(list)
    touched: set[tuple[str, str]] = set()
    feeds_rows, ok = [], True
    failed, guarded, pending_states = [], [], []     # états écrits en dernier : un arrêt en cours de route ne perd aucun changement

    from concurrent.futures import ThreadPoolExecutor

    # Partenaires : marché (devise + langue) puis un seul flux par boutique et par marché.
    def resolve(feed):
        try:
            return tp.resolve_partner(feed)
        except Exception as e:     # devise illisible : traité comme un flux en échec (offres de la veille gardées)
            print(f"  ✗ flux {feed.feed_id} (devise) : {str(e)[:200]}")
            failed.append(feed)
            return None
    partners = [f for f in feeds if f.synthetic_ok]
    with ThreadPoolExecutor(max_workers=8) as rp:
        resolved = [f for f in rp.map(resolve, partners) if f]
    chosen = tp.select_partner_feeds(resolved)
    feeds = [f for f in feeds if not f.synthetic_ok] + chosen
    ok = not failed
    print(f"Partenaires : {len(partners)} flux, {len(resolved)} dans un marché, {len(chosen)} retenus "
          f"(une langue par boutique et par marché)")

    # Produits comparables : codes EAN présents dans au moins deux boutiques d'un même marché (colonne EAN
    # seule, ~20 s pour tous les flux). Les flux plafonnés les gardent en priorité (tp.select_offers).
    def eans_of(feed):
        try:
            return feed, tp.feed_eans(feed)
        except Exception as e:
            print(f"  (EAN du flux {feed.feed_id} indisponibles : {str(e)[:120]})")
            return feed, set()
    with ThreadPoolExecutor(max_workers=6) as ep:
        owners: dict[str, dict[str, set]] = defaultdict(lambda: defaultdict(set))
        for f, eans in ep.map(eans_of, feeds):
            for e in eans:
                owners[f.market][e].add(f.merchant_key)
    prefer = {m: frozenset(e for e, who in d.items() if len(who) >= 2) for m, d in owners.items()}
    del owners
    print("Produits comparables (EAN chez ≥ 2 boutiques) :", {m: len(v) for m, v in prefer.items()})

    def fetch(feed):
        """Téléchargement (flux Awin + état R2), en parallèle : la durée de la tâche CI est facturée."""
        prev = r2_get_json(f"{PREFIX}/state/{feed.feed_id}.json.gz") or {}
        keep = frozenset(aw for aw, v in prev.items() if not v.get("x"))     # retenues au relevé précédent
        return feed, tp.download_feed(feed, prefer.get(feed.market, frozenset()), keep), prev

    pool = ThreadPoolExecutor(max_workers=4)
    futures = [(f, pool.submit(fetch, f)) for f in feeds]
    for orig, fut in futures:          # traités dans l'ordre du périmètre, au fil des téléchargements
        try:
            got = fut.result()
            if got is None:
                continue
            feed, incoming, prev = got
            state_key = f"{PREFIX}/state/{feed.feed_id}.json.gz"
            if args.bootstrap and not prev:
                prev = {aw: {"k": o["ean"], "p": None, "o": None, "s": None, "c": None,
                             "h": history.get((feed.feed_id, aw), []), "x": None} for aw, o in incoming.items()}
            guard_key = f"{PREFIX}/state/{feed.feed_id}.guard.json.gz"
            shrunk = prev and len(incoming) < tp.REMOVAL_GUARD * sum(1 for v in prev.values() if not v.get("x"))
            since = (r2_get_json(guard_key) or {}).get("since") if shrunk else None
            if shrunk and (since is None or since >= (datetime.fromisoformat(today) - timedelta(days=GUARD_DAYS)).date().isoformat()):
                # Flux effondré d'un coup (Awin en cours d'import, flux tronqué) : on garde l'état et les offres de
                # la veille (reprises des fragments, comme un flux en échec) pendant GUARD_DAYS jours au plus.
                print(f"  ⚠ flux {feed.feed_id} anormalement petit ({len(incoming)}) : offres de la veille gardées"
                      f" (depuis le {since or today})")
                if since is None and not args.dry_run:
                    r2_put_json(guard_key, {"since": today}, cache_seconds=0)
                state, t = update_state({k: v for k, v in prev.items() if k in incoming}, incoming, today)
                state.update({k: v for k, v in prev.items() if k not in incoming})
                guarded.append(feed)
            else:
                if shrunk:
                    print(f"  flux {feed.feed_id} réduit depuis plus de {GUARD_DAYS} jours : réduction acceptée")
                if since is not None or shrunk:
                    if not args.dry_run:
                        s3.delete_object(Bucket=R2_BUCKET_NAME, Key=guard_key)
                state, t = update_state(prev, incoming, today)
            touched |= {(feed.market, k) for k in t}
            if t or not prev or args.bootstrap:
                pending_states.append((state_key, state))
            for aw, o in incoming.items():
                st = state[aw]
                offers_by_key[(feed.market, o["ean"])].append({
                    "m": feed.merchant_key, "n": feed.merchant_name or feed.merchant_key, "a": feed.awin_mid,
                    "p": st["p"], "o": st["o"], "s": st["s"], "c": st["c"], "h": st["h"], "x": None,
                    "u": o["merchant_url"], "f": feed.last_imported, "t": o["name"], "b": o["brand"],
                    "i": o["image_url"], "cat": o["merchant_category"], "d": o["description"],
                })
            feeds_rows.append({"feed_id": feed.feed_id, "merchant_key": feed.merchant_key,
                               "merchant_name": feed.merchant_name or feed.merchant_key, "awin_mid": feed.awin_mid,
                               "market": feed.market, "currency": feed.currency, "last_imported": feed.last_imported,
                               "offers": len(incoming), "updated_at": run_start})
            print(f"  flux {feed.feed_id} ({feed.merchant_key}, {feed.market}) : {len(incoming)} offres, {len(t)} produits touchés")
        except Exception as e:  # un flux en échec n'empêche pas les autres
            print(f"  ✗ flux {orig.feed_id} : {str(e)[:200]}")
            failed.append(orig)
            ok = False
    pool.shutdown()

    existing = fetch_existing_index()
    if failed or guarded:
        print(f"Flux en échec : {len(failed)}, anormalement petits : {len(guarded)} ; offres de la veille reprises : "
              f"{carry_over(failed + guarded, offers_by_key, existing)}")

    # Boutiques « toujours en promo » (> 80 % des offres avec prix barré) : pas de bon plan.
    share = defaultdict(lambda: [0, 0])
    for (market, _), offs in offers_by_key.items():
        for o in offs:
            share[(market, o["m"])][0] += 1
            share[(market, o["m"])][1] += 1 if o["o"] else 0
    perpetual = {k for k, (n, d) in share.items() if n >= 50 and d / n > PERPETUAL_SHARE}
    for (market, _), offs in offers_by_key.items():
        for o in offs:
            o["perpetual"] = (market, o["m"]) in perpetual

    # Produits, fragments, index.
    rules = tp_rules()
    products: dict[str, dict[str, dict]] = defaultdict(dict)
    for (market, key), offs in offers_by_key.items():
        prod = build_product(key, offs, today)
        if prod:
            products[market][key] = prod
    total = sum(len(v) for v in products.values())
    print(f"Produits actifs : {total} ({ {m: len(v) for m, v in products.items()} })")

    # Catalogue de taille fixe (CATALOG_MAX) : les produits écartés quittent l'index (liste « gone » plus bas).
    # Boutique affichée (la moins chère) : la première offre pouvait être une offre retirée d'une autre boutique
    # (PCHUB gardait 3 946 produits seuls pour un plafond de 2 029, le 18/11).
    owner = {(m, k): p["row"]["best_merchant"] for m, prods in products.items() for k, p in prods.items()}
    kept, cap = select_catalog(products, owner, existing)
    dropped = [(m, k) for m, prods in products.items() for k in prods if (m, k) not in kept]
    for m, k in dropped:
        del products[m][k]
    print(f"Catalogue : {len(kept)} produits gardés sur {total} (au plus {cap} par boutique hors comparables) ; "
          f"{sum(1 for mk in dropped if mk in existing)} retirés de l'index")

    # Index Supabase : produits nouveaux ou modifiés, et produits disparus (remis à zéro).
    rows = []
    for market, prods in products.items():
        for key, prod in prods.items():
            prev = existing.get((market, key))
            # Écrite seulement si elle diffère de l'index (et non « si le flux a changé ») : un relevé interrompu,
            # un flux en échec ou une nouvelle règle se rattrapent seuls, sans réécriture massive.
            if prev and prev["sig"] == row_sig(prod["row"]) and not (args.bootstrap or args.reindex or args.full):
                continue
            row = {**prod["row"], "market": market, "updated_at": run_start}
            row["slug"] = prev["slug"] if prev else slugify_title(row["title"])
            # Catégorie de la boutique → rayon par défaut de la boutique → « autres » (classé ensuite par
            # Gemini Decision, scripts/classify_rayons.py). Pas de règles sur le titre : trop d'erreurs
            # (« Coquelicot » → téléphones par « coque », « Video Game Vixen Costume » → TV).
            # Rayon de la catégorie décidé par Gemini (category_rayons, scripts/classify_categories.py) d'abord ;
            # catégorie jugée trop vague (« autres ») : pas de mots-clés, le titre décidera (classify_rayons.py).
            by_cat = rules["category"].get(row["category"] or "")
            row["rayon"] = (prev["rayon"] if prev and prev["rayon"] else None) or \
                (by_cat if by_cat and by_cat != "autres" else None) or \
                (None if by_cat else rayon_for(row["category"], rules["cat"])) or \
                rules["merchant"].get(next(o["m"] for o in offers_by_key[(market, key)] if o["n"] == row["best_merchant"]), "autres")
            rows.append(row)
    dirty_shards = defaultdict(set)
    for market, key in touched:
        dirty_shards[market].add(shard_of(key))
    # Fiche modifiée sans changement d'état du flux (offre aberrante écartée, titre décodé…) : son fragment aussi.
    for r in rows:
        dirty_shards[r["market"]].add(shard_of(r["ean"]))
    for m, k in dropped:               # fiche retirée de l'index : retirée aussi de son fragment
        if (m, k) in existing:
            dirty_shards[m].add(shard_of(k))
    if args.bootstrap or args.full:
        for market, prods in products.items():
            dirty_shards[market] |= {shard_of(k) for k in prods}
    # Tous les fragments réécrits d'après le catalogue (sans toucher à l'index) : sur demande, et chaque dimanche.
    # 20/11 : un passage interrompu avait laissé 11 % des fiches UK dans l'index mais absentes de leur fragment
    # (404). ~16 000 écritures R2 par semaine, loin de la limite gratuite (1 million par mois).
    if args.all_shards or datetime.now(timezone.utc).weekday() == 6:
        every = {f"{i:03x}" for i in range(16 ** SHARD_HEX)}
        for market in products:
            dirty_shards[market] |= every
    if args.reindex:
        dirty_shards.clear()
    if args.dry_run:
        print(f"[dry-run] fragments à écrire : { {m: len(s) for m, s in dirty_shards.items()} }")
        print(f"[dry-run] lignes d'index à écrire : {len(rows)} (dont {sum(1 for r in rows if (r['market'], r['ean']) not in existing)} nouvelles)")
        rows_all = [p["row"] for m in products.values() for p in m.values()]
        print(f"[dry-run] prix ayant changé : {sum(1 for r in rows_all if r['price_changed_at'])} ; baisses constatées : "
              f"{sum(1 for r in rows_all if r['drop_pct'])} ; au plus bas relevé : {sum(1 for r in rows_all if r['lowest_since'])}")
        return
    shard_members = defaultdict(lambda: defaultdict(dict))
    for market, prods in products.items():
        for key, prod in prods.items():
            sid = shard_of(key)
            if sid in dirty_shards[market]:
                shard_members[market][sid][key] = prod["doc"]
    jobs =[(f"{PREFIX}/shards/{market}/{sid}.json.gz", shard_members[market].get(sid, {}))
            for market, sids in dirty_shards.items() for sid in sids]
    with ThreadPoolExecutor(max_workers=16) as pool:          # envois en parallèle (milliers de fragments)
        list(pool.map(lambda j: r2_put_json(*j), jobs))
    print(f"R2 : {len(jobs)} fragments écrits")

    gone = [(m, k) for (m, k), prev in existing.items() if prev["offers_count"] > 0 and k not in products.get(m, {})]
    new = [r for r in rows if (r["market"], r["ean"]) not in existing]
    changed = [r for r in rows if (r["market"], r["ean"]) in existing]
    if not args.bootstrap:
        new.sort(key=lambda r: -(r.get("merchants_count") or 0))            # comparables d'abord
        changed.sort(key=lambda r: (r.get("drop_pct") is None, -(r.get("merchants_count") or 0)))   # baisses d'abord
        if len(new) > args.new_max or len(changed) > CHANGED_ROWS_MAX:
            print(f"Écritures plafonnées : {min(len(new), args.new_max)} nouveaux sur {len(new)}, "
                  f"{min(len(changed), CHANGED_ROWS_MAX)} modifiés sur {len(changed)} ; le reste aux prochains relevés")
        rows = changed[:CHANGED_ROWS_MAX] + new[:args.new_max]
    # Garde-fou : base gratuite plafonnée à 500 Mo. Au-delà de DB_GUARD_MB, plus de nouveaux produits
    # (les existants restent à jour) ; le rapport Telegram signale la taille.
    mb = int(tp._sb("POST", "rpc/db_usage", {})["bytes"]) / 1048576
    if mb > DB_GUARD_MB:
        before = len(rows)
        rows = [r for r in rows if (r["market"], r["ean"]) in existing]
        print(f"⚠ base à {mb:.0f} Mo (> {DB_GUARD_MB}) : {before - len(rows)} nouveaux produits non ajoutés")
    upsert_index(rows)
    zero_out(gone, run_start)
    with ThreadPoolExecutor(max_workers=16) as pool:
        list(pool.map(lambda s: r2_put_json(*s, cache_seconds=0), pending_states))
    print(f"R2 : {len(pending_states)} états de flux écrits")
    tp._sb("POST", "feeds?on_conflict=feed_id", feeds_rows, prefer="resolution=merge-duplicates,return=minimal")
    tp._sb("POST", "rpc/refresh_brand_index", {})
    tp._sb("POST", "rpc/refresh_site_stats", {})        # compteurs des pages (rayons, comparés, bons plans)
    try:   # index de recherche : liste d'attente vidée en fin de relevé (recherche rapide ensuite)
        tp._sb("POST", "rpc/clean_search_index", {})
    except RuntimeError as e:
        print(f"  (nettoyage de l'index de recherche : {str(e)[:120]})")
    print(f"Supabase : {len(rows)} produits écrits, {len(gone)} remis à zéro")
    tp.notify_indexnow(run_start)
    sys.exit(0 if ok else 1)


def tp_rules() -> dict:
    rr = tp._sb("GET", "rayon_rules?select=rayon,pattern,priority&limit=1000")
    mr = tp._sb("GET", "merchant_rayons?select=merchant_key,rayon&limit=1000")
    category = {}
    for offset in range(0, 1_000_000, 1000):          # ~5 000 catégories, 1 000 par requête
        page = tp._sb("GET", f"category_rayons?select=category,rayon&order=category&limit=1000&offset={offset}")
        category.update({r["category"]: r["rayon"] for r in page})
        if len(page) < 1000:
            break
    return {"cat": compile_rules(rr), "merchant": {r["merchant_key"]: r["rayon"] for r in mr}, "category": category}


def fetch_existing_index() -> dict[tuple[str, str], dict]:
    out = {}
    for market in ("fr", "uk", "us", "de", "es", "it", "nl"):
        last = ""
        while True:
            for attempt in range(4):      # base lente ou en redémarrage : on réessaie plutôt que d'abandonner le relevé
                try:
                    rows = tp._sb("GET", f"products_ean?select=market,ean,slug,rayon,{','.join(SIG_COLS)}&market=eq.{market}"
                                         f"&ean=gt.{last}&order=ean&limit=1000")
                    break
                except RuntimeError:
                    if attempt == 3:
                        raise
                    time.sleep(20 * (attempt + 1))
            for r in rows:     # seule l'empreinte est gardée en mémoire (300 000 lignes)
                out[(r["market"], r["ean"])] = {"slug": r["slug"], "rayon": r["rayon"], "offers_count": r["offers_count"],
                                                "sig": row_sig(r)}
            if len(rows) < 1000:
                break
            last = rows[-1]["ean"]
    return out


def upsert_index(rows: list[dict]):
    cols = ["market", "ean", "slug", "title", "brand", "category", "rayon", "image_url", "offers_count", "merchants_count",
            "min_price", "max_price", "old_price", "refurbished_min_price", "best_merchant", "deal_ok", "updated_at",
            "prev_price", "price_changed_at", "drop_pct", "lowest_since"]
    # Un envoi à la fois : en parallèle, les écritures ont saturé la base gratuite (incident du 15/11).
    # Lots de 200 et nouvelles tentatives : chaque ligne met à jour les index de recherche (fastupdate = off),
    # un lot de 500 a dépassé le délai de la passerelle (504) pendant la reconstruction du 16/11.
    pause = 0.3
    for i in range(0, len(rows), 200):
        if i:
            time.sleep(pause)             # laisse respirer l'instance gratuite (incidents du 16/11)
        batch = [{c: r.get(c) for c in cols} for r in rows[i:i + 200]]
        for attempt in range(4):
            try:
                t0 = time.monotonic()
                tp._sb("POST", "products_ean?on_conflict=market,ean", batch, prefer="resolution=merge-duplicates,return=minimal")
                # Frein automatique : pause égale à la durée du dernier lot (au plus 30 s). Base lente → relevé plus lent.
                pause = min(30.0, max(0.3, time.monotonic() - t0))
                break
            except RuntimeError as e:
                if attempt == 3 or not any(f"→ {c}" in str(e) for c in (500, 502, 503, 504)):
                    raise
                time.sleep(15 * (attempt + 1))
    print(f"  index : {len(rows)} lignes envoyées")


def zero_out(keys: list[tuple[str, str]], now: str):
    """Produits disparus de tous les flux : supprimés de l'index (leur page renvoyait déjà 404). Ils étaient
    gardés vides auparavant et occupaient 16 % de la base gratuite (17/11). S'il revient, un produit est
    simplement réinséré (même adresse : elle dérive du titre)."""
    by_market = defaultdict(list)
    for m, k in keys:
        by_market[m].append(k)
    for m, ks in by_market.items():
        for i in range(0, len(ks), 200):
            if i:
                time.sleep(0.3)
            tp._sb("DELETE", f"products_ean?market=eq.{m}&ean=in.({','.join(ks[i:i + 200])})", prefer="return=minimal")


if __name__ == "__main__":
    main()
