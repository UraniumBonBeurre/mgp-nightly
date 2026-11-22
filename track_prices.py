#!/usr/bin/env python3
"""
track_prices.py — Relevé quotidien des prix par EAN (phase 1, docs/PLAN-REORIENTATION.md)
==========================================================================================

Pour chaque flux du périmètre :
  1. télécharge le flux Awin (gzip, streaming)
  2. compare avec les offres en base
  3. écrit uniquement les différences :
       - offres nouvelles ou dont le prix change → offers + price_points
       - offres dont le stock change / qui réapparaissent → offers
       - offres absentes du flux → offers.removed_at
Puis recalcule products_ean (fonction SQL refresh_products_ean).

Bibliothèque standard uniquement : le workflow n'installe aucune dépendance.

Usage :
    python3 track_prices.py                 # relevé complet
    python3 track_prices.py --dry-run       # télécharge et compare, n'écrit rien
    python3 track_prices.py --feed 16437    # un seul flux
"""

import argparse
import csv
import gzip
import hashlib
import html
import io
import json
import re
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from urllib.parse import quote

from settings import AWIN_PRODUCTDATA_KEY, SUPABASE_KEY, SUPABASE_URL


@dataclass(frozen=True)
class Feed:
    feed_id: int
    merchant_key: str
    awin_mid: int
    condition: str = "new"                       # 'new' | 'refurbished'
    exclude_category_prefixes: tuple[str, ...] = ()
    market: str = "fr"                           # 'fr' | 'uk' | 'us' (src/lib/markets.ts)
    merchant_name: str = ""
    currency: str = "EUR"
    cap: int | None = None                       # nombre max d'offres gardées (sélection stable)
    synthetic_ok: bool = False                   # produits sans EAN acceptés (identifiant synthétique)
    products: int = 0                            # nombre de produits annoncé par la liste Awin
    last_imported: str | None = None             # date du dernier import Awin du flux
    language: str = "fr"                         # langue demandée au téléchargement du flux


# Périmètre initial : produits vendus par l'enseigne elle-même (pas de place de marché).
PERIMETER = (
    Feed(116516, "darty-fr", 7735, merchant_name="Darty"),
    Feed(116523, "darty-fr", 7735, condition="refurbished", merchant_name="Darty"),
    Feed(16437, "rue-du-commerce", 6901, merchant_name="Rue du Commerce"),
    # CD et DVD : ~115 000 lignes sur 147 000, hors cible.
    Feed(116908, "fnac-fr", 12665, exclude_category_prefixes=("Musique", "DVD"), merchant_name="Fnac"),
)

# ── Tous les autres partenaires (docs/PLAN-REORIENTATION.md §0.4 quater) ────
# Construits chaque nuit depuis la liste des flux Awin des programmes rejoints.
CORE_ADVERTISERS = {12665, 7735, 6901}       # Fnac, Darty, Rue du Commerce : flux ci-dessus seulement
# Aussui (127361) : recharges de jeux (V-Bucks…), sans code-barres, en euros et en anglais (aurait rempli le marché
# français de codes numériques), 21/11. El Corte Inglés (13075), ignoré faute de marché espagnol, repris le 21/11.
SKIP_ADVERTISERS = {127361}
ONLY_FEEDS = {26009: {110451}}               # AliExpress : flux « Bestsellers » seulement
SKIP_FEED_NAME = re.compile(r"test|livre|libro|book|\bold\b|occasion", re.IGNORECASE)
# Un marché = une devise + une langue. En euros, la langue du flux départage France et Allemagne.
MARKET_LANG = {"fr": "fr", "de": "de", "uk": "en", "us": "en", "it": "it", "es": "es"}
EUR_LANG_MARKET = {"fr": "fr", "de": "de", "it": "it", "es": "es", "en": "fr"}   # flux anglais en euros : marché français (repli)
# Plafonds par flux (limite 500 Mo de Supabase). Marché américain prioritaire (demande de l'utilisateur).
PARTNER_CAP = {"us": 5000, "uk": 5000, "fr": 3000, "de": 3000, "it": 5000, "es": 5000}   # + tous les produits comparables (OVERLAP_CAP)
ALIEXPRESS_CAP = 3000
MAX_FEED_AGE_DAYS = 60                       # flux que la boutique n'a pas mis à jour depuis 60 jours : ignoré (revient
                                             # seul s'il est rafraîchi). 21/11 : 160 flux figés depuis mai = 85 749
                                             # produits (36 % du catalogue), 42 prix changés en 5 jours, 2 comparés
DESCRIPTION_MAX = 200

FEED_COLUMNS = (
    "aw_product_id,product_name,brand_name,ean,product_GTIN,search_price,product_price_old,currency,"
    "in_stock,merchant_category,category_name,merchant_deep_link,merchant_image_url,aw_image_url,description"
)
SITE = "https://mygoodpick.net"
INDEXNOW_KEY = "e2f569046b3da5ad02dbfbd17528d111"  # public : servi à {SITE}/{clé}.txt
INDEXNOW_MARKETS = ("fr", "uk", "de", "us", "it", "es")    # marchés indexés (src/lib/markets.ts, SITES indexable)
BATCH = 500
REMOVAL_GUARD = 0.5  # flux < 50 % des offres actives → suspect, pas de retrait
MAX_OLD_PRICE_RATIO = 4  # prix barré > 4 × prix : erreur de flux (ex. 100 000 € → 49,99 €)
# Produits reconditionnés vendus dans les flux « neuf » (le prix barré y est le prix du neuf).
REFURBISHED_NAME = re.compile(r"reconditionn|refurbish|\boccasion\b|seconde vie", re.IGNORECASE)

csv.field_size_limit(10**8)


# ── Fonctions pures (testées dans tests/test_track_prices.py) ────────────────

def normalize_ean(raw: str | None) -> str | None:
    """Chiffres seulement, complétés à 14 caractères. None si moins de 8 chiffres ou nul."""
    digits = "".join(ch for ch in (raw or "") if ch.isdigit())
    if len(digits) < 8 or len(digits) > 14 or not digits.strip("0"):
        return None
    return digits.zfill(14)


def parse_price(raw: str | None) -> Decimal | None:
    try:
        p = Decimal((raw or "").strip().replace(",", ".")).quantize(Decimal("0.01"))
    except InvalidOperation:
        return None
    return p if p > 0 else None


def plausible_old_price(old: Decimal | None, price: Decimal) -> Decimal | None:
    """Prix barré de l'enseigne, gardé seulement s'il est supérieur et plausible."""
    if old is None or old <= price or old > price * MAX_OLD_PRICE_RATIO:
        return None
    return old


def synthetic_key(merchant_key: str, title: str) -> str:
    """Identifiant d'un produit sans EAN : « x » + 13 caractères hexadécimaux, stable.

    Calculé sur la boutique et le titre normalisé : les variantes (tailles, couleurs) qu'une boutique
    publie comme produits distincts sous le même titre deviennent un seul produit (plusieurs offres).
    """
    norm = " ".join(re.sub(r"[^a-z0-9]+", " ", html.unescape(title).lower()).split())
    return "x" + hashlib.sha1(f"{merchant_key}:{norm}".encode()).hexdigest()[:13]


# Images « absentes » publiées par certaines boutiques (pixel vide, « no image available »…).
PLACEHOLDER_IMAGE = re.compile(r"no[-_ ]?image|noimage|placeholder|image[-_]?not[-_]?available|default[-_]?(product|image)|coming[-_]?soon|missing", re.IGNORECASE)


def clean_image(url: str | None) -> str | None:
    """Première image seulement : certains flux en listent plusieurs, séparées par des virgules."""
    url = re.split(r",\s*(?=https?://)", (url or "").strip())[0]
    if not url.startswith("http") or PLACEHOLDER_IMAGE.search(url):
        return None
    return url


def clean_description(raw: str | None) -> str | None:
    """Description du flux : HTML retiré, espaces normalisés, coupée au mot (DESCRIPTION_MAX)."""
    text = " ".join(html.unescape(re.sub(r"<[^>]+>", " ", raw or "")).split())
    if len(text) < 20:
        return None
    if len(text) > DESCRIPTION_MAX:
        text = text[:DESCRIPTION_MAX].rsplit(" ", 1)[0].rstrip(",;:.") + "…"
    return text


# Prix « bidon » des flux : PCHUB et PcNova affichent 1 067 504,79 £ (1 281 005,75 £ TTC) pour les articles
# indisponibles. Aucun produit vendu sur le site n'approche ce montant (le plus cher réel : ~250 000).
PRICE_MAX = Decimal("500000")


def unescape_text(s: str) -> str:
    """Entités HTML, y compris doublement encodées (« &amp;quot; ») : vues dans 7 000 titres de flux."""
    for _ in range(2):
        u = html.unescape(s)
        if u == s:
            break
        s = u
    return s


# Lignes de flux qui ne sont pas des produits : assurance ou protection de livraison vendue comme un article
# (« Shipping Protection » d'AKKO, Monsgeek… classée en périphériques, 22/11), frais de port.
NOT_A_PRODUCT = re.compile(r"\b(shipping|package|delivery|order|parcel)\s+(protection|insurance)\b|^\s*(route|navidium|seel)\b"
                           r"|versandschutz|versandversicherung|protection\s+(de\s+|d.)?(colis|livraison)|assurance\s+livraison"
                           r"|^\s*(frais\s+de\s+port|shipping\s+fee|versandkosten)\b", re.IGNORECASE)


def row_to_offer(row: dict, feed: Feed) -> dict | None:
    """Ligne de flux → offre, ou None si inexploitable."""
    category = (row.get("merchant_category") or row.get("category_name") or "").strip()
    if feed.exclude_category_prefixes and category.startswith(feed.exclude_category_prefixes):
        return None
    ean = normalize_ean(row.get("ean")) or normalize_ean(row.get("product_GTIN"))
    if not ean and feed.synthetic_ok and (row.get("product_name") or "").strip():
        ean = synthetic_key(feed.merchant_key, row["product_name"].strip())
    price = parse_price(row.get("search_price"))
    if price and price >= PRICE_MAX:
        return None
    name = unescape_text((row.get("product_name") or "").strip())
    if NOT_A_PRODUCT.search(name):
        return None
    url = (row.get("merchant_deep_link") or "").strip()
    aw_id = (row.get("aw_product_id") or "").strip()
    if not (ean and price and name and url and aw_id):
        return None
    return {
        "feed_id": feed.feed_id,
        "aw_product_id": aw_id,
        "market": feed.market,
        "merchant_key": feed.merchant_key,
        "merchant_name": feed.merchant_name or feed.merchant_key,
        "awin_mid": feed.awin_mid,
        "ean": ean,
        "name": name[:300],
        "brand": unescape_text((row.get("brand_name") or "").strip()) or None,
        "price": price,
        "old_price": plausible_old_price(parse_price(row.get("product_price_old")), price),
        "currency": (row.get("currency") or feed.currency).strip() or feed.currency,
        "in_stock": (row.get("in_stock") or "1").strip().lower() in ("1", "true", "yes", "y"),
        "condition": "refurbished" if REFURBISHED_NAME.search(name) else feed.condition,
        "merchant_url": url,
        "image_url": clean_image(row.get("merchant_image_url")) or clean_image(row.get("aw_image_url")),
        "merchant_category": category or None,
        "description": clean_description(row.get("description")),
    }


def diff_offers(existing: dict[str, dict], incoming: dict[str, dict]):
    """Compare l'état en base et le flux.

    existing : aw_product_id → {"id", "price" (Decimal), "old_price", "in_stock", "removed_at"}
    incoming : aw_product_id → offre (row_to_offer)
    Retourne (prix_nouveaux_ou_changés, autres_changements, ids_retirés).
    """
    price_changed, other_changed = [], []
    for aw_id, offer in incoming.items():
        cur = existing.get(aw_id)
        if cur is None or cur["price"] != offer["price"]:
            price_changed.append(offer)
        elif (cur["in_stock"] != offer["in_stock"] or cur["removed_at"] is not None
              or cur.get("old_price") != offer.get("old_price")
              or cur.get("condition", offer.get("condition")) != offer.get("condition")
              or (not cur.get("has_description", True) and offer.get("description"))):
            other_changed.append(offer)
    removed = [cur["id"] for aw_id, cur in existing.items()
               if aw_id not in incoming and cur["removed_at"] is None]
    return price_changed, other_changed, removed


# ── E/S : flux Awin ─────────────────────────────────────────────────────────

# Sélection d'un flux plafonné, par valeur et non au hasard (mesure du 16/11 : au Royaume-Uni, 154 000
# produits sont vendus par au moins deux boutiques, que le tirage aléatoire ignorait presque tous).
OVERLAP_CAP = 150_000      # produits comparables (EAN chez ≥ 2 boutiques) gardés au-delà du plafond
PRICE_FLOOR = 10           # en devise du flux : en dessous, commission dérisoire → en dernier


def rank_offer(o: dict, prefer: frozenset | set, aw_id: str, keep: frozenset | set = frozenset()) -> tuple:
    """Ordre de sélection : comparable ou déjà retenu la veille, vraie remise ≥ 20 %, image, prix ≥ PRICE_FLOOR,
    puis tirage stable."""
    deal = bool(o.get("old_price") and o.get("price") and o["price"] <= o["old_price"] * Decimal("0.8"))
    return (not (o["ean"] in prefer or aw_id in keep), not deal, not o.get("image_url"),
            (o.get("price") or 0) < PRICE_FLOOR, hashlib.sha1(aw_id.encode()).hexdigest())


def select_offers(offers: dict[str, dict], cap: int, prefer: frozenset | set,
                  keep: frozenset | set = frozenset()) -> dict[str, dict]:
    """Garde cap offres (et tous les produits comparables ou déjà retenus, jusqu'à OVERLAP_CAP), classées par
    rank_offer. keep : offres retenues au relevé précédent (état R2) — la sélection reste stable quand la liste
    des comparables varie (flux d'une autre boutique tronqué un soir : 16/11, PCHUB 316 000 → 13 000 lignes)."""
    if len(offers) <= cap:
        return offers
    ordered = sorted(offers, key=lambda k: rank_offer(offers[k], prefer, k, keep))
    n_pref = sum(1 for k, o in offers.items() if o["ean"] in prefer or k in keep)
    return {k: offers[k] for k in ordered[: max(cap, min(n_pref, OVERLAP_CAP))]}


def feed_eans(feed: Feed) -> set[str]:
    """Codes EAN d'un flux (colonnes ean/GTIN seulement, rapide) : repère les produits vendus ailleurs."""
    url = (f"https://productdata.awin.com/datafeed/download/apikey/{AWIN_PRODUCTDATA_KEY}/fid/{feed.feed_id}"
           f"/format/csv/language/{feed.language}/delimiter/%2C/compression/gzip/columns/ean,product_GTIN/")
    out = set()
    with urllib.request.urlopen(url, timeout=300) as resp:
        for row in csv.DictReader(io.TextIOWrapper(gzip.GzipFile(fileobj=resp), encoding="utf-8", errors="replace")):
            e = normalize_ean(row.get("ean")) or normalize_ean(row.get("product_GTIN"))
            if e:
                out.add(e)
    return out


def download_feed(feed: Feed, prefer: frozenset | set = frozenset(), keep: frozenset | set = frozenset()) -> dict[str, dict]:
    url = (
        f"https://productdata.awin.com/datafeed/download/apikey/{AWIN_PRODUCTDATA_KEY}"
        f"/fid/{feed.feed_id}/format/csv/language/{feed.language}/delimiter/%2C/compression/gzip"
        f"/adultcontent/1/columns/{quote(FEED_COLUMNS, safe='')}/"
    )
    offers: dict[str, dict] = {}
    with urllib.request.urlopen(url, timeout=300) as resp:
        reader = csv.DictReader(io.TextIOWrapper(gzip.GzipFile(fileobj=resp), encoding="utf-8", errors="replace"))
        for row in reader:
            o = row_to_offer(row, feed)
            if o:
                offers[o["aw_product_id"]] = o
    if feed.cap:
        offers = select_offers(offers, feed.cap, prefer, keep)
    return offers


LANG_CODES = {"french": "fr", "english": "en", "german": "de", "dutch": "nl", "spanish": "es", "italian": "it"}


def slugify(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", html.unescape(name).lower()).strip("-")[:60]


def build_perimeter() -> list[Feed]:
    """Flux principaux (PERIMETER) + tous les flux des autres programmes rejoints (liste Awin)."""
    url = f"https://productdata.awin.com/datafeed/list/apikey/{AWIN_PRODUCTDATA_KEY}"
    with urllib.request.urlopen(url, timeout=120) as r:
        rows = list(csv.DictReader(io.StringIO(r.read().decode("utf-8", "replace"))))
    by_fid = {int(x["Feed ID"]): x for x in rows if (x.get("Feed ID") or "").isdigit()}
    feeds = [Feed(**{**f.__dict__, "last_imported": (by_fid.get(f.feed_id, {}).get("Last Imported") or "")[:10] or None})
             for f in PERIMETER]
    partners = []
    for x in rows:
        if (x.get("Membership Status") or "").lower() != "active":
            continue
        adv, fid = int(x["Advertiser ID"]), int(x["Feed ID"])
        if adv in CORE_ADVERTISERS or adv in SKIP_ADVERTISERS or SKIP_FEED_NAME.search(x.get("Feed Name") or ""):
            continue
        if adv in ONLY_FEEDS and fid not in ONLY_FEEDS[adv]:
            continue
        if not (x.get("URL") or "").startswith("https://"):
            continue
        name = html.unescape(x["Advertiser Name"]).strip()
        partners.append(Feed(
            feed_id=fid, merchant_key=slugify(name), awin_mid=adv, merchant_name=name,
            language=LANG_CODES.get((x.get("Language") or "").strip().lower(), "en"),
            synthetic_ok=True, last_imported=(x.get("Last Imported") or "")[:10] or None,
            products=int(x.get("No of products") or 0) if (x.get("No of products") or "").isdigit() else 0,
        ))
    return feeds + partners


def detect_currency(feed: Feed) -> str | None:
    """Devise d'un flux partenaire (lit les premières lignes) → fixe son marché."""
    url = (f"https://productdata.awin.com/datafeed/download/apikey/{AWIN_PRODUCTDATA_KEY}/fid/{feed.feed_id}"
           f"/format/csv/language/{feed.language}/delimiter/%2C/compression/gzip/columns/currency/")
    with urllib.request.urlopen(url, timeout=120) as resp:
        reader = csv.DictReader(io.TextIOWrapper(gzip.GzipFile(fileobj=resp), encoding="utf-8", errors="replace"))
        seen = [((r.get("currency") or "").strip().upper()) for _, r in zip(range(50), reader)]
    seen = [c for c in seen if c]
    return max(set(seen), key=seen.count) if seen else None


# ── E/S : Supabase REST ─────────────────────────────────────────────────────

def _sb(method: str, path: str, body=None, prefer: str = ""):
    headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}",
               "Content-Type": "application/json"}
    if prefer:
        headers["Prefer"] = prefer
    data = json.dumps(body, default=str).encode() if body is not None else None
    for attempt in range(4):
        req = urllib.request.Request(f"{SUPABASE_URL}/rest/v1/{path}", data=data, method=method, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=330) as r:   # recalcul : jusqu'à 300 s côté base
                raw = r.read()
                return json.loads(raw) if raw else None
        except urllib.error.HTTPError as e:
            if e.code < 500 or attempt == 3:
                raise RuntimeError(f"Supabase {method} {path[:60]} → {e.code} {e.read().decode()[:300]}") from None
        except (urllib.error.URLError, TimeoutError):
            if attempt == 3:
                raise
        time.sleep(2 ** attempt)


def fetch_existing(feed_id: int) -> dict[str, dict]:
    out, last_id = {}, 0
    while True:
        rows = _sb("GET", f"offers?select=id,aw_product_id,price,old_price,in_stock,condition,has_description,removed_at"
                          f"&feed_id=eq.{feed_id}&id=gt.{last_id}&order=id&limit=1000")
        for r in rows:
            out[r["aw_product_id"]] = {"id": r["id"], "price": parse_price(str(r["price"])),
                                       "old_price": parse_price(str(r["old_price"])) if r["old_price"] is not None else None,
                                       "in_stock": r["in_stock"], "condition": r["condition"],
                                       "has_description": r["has_description"], "removed_at": r["removed_at"]}
        if len(rows) < 1000:
            return out
        last_id = rows[-1]["id"]


def upsert_offers(offers: list[dict], now: str, price_changed: bool) -> list[dict]:
    returned = []
    for i in range(0, len(offers), BATCH):
        payload = [{**o, "removed_at": None, "updated_at": now,
                    **({"price_changed_at": now} if price_changed else {})}
                   for o in offers[i:i + BATCH]]
        returned += _sb("POST", "offers?on_conflict=feed_id,aw_product_id&select=id,price",
                        payload, prefer="resolution=merge-duplicates,return=representation")
    return returned


def insert_price_points(rows: list[dict], today: str):
    for i in range(0, len(rows), BATCH):
        payload = [{"offer_id": r["id"], "observed_on": today, "price": r["price"]} for r in rows[i:i + BATCH]]
        _sb("POST", "price_points?on_conflict=offer_id,observed_on", payload,
            prefer="resolution=merge-duplicates,return=minimal")


def mark_removed(ids: list[int], now: str):
    for i in range(0, len(ids), 200):
        chunk = ",".join(str(x) for x in ids[i:i + 200])
        _sb("PATCH", f"offers?id=in.({chunk})", {"removed_at": now, "updated_at": now}, prefer="return=minimal")


def notify_indexnow(since_iso: str) -> None:
    """Signale à IndexNow (Bing…) les fiches indexables modifiées depuis since_iso. Jamais bloquant."""
    try:
        # Tous les marchés indexés (le 18/11, seules les fiches françaises étaient signalées). Même chemin « prix ».
        urls = []
        for m in INDEXNOW_MARKETS:
            last, found = "", []
            while True:
                rows = _sb("GET", f"products_ean?select=ean,slug&market=eq.{m}&merchants_count=gte.2"
                                  f"&updated_at=gte.{quote(since_iso)}&ean=gt.{last}&order=ean&limit=1000")
                found += [f"{SITE}/{m}/prix/{(r['slug'] + '-') if r['slug'] else ''}{r['ean']}" for r in rows]
                if len(rows) < 1000:
                    break
                last = rows[-1]["ean"]
            if found:
                urls += [f"{SITE}/{m}", f"{SITE}/{m}/bons-plans", f"{SITE}/{m}/baisses-de-prix", *found]
        if not urls:
            print("IndexNow : aucune fiche modifiée")
            return
        for i in range(0, len(urls), 10_000):          # 10 000 adresses au plus par envoi
            chunk = urls[i:i + 10_000]
            body = json.dumps({"host": SITE.split("//")[1], "key": INDEXNOW_KEY,
                               "keyLocation": f"{SITE}/{INDEXNOW_KEY}.txt", "urlList": chunk}).encode()
            req = urllib.request.Request("https://api.indexnow.org/indexnow", data=body, method="POST",
                                         headers={"Content-Type": "application/json; charset=utf-8"})
            with urllib.request.urlopen(req, timeout=60) as r:
                print(f"IndexNow : {len(chunk)} URL signalées (HTTP {r.status})")
    except Exception as e:  # noqa: BLE001 — la diffusion ne doit jamais faire échouer le relevé
        print(f"  ⚠ IndexNow non envoyé : {e}")


# ── Orchestration ───────────────────────────────────────────────────────────

def track_feed(feed: Feed, dry_run: bool) -> bool:
    t0 = time.time()
    incoming = download_feed(feed)
    existing = fetch_existing(feed.feed_id)
    price_changed, other_changed, removed = diff_offers(existing, incoming)
    active = sum(1 for e in existing.values() if e["removed_at"] is None)
    suspect = active > 0 and len(incoming) < REMOVAL_GUARD * active
    print(f"  flux {feed.feed_id} ({feed.merchant_key}, {feed.market}, {feed.condition}) : {len(incoming)} offres valides, "
          f"{active} actives en base → {len(price_changed)} prix nouveaux/changés, "
          f"{len(other_changed)} autres changements, {len(removed)} retraits")
    if suspect:
        print(f"  ⚠ flux anormalement petit ({len(incoming)} < {REMOVAL_GUARD:.0%} de {active}) : rien n'est écrit")
        return False
    if dry_run:
        return True
    now = datetime.now(timezone.utc).isoformat()
    today = now[:10]
    written = upsert_offers(price_changed, now, price_changed=True)
    insert_price_points(written, today)
    upsert_offers(other_changed, now, price_changed=False)
    mark_removed(removed, now)
    _sb("POST", "feeds?on_conflict=feed_id", [{
        "feed_id": feed.feed_id, "merchant_key": feed.merchant_key, "merchant_name": feed.merchant_name or feed.merchant_key,
        "awin_mid": feed.awin_mid, "market": feed.market, "currency": feed.currency,
        "last_imported": feed.last_imported, "offers": len(incoming), "updated_at": now,
    }], prefer="resolution=merge-duplicates,return=minimal")
    print(f"    écrit en {time.time() - t0:.0f}s")
    return True


def resolve_partner(feed: Feed) -> Feed | None:
    """Fixe marché, devise et plafond d'un flux partenaire ; None si hors périmètre."""
    if feed.last_imported:
        age = (datetime.now(timezone.utc).date() - datetime.fromisoformat(feed.last_imported).date()).days
        if age > MAX_FEED_AGE_DAYS:
            return None
    currency = detect_currency(feed)
    market = partner_market(currency, feed.language)
    if not market:
        return None
    cap = ALIEXPRESS_CAP if feed.awin_mid == 26009 else PARTNER_CAP[market]
    return Feed(**{**feed.__dict__, "market": market, "currency": currency, "cap": cap})


def partner_market(currency: str | None, language: str) -> str | None:
    """Dollar → us, livre → uk ; en euros, la langue : français (ou anglais) → fr, allemand → de, italien → it,
    espagnol → es (marchés ouverts le 21/11)."""
    if currency == "USD":
        return "us"
    if currency == "GBP":
        return "uk"
    if currency == "EUR":
        return EUR_LANG_MARKET.get(language)
    return None


def select_partner_feeds(feeds: list[Feed]) -> list[Feed]:
    """Par annonceur et par marché, les flux d'une seule langue : celle du marché, sinon l'anglais.

    Beaucoup de boutiques publient le même catalogue en 5 à 10 langues (House-of-Sneakers,
    Cronjager, Tsarbomba…) : sans ce tri, chaque traduction devenait un produit en double.
    Plusieurs flux d'une même langue (gammes distinctes) sont tous gardés ; un article présent
    dans deux d'entre eux est fusionné par catalog_sync.build_product."""
    groups: dict[tuple[int, str], list[Feed]] = {}
    for f in feeds:
        groups.setdefault((f.awin_mid, f.market), []).append(f)
    out = []
    for (_, market), fs in groups.items():
        for lang in dict.fromkeys((MARKET_LANG[market], "en")):
            same = [f for f in fs if f.language == lang]
            if same:
                out.extend(same)
                break
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--feed", type=int, help="ne traiter que ce feed_id")
    ap.add_argument("--core-only", action="store_true", help="Darty, Fnac, Rue du Commerce seulement")
    args = ap.parse_args()

    if not (SUPABASE_URL and SUPABASE_KEY and AWIN_PRODUCTDATA_KEY):
        sys.exit("NEXT_PUBLIC_SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY et AWIN_PRODUCTDATA_KEY requis")

    run_start = datetime.now(timezone.utc).isoformat()
    feeds = [f for f in (PERIMETER if args.core_only else build_perimeter()) if args.feed in (None, f.feed_id)]
    print(f"Relevé des prix — {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC — {len(feeds)} flux"
          f"{' (dry-run)' if args.dry_run else ''}")
    ok = True
    for feed in feeds:
        try:
            if feed.synthetic_ok:                      # partenaire : marché, devise, plafond
                resolved = resolve_partner(feed)
                if resolved is None:
                    print(f"  · flux {feed.feed_id} ({feed.merchant_key}) ignoré : trop ancien ou devise hors marché")
                    continue
                feed = resolved
            ok &= track_feed(feed, args.dry_run)
        except Exception as e:  # un flux en échec n'empêche pas les autres
            print(f"  ✗ flux {feed.feed_id} : {e}")
            ok = False

    if not args.dry_run:
        n = _sb("POST", "rpc/refresh_products_ean", {})
        print(f"products_ean : {n} lignes créées ou modifiées")
        _sb("POST", "rpc/refresh_discount_stats", {})   # boutiques en « promo permanente » exclues des bons plans
        notify_indexnow(run_start)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
