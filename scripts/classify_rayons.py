#!/usr/bin/env python3
"""
scripts/classify_rayons.py — Classe par Gemini les produits restés en rayon « autres »
=====================================================================================

Le relevé (catalog_sync.py) range un produit d'après la catégorie de la boutique ou le rayon par
défaut de la boutique ; ceux qui restent en « autres » sont envoyés ici à Gemini Flash Lite par lots
de 600 (réponse JSON contrainte à la liste des rayons), 3 appels en parallèle, nouvelles tentatives à
attente exponentielle. Mesuré le 16 novembre : lots de 200 / 400 / 600 / 800 → 44 / 39 / 37 / 30 ms par
produit, toutes réponses complètes, accord 86 → 95 % avec le classement existant. Gemini Decision 1 ne
traitait que 40 produits par appel et 4 appels par minute (10 à 15 fois plus lent).

Chaque produit soumis est daté (rayon_ai_at) : il n'est jamais renvoyé, même laissé en « autres ».

Usage :
    python3 scripts/classify_rayons.py              # jusqu'à 3 000 produits (nuit)
    python3 scripts/classify_rayons.py --max 60000  # rattrapage
"""

import argparse
import json
import random
import sys
import time
import urllib.error
import urllib.request
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from settings import GOOGLE_AI_API_KEY, SUPABASE_KEY, SUPABASE_URL  # noqa: E402

BATCH = 600                                              # produits par appel (~22 000 + 9 000 jetons)
MODELS = ("gemini-3.1-flash-lite", "gemini-3.5-flash-lite")  # le second en secours (surcharge, quota)
WORKERS = 3                                              # appels simultanés (~8 appels/min, sous la limite gratuite)
API = "https://generativelanguage.googleapis.com/v1beta/models/{}:generateContent"


def sb(method: str, path: str, body=None):
    req = urllib.request.Request(f"{SUPABASE_URL}/rest/v1/{path}", method=method,
                                 data=json.dumps(body).encode() if body is not None else None,
                                 headers={"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}",
                                          "Content-Type": "application/json", "Prefer": "return=minimal"})
    with urllib.request.urlopen(req, timeout=120) as r:
        raw = r.read()
        return json.loads(raw) if raw else None


def classify(products: list[dict], rayons: list[dict]) -> dict[int, str]:
    """Indice du produit dans le lot → slug du rayon."""
    cats = "\n".join(f"- {r['slug']}: {r['label_en']}" for r in rayons)
    lines = "\n".join(f"{i}\t{p['title'][:140]}\t{p.get('best_merchant') or ''}" for i, p in enumerate(products))
    return ask_gemini(f"Classify each product (id<TAB>title<TAB>store) into exactly one shop department slug.\n"
                      f"Departments:\n{cats}\nUse 'autres' only if none fits.\nProducts:\n{lines}",
                      [r["slug"] for r in rayons])


def ask_gemini(prompt: str, slugs: list[str], models: tuple[str, ...] = MODELS) -> dict[int, str]:
    """Réponse JSON [{i, r}] contrainte aux slugs → {i: slug}. Essais à attente exponentielle, modèle de secours."""
    body = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "responseMimeType": "application/json", "temperature": 0,
            "responseSchema": {"type": "ARRAY", "items": {"type": "OBJECT", "required": ["i", "r"], "properties": {
                "i": {"type": "INTEGER"}, "r": {"type": "STRING", "enum": slugs}}}},
        },
    }
    last = None
    for attempt in range(7):
        model = models[min(attempt // 3, len(models) - 1)]
        req = urllib.request.Request(API.format(model), data=json.dumps(body).encode(), method="POST",
                                     headers={"x-goog-api-key": GOOGLE_AI_API_KEY, "Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=180) as r:
                res = json.loads(r.read())
            out = json.loads(res["candidates"][0]["content"]["parts"][0]["text"])
            return {o["i"]: o["r"] for o in out if isinstance(o.get("i"), int) and o.get("r") in slugs}
        except urllib.error.HTTPError as e:
            last = f"{model} {e.code}"
            if e.code in (429, 500, 503):
                time.sleep(min(120, 5 * 2 ** attempt) + random.uniform(0, 3))   # attente exponentielle
                continue
            raise RuntimeError(f"Gemini {e.code}: {e.read().decode()[:200]}") from None
        except (KeyError, IndexError, json.JSONDecodeError, TimeoutError, urllib.error.URLError) as e:
            last = f"{model} réponse illisible ou réseau ({str(e)[:80]})"   # réponse tronquée, délai : on réessaie
            time.sleep(min(60, 3 * 2 ** attempt))
    raise RuntimeError(f"Gemini indisponible ({last})")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--max", type=int, default=3000)
    args = ap.parse_args()
    if not GOOGLE_AI_API_KEY:
        sys.exit("GOOGLE_AI_API_KEY requis")

    rayons = sb("GET", "rayons?select=slug,label_en&order=position")
    todo, offset = [], 0
    while len(todo) < args.max:                      # l'API renvoie au plus 1 000 lignes par requête
        page = sb("GET", f"products_ean?select=market,ean,title,best_merchant&rayon=eq.autres&offers_count=gt.0"
                         f"&rayon_ai_at=is.null&order=market,ean&limit=1000&offset={offset}")
        todo += page
        if len(page) < 1000:
            break
        offset += 1000
    todo = todo[: args.max]
    print(f"{len(todo)} produits en « autres » à classer")

    def run(batch: list[dict]) -> tuple[int, int]:
        try:
            got = classify(batch, rayons)
        except RuntimeError as e:          # lot sauté, non daté : repris au prochain passage
            print(f"  lot ignoré : {e}")
            return 0, 0
        updates = defaultdict(lambda: defaultdict(list))   # rayon → market → [ean] (« autres » : gardé, marqué vu)
        for j, p in enumerate(batch):
            updates[got.get(j, "autres")][p["market"]].append(p["ean"])
        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        for rayon, by_market in updates.items():
            for market, eans in by_market.items():
                for k in range(0, len(eans), 100):
                    sb("PATCH", f"products_ean?market=eq.{market}&ean=in.({','.join(eans[k:k + 100])})",
                       {"rayon": rayon, "rayon_ai_at": now})
        kept = sum(len(v) for v in updates.get("autres", {}).values())
        return len(batch) - kept, kept

    done = kept = 0
    t0 = time.monotonic()
    batches = [todo[i:i + BATCH] for i in range(0, len(todo), BATCH)]
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        for n, (d, k) in enumerate(pool.map(run, batches), 1):
            done, kept = done + d, kept + k
            if n % 10 == 0:
                print(f"  {n}/{len(batches)} lots, {done} classés, {kept} laissés en « autres » ({time.monotonic() - t0:.0f} s)")
    print(f"Classés par Gemini : {done} ; laissés en « autres » : {kept} ; {time.monotonic() - t0:.0f} s")
    if todo:
        sb("POST", "rpc/refresh_site_stats", {})        # les compteurs par rayon suivent le nouveau classement


if __name__ == "__main__":
    main()
