#!/usr/bin/env python3
"""
scripts/classify_sous_titles.py — Sous-rayon des produits d'après leur titre (Gemini Flash Lite)
================================================================================================

Les produits dont la catégorie de boutique a reçu un sous-rayon le prennent par la table category_rayons
(scripts/classify_categories.py). Restent ceux sans catégorie utilisable (aucune, code numérique, catégorie trop
large) : 72 000 le 21/11. Ils sont envoyés ici par rayon, lots de 600 titres, réponse JSON contrainte aux
sous-rayons de ce rayon. Un produit que Gemini ne sait pas classer reçoit '' (examiné, jamais renvoyé).

Usage :
    python3 scripts/classify_sous_titles.py              # jusqu'à 20 000 produits (nuit)
    python3 scripts/classify_sous_titles.py --max 80000  # rattrapage
"""

import argparse
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from classify_rayons import BATCH, MODELS, WORKERS, ask_gemini, sb  # noqa: E402  (charge aussi settings / .env.local)
from settings import GOOGLE_AI_API_KEY  # noqa: E402
from sous_rayons import TAXONOMY  # noqa: E402

NONE = "aucun"


def classify(rayon: str, batch: list[dict]) -> dict[int, str]:
    subs = TAXONOMY[rayon]
    opts = "\n".join(f"- {s[0]}: {s[2]}" for s in subs)
    lines = "\n".join(f"{i}\t{p['title'][:140]}" for i, p in enumerate(batch))
    return ask_gemini(
        f"Each line is a product (id<TAB>title) from the '{rayon}' department of a price comparison site.\n"
        f"Choose its ONE sub-department. Answer '{NONE}' when the title is too vague or no sub-department fits;"
        " a wrong sub-department is worse than none.\n"
        f"Sub-departments:\n{opts}\nProducts:\n{lines}",
        [s[0] for s in subs] + [NONE], MODELS)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--max", type=int, default=20_000)
    args = ap.parse_args()
    if not GOOGLE_AI_API_KEY:
        sys.exit("GOOGLE_AI_API_KEY requis")
    jobs: list[tuple[str, list[dict]]] = []
    total = 0
    for rayon in TAXONOMY:
        if total >= args.max:
            break
        todo, last = [], ""
        while total + len(todo) < args.max:          # pagination par clé (marché, EAN)
            page = sb("GET", f"products_ean?select=market,ean,title&rayon=eq.{rayon}&sous_rayon=is.null&offers_count=gt.0"
                             f"&ean=gt.{last}&order=ean&limit=1000") or []
            todo += page
            if len(page) < 1000:
                break
            last = page[-1]["ean"]
        todo = todo[: args.max - total]
        total += len(todo)
        jobs += [(rayon, todo[i:i + BATCH]) for i in range(0, len(todo), BATCH)]
    print(f"{total} produits sans sous-rayon à classer par leur titre ({len(jobs)} lots)")

    def run(job: tuple[str, list[dict]]) -> tuple[int, int]:
        rayon, batch = job
        try:
            got = classify(rayon, batch)
        except RuntimeError as e:          # lot sauté : repris au prochain passage
            print(f"  lot ignoré ({rayon}) : {e}")
            return 0, 0
        updates = defaultdict(lambda: defaultdict(list))   # sous-rayon → marché → [ean]
        for j, p in enumerate(batch):
            sous = got.get(j, NONE)
            updates["" if sous == NONE else sous][p["market"]].append(p["ean"])
        for sous, by_market in updates.items():
            for market, eans in by_market.items():
                for k in range(0, len(eans), 100):
                    sb("PATCH", f"products_ean?market=eq.{market}&ean=in.({','.join(eans[k:k + 100])})", {"sous_rayon": sous})
        none = sum(len(v) for v in updates.get("", {}).values())
        return len(batch) - none, none

    done = none = 0
    t0 = time.monotonic()
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        for n, (d, k) in enumerate(pool.map(run, jobs), 1):
            done, none = done + d, none + k
            if n % 10 == 0:
                print(f"  {n}/{len(jobs)} lots, {done} classés, {none} sans sous-rayon ({time.monotonic() - t0:.0f} s)")
    print(f"Classés par leur titre : {done} ; sans sous-rayon : {none} ; {time.monotonic() - t0:.0f} s")
    if total:
        sb("POST", "rpc/refresh_sous_rayon_stats", {})


if __name__ == "__main__":
    main()
