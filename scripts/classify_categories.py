#!/usr/bin/env python3
"""
scripts/classify_categories.py — Rayon de chaque catégorie de boutique, décidé une fois par Gemini
==================================================================================================

Les règles par mots-clés (table rayon_rules) se trompaient sur des catégories entières (18/11 : « Entretien
et soin de la maison » → beauté, « chair » → beauté via « hair », « portable » → maison via « table »).
Chaque catégorie distincte encore inconnue est envoyée à Gemini avec quatre titres d'exemple (lots de 200) ;
la réponse est gardée dans category_rayons, puis appliquée aux produits (fonction apply_category_rayons).
Le relevé utilise ensuite cette table avant les règles. Chaque nuit, seules les nouvelles catégories partent.

Usage :
    python3 scripts/classify_categories.py              # catégories nouvelles (nuit)
    python3 scripts/classify_categories.py --dry-run    # affiche les changements sans rien écrire
"""

import re
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from classify_rayons import WORKERS, ask_gemini, sb  # noqa: E402  (charge aussi settings / .env.local)
from settings import GOOGLE_AI_API_KEY  # noqa: E402

BATCH = 200
# Modèle complet d'abord : quelques dizaines d'appels seulement (une fois par catégorie), la précision prime.
MODELS = ("gemini-3.5-flash", "gemini-3.1-flash-lite")


def descriptive(category: str) -> bool:
    """« 8 », « A12 », « 10/24 » : pas un nom de catégorie (une autre boutique peut l'employer pour autre chose)."""
    return re.search(r"[^\W\d_]{3,}", category) is not None


def classify_batch(cats: list[dict], rayons: list[dict]) -> dict[str, str]:
    out = {c["category"]: "autres" for c in cats if not descriptive(c["category"])}
    cats = [c for c in cats if descriptive(c["category"])]
    if not cats:
        return out
    deps = "\n".join(f"- {r['slug']}: {r['label_en']}" for r in rayons)
    lines = "\n".join(f"{i}\t{c['category'][:160]}\t{' | '.join(c['examples'] or [])}" for i, c in enumerate(cats))
    got = ask_gemini(
        "Each line is a product category used by an online store (id<TAB>category<TAB>example products).\n"
        "Choose the ONE shop department where ALL products of this category belong. Decide from the category"
        " name; examples only clarify its meaning. Answer 'autres' when the category is not descriptive (a code,"
        " 'sale', 'new', 'all products', a brand alone), when it groups several departments (e.g. 'Computer &"
        " Office', 'Leisure & Hobby', 'Gifts', 'Outdoor'), or when no department fits well. A wrong department"
        " is worse than 'autres'.\n"
        f"Departments:\n{deps}\nCategories:\n{lines}",
        [r["slug"] for r in rayons], MODELS)
    out.update({c["category"]: got.get(i, "autres") for i, c in enumerate(cats)})
    return out


def main():
    dry = "--dry-run" in sys.argv
    if not GOOGLE_AI_API_KEY:
        sys.exit("GOOGLE_AI_API_KEY requis")
    rayons = sb("GET", "rayons?select=slug,label_en&order=position")
    total, t0, seen = Counter(), time.monotonic(), set()
    while True:
        todo = [c for c in sb("POST", "rpc/unmapped_categories", {"p_limit": 1000}) or [] if c["category"] not in seen]
        if not todo:
            break
        seen |= {c["category"] for c in todo}
        batches = [todo[i:i + BATCH] for i in range(0, len(todo), BATCH)]

        def run(batch):
            try:
                return classify_batch(batch, rayons)
            except RuntimeError as e:          # lot sauté : repris au prochain passage
                print(f"  lot ignoré : {e}")
                return {}
        with ThreadPoolExecutor(max_workers=WORKERS) as pool:
            for mapping in pool.map(run, batches):
                total.update(mapping.values())
                if mapping and not dry:
                    sb("POST", "category_rayons?on_conflict=category",
                       [{"category": c, "rayon": r} for c, r in mapping.items()])
        print(f"  {len(seen)} catégories classées ({time.monotonic() - t0:.0f} s)")
        if dry:
            break
    print(f"Catégories classées : {sum(total.values())} ; « autres » (classées produit par produit) : {total['autres']}")
    if not dry and total:
        moved = 0
        while True:                                 # par lots : la 1re application (60 000 lignes) dépassait le délai
            n = sb("POST", "rpc/apply_category_rayons", {"p_limit": 10000}) or 0
            moved += n
            if n < 10000:
                break
        print(f"Produits changés de rayon : {moved}")
        sb("POST", "rpc/refresh_site_stats", {})


if __name__ == "__main__":
    main()
