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
from classify_rayons import WORKERS, ask_gemini, refresh_stats, sb  # noqa: E402  (charge aussi settings / .env.local)
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


def classify_sous_batch(cats: list[dict], labels: dict[str, str]) -> dict[str, str]:
    """Catégories → « rayon/sous-rayon » (ou « autres »). Peut corriger le rayon (rollers rangés en mode, 21/11)."""
    out = {c["category"]: "autres" for c in cats if not descriptive(c["category"])}    # « 8 », « 1611 » : codes propres à une boutique
    cats = [c for c in cats if descriptive(c["category"])]
    if not cats:
        return out
    lines = "\n".join(f"{i}\t{c['category'][:160]}\t{' | '.join(c['examples'] or [])}" for i, c in enumerate(cats))
    opts = "\n".join(f"- {code}: {label}" for code, label in labels.items())
    got = ask_gemini(
        "Each line is a product category used by an online store (id<TAB>category<TAB>example products).\n"
        "Choose the ONE department/sub-department where ALL products of this category belong. Decide from the"
        " category name; examples only clarify its meaning (e.g. roller skates and ski helmets are sport, not"
        " fashion; a laptop sleeve is a computer accessory). When the department is clear, choose its closest"
        " sub-department (e.g. 'Men's Outerwear' → mode/vetements-homme, 'General Clothing' or 'T-Shirts' →"
        " mode/vetements). Answer 'autres' only when the category is not descriptive (a brand, 'sale', 'new'),"
        " groups several departments (e.g. 'Apparel & Accessories', 'Gifts', 'Home & Garden'), or nothing fits.\n"
        f"Options:\n{opts}\nCategories:\n{lines}",
        list(labels) + ["autres"], MODELS)
    out.update({c["category"]: got.get(i, "autres") for i, c in enumerate(cats)})
    return out


def classify_sous(dry: bool) -> Counter:
    """Sous-rayon des catégories qui n'en ont pas encore (une fois par catégorie), puis application par lots."""
    from sous_rayons import TAXONOMY
    rayon_en = {r["slug"]: r["label_en"] for r in sb("GET", "rayons?select=slug,label_en")}
    labels = {f"{r}/{s[0]}": f"{rayon_en.get(r, r)} > {s[2]}" for r, subs in TAXONOMY.items() for s in subs}
    total, seen, t0 = Counter(), set(), time.monotonic()
    while True:
        todo = [c for c in sb("POST", "rpc/unsub_categories", {"p_limit": 1000}) or [] if c["category"] not in seen]
        if not todo:
            break
        seen |= {c["category"] for c in todo}
        batches = [todo[i:i + BATCH] for i in range(0, len(todo), BATCH)]

        def run(batch):
            try:
                return classify_sous_batch(batch, labels)
            except RuntimeError as e:          # lot sauté : repris au prochain passage
                print(f"  lot ignoré : {e}")
                return {}
        with ThreadPoolExecutor(max_workers=WORKERS) as pool:
            for batch, mapping in zip(batches, pool.map(run, batches)):
                total.update(v.split("/")[0] for v in mapping.values())
                if mapping and not dry:
                    old = {c["category"]: c["rayon"] for c in batch}
                    sb("POST", "category_rayons?on_conflict=category", [
                        {"category": c, "rayon": v.split("/")[0], "sous_rayon": v.split("/")[1]} if v != "autres"
                        else {"category": c, "rayon": old[c], "sous_rayon": ""} for c, v in mapping.items()])
        print(f"  {len(seen)} catégories : sous-rayon décidé ({time.monotonic() - t0:.0f} s)")
        if dry:
            break
    return total


def apply_sous(max_rows: int = 20_000) -> int:
    """Produits mis à jour par lots de 2 000 espacés, au plus max_rows par appel (gonflement de la base)."""
    moved = 0
    while moved < max_rows:
        n = sb("POST", "rpc/apply_category_sous_rayons", {"p_limit": 2000}) or 0
        moved += n
        if n < 2000:
            break
        time.sleep(1)
    return moved


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
    if not dry:
        sous = classify_sous(dry)
        if sous:
            print(f"Sous-rayons : {sum(sous.values())} catégories ({dict(sous.most_common(5))}…)")
        print(f"Produits classés en sous-rayon : {apply_sous()}")
        refresh_stats("refresh_site_stats", "refresh_sous_rayon_stats")


if __name__ == "__main__":
    main()
