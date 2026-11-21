#!/usr/bin/env python3
"""
scripts/classify_sous_titles.py — Rayon et sous-rayon des produits d'après leur titre (Gemini Flash Lite)
=========================================================================================================

Les produits dont la catégorie de boutique a reçu un sous-rayon le prennent par la table category_rayons
(scripts/classify_categories.py). Les autres (aucune catégorie, code numérique, catégorie trop large) prenaient
le rayon par défaut de leur boutique : House of Rugby (« maison ») mettait ses maillots en décoration (22/11).
Ils sont envoyés ici par lots de 600 titres ; Gemini choisit le rayon ET le sous-rayon parmi les 115 (réponse JSON
contrainte). Indécis : rayon inchangé, sous-rayon ''. Chaque produit traité est daté (rayon_ai_at) : jamais renvoyé.

Usage :
    python3 scripts/classify_sous_titles.py                    # jusqu'à 20 000 produits nouveaux (nuit)
    python3 scripts/classify_sous_titles.py --recheck --max 60000   # reprise des produits classés avant le 22/11
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

NONE = "autres"


def options() -> dict[str, str]:
    rayon_en = {r["slug"]: r["label_en"] for r in sb("GET", "rayons?select=slug,label_en")}
    return {f"{r}/{s[0]}": f"{rayon_en.get(r, r)} > {s[2]}" for r, subs in TAXONOMY.items() for s in subs}


def classify(batch: list[dict], opts: dict[str, str]) -> dict[int, str]:
    listing = "\n".join(f"- {code}: {label}" for code, label in opts.items())
    lines = "\n".join(f"{i}\t{p['title'][:140]}" for i, p in enumerate(batch))
    return ask_gemini(
        "Each line is a product (id<TAB>title) from a price comparison site. Choose its ONE department/sub-department"
        f" from the list. Answer '{NONE}' when the title is too vague or nothing fits; a wrong answer is worse than"
        f" '{NONE}'. Rugby shirts and fishing lures are sport, not computers or home decor.\n"
        f"Options:\n{listing}\nProducts:\n{lines}",
        list(opts) + [NONE], MODELS)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--max", type=int, default=20_000)
    ap.add_argument("--recheck", action="store_true", help="aussi les produits classés avant le 22/11 (rayon_ai_at vide)")
    args = ap.parse_args()
    if not GOOGLE_AI_API_KEY:
        sys.exit("GOOGLE_AI_API_KEY requis")
    opts = options()
    todo = (sb("POST", "rpc/title_candidates", {"p_limit": args.max, "p_recheck": args.recheck}) or [])[: args.max]
    jobs = [todo[i:i + BATCH] for i in range(0, len(todo), BATCH)]
    print(f"{len(todo)} produits à classer d'après leur titre ({len(jobs)} lots)")

    def run(batch: list[dict]) -> tuple[int, int, int]:
        try:
            got = classify(batch, opts)
        except RuntimeError as e:          # lot sauté, non daté : repris au prochain passage
            print(f"  lot ignoré : {e}")
            return 0, 0, 0
        updates = defaultdict(lambda: defaultdict(list))   # (rayon, sous-rayon) → marché → [ean]
        moved = 0
        for j, p in enumerate(batch):
            code = got.get(j, NONE)
            rayon, sous = code.split("/") if code != NONE else (p["rayon"], "")
            moved += rayon != p["rayon"]
            updates[(rayon, sous)][p["market"]].append(p["ean"])
        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        for (rayon, sous), by_market in updates.items():
            for market, eans in by_market.items():
                for k in range(0, len(eans), 100):
                    sb("PATCH", f"products_ean?market=eq.{market}&ean=in.({','.join(eans[k:k + 100])})",
                       {"rayon": rayon, "sous_rayon": sous, "rayon_ai_at": now})
        none = sum(len(v) for (_, s), v2 in updates.items() if s == "" for v in v2.values())
        return len(batch) - none, none, moved

    done = none = moved = 0
    t0 = time.monotonic()
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        for n, (d, k, m) in enumerate(pool.map(run, jobs), 1):
            done, none, moved = done + d, none + k, moved + m
            if n % 10 == 0:
                print(f"  {n}/{len(jobs)} lots : {done} classés ({moved} changés de rayon), {none} indécis ({time.monotonic() - t0:.0f} s)")
    print(f"Classés par leur titre : {done} ({moved} changés de rayon) ; indécis : {none} ; {time.monotonic() - t0:.0f} s")
    if todo:
        sb("POST", "rpc/refresh_site_stats", {})
        sb("POST", "rpc/refresh_sous_rayon_stats", {})


if __name__ == "__main__":
    main()
