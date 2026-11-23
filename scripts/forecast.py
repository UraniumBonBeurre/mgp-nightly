#!/usr/bin/env python3
"""
scripts/forecast.py — Prévision des revenus d'affiliation, jour par jour (Monte-Carlo)
======================================================================================

Chaque nuit (après la synchronisation des ventes Awin), simule SIMS trajectoires des HORIZON prochains jours :

  visites/jour   = niveau actuel × croissance qui ralentit. Taux initial : moyenne pondérée (par la précision)
                   de la pente des visites (10 jours) et de celle des affichages Google (21 jours, plus stable) ;
                   amorti avec une constante de temps tirée au sort (un site neuf ne double pas indéfiniment)
  clics/jour     ~ Poisson(visites × taux de clic)          taux de clic : loi Beta d'après les 14 derniers jours
  ventes/jour    ~ Poisson(clics × Σ part_boutique × conversion_boutique × saison)
                   + ventes encore possibles des clics passés (cookie de 30 jours)
  commission     par vente : EPC ÷ conversion de la boutique (Awin, tous éditeurs), loi log-normale ; en euros.
                 23/11 : mêlée à la commission des produits réellement cliqués (prix hors TVA × taux de la boutique
                 pour le rayon, scripts/awin_commissions.py), d'autant plus que la boutique a de clics
  effet du prix  un produit plus cher que le panier moyen de la boutique se vend moins souvent (élasticité 0,3)
  trafic         saison des visites (semaine du Black Friday, Noël) en plus de celle de la conversion
  validées       commission × taux de validation de la boutique

Part des clics par boutique : clics observés + a priori proportionnel aux offres du catalogue (marchés visités).
Saison : Black Friday, Cyber Monday, Noël, soldes d'hiver (hypothèses, SEASON). Le résultat (percentiles par jour,
probabilité de la première vente, hypothèses) est écrit dans dashboard_cache (« forecast ») et affiché par le
Worker sur /stats/prevision. Fonctions de calcul pures, testées dans tests/test_forecast.py.

Usage : python3 scripts/forecast.py [--dry-run] [--sims 2000]
"""

import argparse
import json
import math
import random
import sys
import time
import urllib.request
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))
from classify_rayons import sb  # noqa: E402  (charge aussi settings / .env.local)
from settings import AWIN_API_TOKEN, AWIN_PUBLISHER_ID  # noqa: E402

HORIZON = 90
SIMS = 2000
LAG = [0.60, 0.15] + [0.25 / 28] * 28          # jours entre le clic et l'achat (cookie de 30 jours)
PRIOR_WEIGHT = 20                               # poids (en clics) de l'a priori « offres du catalogue »
GROWTH_MAX = 0.30                               # croissance quotidienne initiale au plus (≈ ×1,35 par jour)
# Incertitude sur les hypothèses elles-mêmes (23/11 : la fourchette ne mesurait que le hasard des ventes) :
GROWTH_MIN = -0.03                              # le trafic peut aussi baisser (Google qui n'indexe plus, IA qui ne cite plus)
STALL_P, STALL_TAU = 0.25, (2.0, 6.0)           # 1 chance sur 4 : la croissance s'arrête en quelques jours
QUALITY_SIGMA = 0.8                             # conversion de NOS visiteurs vs moyenne Awin : ×0,27 à ×3,7 (90 %)
TRACKING = (0.75, 0.95)                         # part des ventes attribuées (bloqueurs, Safari, cookies refusés)
TRACKING_MID = sum(TRACKING) / 2
TAU_MEDIAN, TAU_SIGMA = 14.0, 0.5               # constante d'amortissement de la croissance (jours)
SESSIONS_CAP = 5_000                            # visites par jour au plus
SALE_SIGMA = 1.0                                # dispersion de la commission d'une vente (log-normale)
KPI_MAX_AGE_DAYS = 7
KPI_CALLS_MAX = 40                              # appels Awin par nuit (limite : 20 par minute)
DEFAULT_KPI = {"epc": 0.15, "conversion_rate": 2.0, "approval_pct": 80.0, "currency": "EUR"}
# Indicateurs Awin des petits programmes calculés sur une poignée de clics (22/11 : 60 $ par clic, 50 % de
# conversion) : bornés à des valeurs plausibles pour du commerce en ligne.
CONV_MIN, CONV_MAX = 0.1, 10.0                  # conversion, en %
EPC_MAX_EUR = 1.5
# Produits cliqués (23/11) : commission estimée d'après le prix et le taux du rayon, mêlée à la moyenne Awin
CLICK_PRIOR = 5                                 # poids (en clics) de la moyenne Awin face aux produits cliqués
PRICE_ELASTICITY = 0.3                          # conversion × (panier moyen / prix)^0,3 : un produit 4× plus cher → ×0,66
PRICE_FACTOR = (0.5, 1.5)
VAT = {"fr": 0.2, "en": 0.2, "uk": 0.2, "de": 0.19, "it": 0.22, "es": 0.21, "us": 0.0}
MARKET_CURRENCY = {"uk": "GBP", "us": "USD"}


def traffic_season(d: date) -> float:
    """Multiplicateur des visites selon la période (recherches de bons plans et de cadeaux ; hypothèses prudentes)."""
    md = (d.month, d.day)
    if md in ((11, 27), (11, 30)):
        return 1.5                                   # Black Friday, Cyber Monday
    if (11, 24) <= md <= (12, 1):
        return 1.25
    if (12, 2) <= md <= (12, 20):
        return 1.1
    if (12, 24) <= md <= (12, 26):
        return 0.8
    return 1.0


def click_adjust(conv: float, per_sale_awin: float, products: list[tuple[float, float]], avg_pct: float | None) -> tuple[float, float, dict]:
    """Conversion et commission par vente d'une boutique, corrigées par les produits réellement cliqués.
    products : [(prix hors TVA en euros, taux en %)] ; avg_pct : taux moyen de la boutique (panier moyen Awin =
    commission moyenne ÷ taux). Sans clic : valeurs Awin inchangées."""
    n = len(products)
    if not n:
        return conv, per_sale_awin, {}
    own = sum(p * r / 100 for p, r in products) / n
    w = n / (n + CLICK_PRIOR)
    per = w * own + (1 - w) * per_sale_awin
    factor = 1.0
    if avg_pct and avg_pct > 0:
        basket = per_sale_awin / (avg_pct / 100)                     # panier moyen des acheteurs Awin
        price = sum(p for p, _ in products) / n
        factor = min(PRICE_FACTOR[1], max(PRICE_FACTOR[0], (basket / price) ** PRICE_ELASTICITY)) if price > 0 else 1.0
    return conv * (w * factor + (1 - w)), per, {"own_eur": round(own, 2), "clicks": n, "price_factor": round(factor, 2),
                                                 "basket_eur": round(per_sale_awin / (avg_pct / 100), 0) if avg_pct else None}


def season(d: date) -> float:
    """Multiplicateur de conversion selon la période (hypothèses ; 1 = normal)."""
    md = (d.month, d.day)
    if md == (11, 27):
        return 2.0                                   # Black Friday
    if md == (11, 30):
        return 1.6                                   # Cyber Monday
    if (11, 24) <= md <= (12, 1):
        return 1.4                                   # semaine du Black Friday
    if (12, 2) <= md <= (12, 20):
        return 1.25                                  # achats de Noël
    if (12, 25) <= md <= (12, 31):
        return 0.8
    if d.month == 1 and 7 <= d.day <= 31:
        return 1.1                                   # soldes d'hiver
    return 1.0


# ── Fonctions pures ─────────────────────────────────────────────────────────────────────────────────────

def fit_growth(series: list[float]) -> tuple[float, float]:
    """Taux de croissance quotidien (pente de log(visites+1), moindres carrés) et son écart type."""
    ys = [math.log(v + 1) for v in series]
    n = len(ys)
    if n < 3:
        return 0.0, 0.1
    xs = list(range(n))
    mx, my = sum(xs) / n, sum(ys) / n
    sxx = sum((x - mx) ** 2 for x in xs)
    slope = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / sxx
    resid = sum((y - (my + slope * (x - mx))) ** 2 for x, y in zip(xs, ys))
    se = math.sqrt(resid / max(n - 2, 1) / sxx)
    return slope, se


def combine(estimates: list[tuple[float, float]]) -> tuple[float, float]:
    """Moyenne de plusieurs estimations (valeur, écart type) pondérée par leur précision."""
    w = [(v, 1 / max(se, 1e-3) ** 2) for v, se in estimates]
    tw = sum(x for _, x in w)
    return sum(v * x for v, x in w) / tw, math.sqrt(1 / tw)


def sessions_path(s0: float, g0: float, tau: float, days: int) -> list[float]:
    """Visites prévues jour par jour : croissance initiale g0 amortie (constante tau), plafonnée."""
    return [min(SESSIONS_CAP, s0 * math.exp(g0 * tau * (1 - math.exp(-t / tau)))) for t in range(1, days + 1)]


def poisson(lam: float, rng: random.Random) -> int:
    if lam <= 0:
        return 0
    if lam > 50:                                     # approximation normale
        return max(0, int(round(rng.gauss(lam, math.sqrt(lam)))))
    k, p, limit = 0, 1.0, math.exp(-lam)
    while True:
        p *= rng.random()
        if p <= limit:
            return k
        k += 1


def merchant_mix(clicks: dict, prior: dict) -> dict:
    """Part attendue des clics par boutique : clics observés + a priori (offres), normalisé."""
    tot_prior = sum(prior.values()) or 1
    w = defaultdict(float)
    for m, v in prior.items():
        w[m] += PRIOR_WEIGHT * v / tot_prior
    for m, v in clicks.items():
        w[m] += v
    tot = sum(w.values()) or 1
    return {m: v / tot for m, v in w.items() if v > 0}


def per_sale(kpi: dict, rate_eur: float) -> tuple[float, float]:
    """(probabilité qu'un clic devienne une vente, commission moyenne d'une vente en euros)."""
    conv = min(CONV_MAX, max(CONV_MIN, float(kpi.get("conversion_rate") or DEFAULT_KPI["conversion_rate"]))) / 100
    epc = min(EPC_MAX_EUR, float(kpi.get("epc") or DEFAULT_KPI["epc"]) / rate_eur)
    return conv, epc / conv


def wpercentile(values: list[float], weights: list[float], q: float) -> float:
    """Percentile pondéré (poids des simulations selon les ventes réellement observées)."""
    pairs = sorted(zip(values, weights))
    tot = sum(weights) or 1.0
    acc = 0.0
    for v, w in pairs:
        acc += w
        if acc >= q * tot:
            return v
    return pairs[-1][0] if pairs else 0.0


def observed_weights(effs: list[float], exposure: float, sales: int) -> list[float]:
    """Poids de chaque simulation : vraisemblance (loi de Poisson) des ventes réellement observées sur les clics passés.
    exposure = ventes attendues des clics passés si les visiteurs achetaient comme la moyenne Awin ; eff = facteur
    « qualité du trafic × ventes attribuées » de la simulation. Beaucoup de clics sans vente : les simulations à faible
    conversion pèsent plus ; des ventes : l'inverse. Sans historique, tous les poids sont égaux."""
    logs = []
    for e in effs:
        lam = exposure * e
        logs.append(-lam + (sales * math.log(lam) if sales and lam > 0 else (0.0 if not sales else -1e9)))
    top = max(logs) if logs else 0.0
    return [math.exp(v - top) for v in logs]


def percentile(sorted_vals: list[float], q: float) -> float:
    if not sorted_vals:
        return 0.0
    i = min(len(sorted_vals) - 1, max(0, int(round(q * (len(sorted_vals) - 1)))))
    return sorted_vals[i]


def simulate(inputs: dict, start: date, sims: int = SIMS, horizon: int = HORIZON, seed: int = 7) -> dict:
    """inputs : s0, growth, growth_se, clicks14, sessions14, mix {m: part}, sale {m: (conv, commission €)},
    approval {m: %}, pending [(jours écoulés depuis le clic, m)]. Renvoie percentiles par jour et cumulés."""
    rng = random.Random(seed)
    mix, sale = inputs["mix"], inputs["sale"]
    weights = {m: mix[m] * sale[m][0] for m in mix}             # ventes par clic, par boutique
    conv_per_click = sum(weights.values())
    ms = list(weights)
    wsum = sum(weights.values()) or 1
    cum_w = []
    acc = 0.0
    for m in ms:
        acc += weights[m] / wsum
        cum_w.append(acc)

    def draw_merchant() -> str:
        u = rng.random()
        for m, c in zip(ms, cum_w):
            if u <= c:
                return m
        return ms[-1]

    def commission(m: str) -> float:
        mean = sale[m][1]
        return rng.lognormvariate(math.log(mean) - SALE_SIGMA ** 2 / 2, SALE_SIGMA)

    days = [start + timedelta(days=t) for t in range(1, horizon + 1)]
    seas = [season(d) for d in days]
    tseas = [traffic_season(d) for d in days]
    # Ventes encore possibles des clics passés : probabilité restante d'achat (cookie) répartie sur les jours à venir
    pend = [0.0] * horizon
    for age, m in inputs.get("pending", []):
        if m not in sale:
            continue
        for t in range(horizon):
            lag = age + t + 1
            if lag < len(LAG):
                pend[t] += sale[m][0] * LAG[lag] * seas[t]
    # Scénario central (hypothèses médianes, sans tirage) : prévision jour par jour lisible ; les simulations
    # donnent la fourchette (les rares trajectoires à très fort trafic tirent la moyenne vers le haut).
    per_sale_eur = sum(weights[m] * sale[m][1] for m in ms) / wsum
    approval_avg = sum(weights[m] * inputs["approval"].get(m, DEFAULT_KPI["approval_pct"]) for m in ms) / wsum / 100
    ctr_mean = (1 + inputs["clicks14"]) / (2 + inputs["sessions14"])
    c_sess = [v * f for v, f in zip(sessions_path(inputs["s0"], inputs["growth"], TAU_MEDIAN, horizon), tseas)]
    c_lam = [0.0] * horizon
    for t in range(horizon):
        for k, p in enumerate(LAG):
            if t + k < horizon:
                c_lam[t + k] += c_sess[t] * ctr_mean * conv_per_click * p
    c_sales = [(c_lam[t] * seas[t] + pend[t]) * TRACKING_MID for t in range(horizon)]
    daily = [[0.0] * sims for _ in range(horizon)]
    validated = [[0.0] * sims for _ in range(horizon)]
    sales_out = [[0] * sims for _ in range(horizon)]
    first_sale = []
    effs = []
    for s in range(sims):
        g0 = min(GROWTH_MAX, max(GROWTH_MIN, rng.gauss(inputs["growth"], inputs["growth_se"])))
        tau = rng.uniform(*STALL_TAU) if rng.random() < STALL_P else rng.lognormvariate(math.log(TAU_MEDIAN), TAU_SIGMA)
        eff = rng.lognormvariate(0.0, QUALITY_SIGMA) * rng.uniform(*TRACKING)   # qualité du trafic × ventes attribuées
        effs.append(eff)
        ctr = rng.betavariate(1 + inputs["clicks14"], 1 + max(0, inputs["sessions14"] - inputs["clicks14"]))
        sess = [v * f for v, f in zip(sessions_path(inputs["s0"], g0, tau, horizon), tseas)]
        lam_conv = [0.0] * (horizon + len(LAG))
        first = None
        for t in range(horizon):
            c = poisson(sess[t] * ctr, rng)
            for k, p in enumerate(LAG):                          # achats répartis sur les jours suivant le clic
                if t + k < horizon:
                    lam_conv[t + k] += c * conv_per_click * eff * p
            n = poisson(lam_conv[t] * seas[t] + pend[t] * eff, rng)
            sales_out[t][s] = n
            tot = val = 0.0
            for _ in range(n):
                m = draw_merchant()
                v = commission(m)
                tot += v
                val += v * inputs["approval"].get(m, DEFAULT_KPI["approval_pct"]) / 100
            daily[t][s] = tot
            validated[t][s] = val
            if n and first is None:
                first = t
        first_sale.append(first)
    exposure, observed_sales = inputs.get("observed", (0.0, 0))
    w = observed_weights(effs, exposure, observed_sales)
    wsum_sims = sum(w)
    share = lambda flags: round(sum(wi for wi, f in zip(w, flags) if f) / wsum_sims, 3)   # noqa: E731
    out_days, cum = [], [0.0] * sims
    cum_val = [0.0] * sims
    c_cum = 0.0
    for t in range(horizon):
        for s in range(sims):
            cum[s] += daily[t][s]
            cum_val[s] += validated[t][s]
        c_cum += c_sales[t] * per_sale_eur
        out_days.append({
            "day": days[t].isoformat(),
            "sessions": round(c_sess[t], 1), "clicks": round(c_sess[t] * ctr_mean, 1),       # scénario central
            "sales": round(c_sales[t], 3), "revenue": round(c_sales[t] * per_sale_eur, 2),
            "validated": round(c_sales[t] * per_sale_eur * approval_avg, 2), "cum": round(c_cum, 2),
            **{f"cum_p{int(q * 100)}": round(wpercentile(cum, w, q), 2) for q in (0.1, 0.25, 0.5, 0.75, 0.9)},
            "val_p50": round(wpercentile(cum_val, w, 0.5), 2),
            "p_sale_day": share(v > 0 for v in sales_out[t]),
            "p_first_sale": share(f is not None and f <= t for f in first_sale),
        })
    median_first = next((t for t, d in enumerate(out_days) if d["p_first_sale"] >= 0.5), None)
    quality_post = sum(wi * e for wi, e in zip(w, effs)) / wsum_sims / (sum(effs) / len(effs))   # 1 = comme la moyenne Awin
    return {"days": out_days, "first_sale_median": days[median_first].isoformat() if median_first is not None else None,
            "quality_post": quality_post,
            "conv_per_click": conv_per_click, "per_sale_eur": per_sale_eur, "approval_avg": approval_avg,
            "epc_eur": sum(mix[m] * sale[m][0] * sale[m][1] for m in mix)}


# ── Données ───────────────────────────────────────────────────────────────────────────────────────────

def awin_kpi(mid: int) -> dict | None:
    req = urllib.request.Request(
        f"https://api.awin.com/publishers/{AWIN_PUBLISHER_ID}/programmedetails?advertiserId={mid}&relationship=joined",
        headers={"Authorization": f"Bearer {AWIN_API_TOKEN}"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            d = json.loads(r.read())
    except Exception as e:  # noqa: BLE001
        print(f"  (KPI Awin {mid} : {str(e)[:80]})")
        return None
    k, info = d.get("kpi") or {}, d.get("programmeInfo") or {}
    return {"awin_mid": mid, "name": info.get("name"), "currency": info.get("currencyCode") or "EUR",
            "epc": k.get("epc"), "conversion_rate": k.get("conversionRate"), "approval_pct": k.get("approvalPercentage"),
            "validation_days": k.get("validationDays"), "payment_days": int(k["averagePaymentTime"]) if str(k.get("averagePaymentTime") or "").isdigit() else None,
            "updated_at": datetime.now(timezone.utc).isoformat()}


def paged(path: str) -> list[dict]:
    """Toutes les lignes (l'API renvoie au plus 1 000 lignes par requête)."""
    out, offset = [], 0
    while True:
        page = sb("GET", f"{path}&order=id&limit=1000&offset={offset}") or []
        out += page
        if len(page) < 1000:
            return out
        offset += 1000


def daily_series(since: str) -> tuple[list[dict], dict]:
    rows = paged(f"site_events?select=ts,type,session_id,merchant_key,market,ean&excluded=is.false&ts=gte.{since}&type=in.(pageview,outbound)")
    by = defaultdict(lambda: {"s": set(), "c": 0})
    clicks_by_merchant = defaultdict(list)
    for r in rows:
        d = r["ts"][:10]
        if r["type"] == "pageview" and r.get("session_id"):
            by[d]["s"].add(r["session_id"])
        elif r["type"] == "outbound":
            by[d]["c"] += 1
            if r.get("merchant_key"):
                clicks_by_merchant[r["merchant_key"]].append((d, r.get("market"), r.get("ean")))
    return [{"day": d, "sessions": len(v["s"]), "clicks": v["c"]} for d, v in sorted(by.items())], clicks_by_merchant


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--sims", type=int, default=SIMS)
    args = ap.parse_args()
    from track_prices import ecb_rates   # noqa: E402 — même source de cours que les bons plans de l'étranger
    today = datetime.now(timezone.utc).date()
    since = (today - timedelta(days=40)).isoformat()
    series, clicks_by_merchant = daily_series(since)
    complete = [r for r in series if r["day"] < today.isoformat()]          # jour en cours incomplet : écarté
    if len(complete) < 3:
        sys.exit("pas assez d'historique (3 jours complets au moins)")
    last10 = [r["sessions"] for r in complete[-10:]]
    g_visits = fit_growth(last10)
    gsc = sb("GET", f"gsc_daily?select=day,impressions&day=gte.{(today - timedelta(days=24)).isoformat()}&order=day") or []
    g_google = fit_growth([float(r["impressions"] or 0) for r in gsc][-21:])
    growth, growth_se = combine([g_visits, g_google])
    s0 = sum(r["sessions"] for r in complete[-3:]) / 3
    sessions14 = sum(r["sessions"] for r in complete[-14:])
    clicks14 = sum(r["clicks"] for r in complete[-14:])

    feeds = sb("GET", "feeds?select=merchant_key,awin_mid,market,offers&offers=gt.0&limit=2000") or []
    mid_of = {f["merchant_key"]: f["awin_mid"] for f in feeds}
    # A priori : offres par boutique, pondérées par la part des visites de chaque marché (14 jours)
    visits_by_market = defaultdict(int)
    for r in paged(f"site_events?select=market&excluded=is.false&type=eq.pageview&ts=gte.{(today - timedelta(days=14)).isoformat()}"):
        visits_by_market[{"en": "fr"}.get(r.get("market"), r.get("market"))] += 1
    tv = sum(visits_by_market.values()) or 1
    prior = defaultdict(float)
    for f in feeds:
        prior[f["awin_mid"]] += f["offers"] * visits_by_market.get(f["market"], 0) / tv
    top_prior = dict(sorted(prior.items(), key=lambda kv: -kv[1])[:60])
    clicks = defaultdict(int)
    pending = []
    for mk, days_ in clicks_by_merchant.items():
        mid = mid_of.get(mk)
        if mid is None:
            continue
        clicks[mid] += len(days_)
        for d, _, _ in days_:
            age = (today - date.fromisoformat(d)).days
            if age < len(LAG):
                pending.append((age, mid))
    mix = merchant_mix(clicks, top_prior)

    # Indicateurs Awin (cache merchant_kpi, relus au plus une fois par semaine)
    kpis = {r["awin_mid"]: r for r in sb("GET", "merchant_kpi?select=*&limit=5000") or []}
    stale = [m for m in sorted(mix, key=lambda m: -mix[m])
             if m not in kpis or kpis[m]["updated_at"] < (datetime.now(timezone.utc) - timedelta(days=KPI_MAX_AGE_DAYS)).isoformat()]
    fresh = []
    for i, m in enumerate(stale[:KPI_CALLS_MAX]):
        if i:
            time.sleep(3.2)                              # 20 appels par minute au plus
        k = awin_kpi(m)
        if k:
            kpis[m] = k
            fresh.append(k)
    if fresh and not args.dry_run:
        sb("POST", "merchant_kpi?on_conflict=awin_mid", fresh)
    rates = ecb_rates() or {"EUR": 1.0, "USD": 1.1, "GBP": 0.85}
    sale, approval, known = {}, {}, []
    for m in mix:
        k = kpis.get(m) or {}
        if k.get("epc") and k.get("conversion_rate"):
            known.append(k)
        cur = (k.get("currency") or "EUR").upper()
        sale[m] = per_sale({**DEFAULT_KPI, **{x: v for x, v in k.items() if v is not None}}, rates.get(cur, 1.0))
        approval[m] = float(k.get("approval_pct") or DEFAULT_KPI["approval_pct"])
    # Produits cliqués : prix (index du catalogue) et taux de la boutique pour leur rayon
    clicked = defaultdict(list)                                   # boutique → [(marché, produit)]
    for mk, days_ in clicks_by_merchant.items():
        if mid_of.get(mk) in sale:
            clicked[mid_of[mk]] += [(mkt, e) for _, mkt, e in days_ if mkt and e]
    keys = {(mkt, e) for v in clicked.values() for mkt, e in v}
    prod = {}
    for mkt in {k[0] for k in keys}:
        eans = sorted(e for m2, e in keys if m2 == mkt)
        for i in range(0, len(eans), 100):
            for r in sb("GET", f"products_ean?select=ean,rayon,min_price&market=eq.{mkt}&ean=in.({','.join(eans[i:i + 100])})") or []:
                prod[(mkt, r["ean"])] = r
    rr = defaultdict(dict)
    if clicked:
        for r in sb("GET", f"merchant_rayon_rates?select=awin_mid,rayon,pct&awin_mid=in.({','.join(map(str, clicked))})") or []:
            if r["pct"] is not None:
                rr[r["awin_mid"]][r["rayon"]] = float(r["pct"])
    adjust = {}
    for m, items in clicked.items():
        products = []
        for mkt, e in items:
            p = prod.get((mkt, e))
            pct = rr[m].get((p or {}).get("rayon") or "autres") if p else None
            if p and p.get("min_price") and pct:
                eur_price = float(p["min_price"]) / rates.get(MARKET_CURRENCY.get(mkt, "EUR"), 1.0)
                products.append((eur_price / (1 + VAT.get(mkt, 0.2)), pct))
        avg_pct = sum(rr[m].values()) / len(rr[m]) if rr[m] else None
        c, per, info = click_adjust(sale[m][0], sale[m][1], products, avg_pct)
        if info:
            adjust[m] = {**info, "awin_eur": round(sale[m][1], 2)}
            sale[m] = (c, per)
    conv = sb("GET", f"conversions?select=transaction_date,commission,status&transaction_date=gte.{since}&limit=5000") or []
    # Apprentissage : ventes attendues des clics passés (s'ils achetaient comme la moyenne Awin) vs ventes observées
    exposure = 0.0
    for mk, days_ in clicks_by_merchant.items():
        mid = mid_of.get(mk)
        conv_m = sale[mid][0] if mid in sale else DEFAULT_KPI["conversion_rate"] / 100
        for d, _, _ in days_:
            age = (today - date.fromisoformat(d)).days
            exposure += conv_m * min(1.0, sum(LAG[:age + 1]))
    observed_sales = sum(1 for c in conv if c.get("status") != "declined")
    inputs = {"s0": s0, "growth": growth, "growth_se": growth_se, "clicks14": clicks14, "sessions14": sessions14,
              "mix": mix, "sale": sale, "approval": approval, "pending": pending, "observed": (exposure, observed_sales)}
    t0 = time.monotonic()
    res = simulate(inputs, today - timedelta(days=1), sims=args.sims)   # premier jour prévu : aujourd'hui
    actual = defaultdict(float)
    for c in conv:
        if c.get("status") != "declined":
            actual[c["transaction_date"][:10]] += float(c.get("commission") or 0)
    names = {m: (kpis.get(m) or {}).get("name") for m in mix}
    out = {
        "generated_at": datetime.now(timezone.utc).isoformat(), "sims": args.sims, "horizon": HORIZON,
        "history": [{**r, "revenue": round(actual.get(r["day"], 0.0), 2)} for r in complete[-30:]],
        "days": res["days"], "first_sale_median": res["first_sale_median"],
        "assumptions": {
            "sessions_now": round(s0, 1), "growth_per_day": round(growth, 3), "growth_se": round(growth_se, 3),
            "growth_visits": round(g_visits[0], 3), "growth_google": round(g_google[0], 3),
            "tau_median_days": TAU_MEDIAN, "click_rate": round((clicks14 + 1) / (sessions14 + 2), 3),
            "clicks14": clicks14, "sessions14": sessions14, "conv_per_click": round(res["conv_per_click"], 4),
            "epc_eur": round(res["epc_eur"], 3), "per_sale_eur": round(res["per_sale_eur"], 2),
            "approval_avg": round(res["approval_avg"], 3), "kpi_known": len(known), "merchants": len(mix),
            "pending_clicks": len(pending), "lag_same_day": LAG[0],
            "observed_exposure": round(exposure, 2), "observed_sales": observed_sales, "quality_post": round(res["quality_post"], 2),
            "stall_p": STALL_P, "quality_sigma": QUALITY_SIGMA, "tracking": list(TRACKING), "growth_min": GROWTH_MIN,
            "products_clicked": sum(a["clicks"] for a in adjust.values()),
            "price_factor_avg": round(sum(a["price_factor"] * a["clicks"] for a in adjust.values()) / max(1, sum(a["clicks"] for a in adjust.values())), 2),
            "traffic_season": True,
            "top_merchants": [{"name": names[m] or str(m), "share": round(mix[m], 3), "conv_pct": round(sale[m][0] * 100, 2),
                               "per_sale_eur": round(sale[m][1], 2), "approval": approval[m], **adjust.get(m, {})}
                              for m in sorted(mix, key=lambda m: -mix[m])[:8]],
        },
    }
    d30, d90 = res["days"][29], res["days"][-1]
    print(f"Prévision : {s0:.1f} visites/j, croissance {growth:+.2f}/j (±{growth_se:.2f}), clic {out['assumptions']['click_rate']:.1%}, "
          f"EPC pondéré {res['epc_eur']:.2f} € ; 30 j : {d30['cum_p50']:.0f} € (P10 {d30['cum_p10']:.0f} – P90 {d30['cum_p90']:.0f}) ; "
          f"90 j : {d90['cum_p50']:.0f} € (P10 {d90['cum_p10']:.0f} – P90 {d90['cum_p90']:.0f}) ; "
          f"1re vente médiane : {res['first_sale_median']} ; {time.monotonic() - t0:.0f} s")
    if args.dry_run:
        return out
    sb("POST", "dashboard_cache?on_conflict=key", [{"key": "forecast", "value": out, "updated_at": out["generated_at"]}])
    return out


if __name__ == "__main__":
    main()
