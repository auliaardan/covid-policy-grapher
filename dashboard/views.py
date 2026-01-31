from __future__ import annotations

from collections import defaultdict
from datetime import date
from statistics import mean
from typing import Any, Dict, Iterable, List, Optional

from django.http import JsonResponse
from django.shortcuts import render
from django.views.decorators.http import require_GET

from .models import Country, DailyMetric, PolicyDaily, PolicyEvent


# --- Policy metadata (labels + level meanings) ------------------------------

POLICY_LABELS: Dict[str, str] = {
    "c1_school_closing": "School closing",
    "c2_workplace_closing": "Workplace closing",
    "c6_stay_at_home": "Stay-at-home requirements",
    "c8_international_travel_controls": "International travel controls",
    "h6_facial_coverings": "Facial coverings",
}

# Common OxCGRT ordinal definitions (kept short for UI)
POLICY_LEVELS: Dict[str, Dict[str, str]] = {
    "c1_school_closing": {
        "0": "no measures",
        "1": "recommend closing",
        "2": "require closing (some levels)",
        "3": "require closing (all levels)",
    },
    "c2_workplace_closing": {
        "0": "no measures",
        "1": "recommend closing / WFH",
        "2": "require closing (some sectors)",
        "3": "require closing (all but essential)",
    },
    "c6_stay_at_home": {
        "0": "no measures",
        "1": "recommend not leaving home",
        "2": "require not leaving home (exceptions)",
        "3": "require not leaving home (minimal exceptions)",
    },
    "c8_international_travel_controls": {
        "0": "no restrictions",
        "1": "screening",
        "2": "quarantine arrivals (some)",
        "3": "ban arrivals (some regions)",
        "4": "ban arrivals (all regions / total)",
    },
    "h6_facial_coverings": {
        "0": "no policy",
        "1": "recommended",
        "2": "required (some spaces)",
        "3": "required (most spaces)",
        "4": "required outside home at all times",
    },
}

STRINGENCY_EXPLAINER = (
    "Stringency Index is a composite indicator (0–100) summarising the strictness "
    "of government responses (e.g., school/workplace closures, travel bans). "
    "It is descriptive (policy intensity), not a direct measure of enforcement or effectiveness."
)


def _iso(d: date) -> str:
    return d.isoformat()


def index(request):
    return render(request, "dashboard/index.html")


@require_GET
def api_countries(request):
    # Only countries that actually have OWID metrics (so charts won't be empty)
    countries = (
        Country.objects.filter(metrics__isnull=False)
        .distinct()
        .order_by("name")
        .values("iso_code", "name")
    )
    return JsonResponse({"countries": list(countries)})


@require_GET
def api_timeseries(request, iso_code: str):
    try:
        country = Country.objects.get(iso_code=iso_code)
    except Country.DoesNotExist:
        return JsonResponse({"error": "Country not found"}, status=404)

    # --- Metrics (OWID) ---
    metric_rows = list(
        DailyMetric.objects.filter(country=country)
        .order_by("date")
        .values(
            "date",
            "new_cases_smoothed",
            "new_deaths_smoothed",
            "cases_per_million",
            "deaths_per_million",
            "people_fully_vaccinated_per_hundred",
            "total_vaccinations_per_hundred",
        )
    )

    dates: List[str] = [_iso(r["date"]) for r in metric_rows]

    def arr(key: str) -> List[Optional[float]]:
        return [r.get(key) for r in metric_rows]

    series: Dict[str, List[Optional[float]]] = {
        "cases": arr("new_cases_smoothed"),
        "deaths": arr("new_deaths_smoothed"),
        "cases_pm": arr("cases_per_million"),
        "deaths_pm": arr("deaths_per_million"),
        "vax_full": arr("people_fully_vaccinated_per_hundred"),
        "vax_total": arr("total_vaccinations_per_hundred"),
    }

    # --- Policies (OxCGRT) ---
    policy_rows = list(
        PolicyDaily.objects.filter(country=country)
        .order_by("date")
        .values(
            "date",
            "stringency_index",
            "c1_school_closing",
            "c2_workplace_closing",
            "c6_stay_at_home",
            "c8_international_travel_controls",
            "h6_facial_coverings",
        )
    )
    pmap: Dict[str, Dict[str, Any]] = {_iso(r["date"]): r for r in policy_rows}

    def p_arr(key: str) -> List[Optional[float]]:
        out: List[Optional[float]] = []
        for d in dates:
            r = pmap.get(d)
            out.append(r.get(key) if r else None)
        return out

    series.update(
        {
            "stringency": p_arr("stringency_index"),
            "school": p_arr("c1_school_closing"),
            "work": p_arr("c2_workplace_closing"),
            "stayhome": p_arr("c6_stay_at_home"),
            "travel": p_arr("c8_international_travel_controls"),
            "masks": p_arr("h6_facial_coverings"),
        }
    )

    # --- Events (policy diffs from PolicyEvent table) ---
    events_qs = PolicyEvent.objects.filter(country=country).order_by("date").values("date", "text")
    events_by_date: Dict[str, List[str]] = defaultdict(list)
    for e in events_qs:
        events_by_date[_iso(e["date"])].append(e["text"])

    events_payload = [{"date": d, "items": events_by_date.get(d, [])} for d in dates]

    # --- Highlights (auto pause points) ---
    highlights: List[Dict[str, Any]] = []

    # 1) Policy highlights: big stringency shifts or any ordinal indicator changes
    prev_p: Optional[Dict[str, Any]] = None
    for d in dates:
        cur = pmap.get(d)
        if not cur:
            prev_p = cur
            continue

        if prev_p:
            s0, s1 = prev_p.get("stringency_index"), cur.get("stringency_index")
            if s0 is not None and s1 is not None and abs(float(s1) - float(s0)) >= 10:
                direction = "tightened" if float(s1) > float(s0) else "relaxed"
                highlights.append(
                    {
                        "date": d,
                        "type": "policy",
                        "title": f"Policy stringency {direction}",
                        "details": [f"Stringency: {float(s0):.1f} → {float(s1):.1f} (Δ {float(s1)-float(s0):+.1f})"],
                    }
                )

            for key, label in POLICY_LABELS.items():
                a, b = prev_p.get(key), cur.get(key)
                if a != b and (a is not None or b is not None):
                    highlights.append(
                        {
                            "date": d,
                            "type": "policy",
                            "title": f"{label} changed",
                            "details": [f"{label}: {a} → {b}"],
                        }
                    )

        prev_p = cur

    # 2) Case/death acceleration highlights (7d avg vs previous 7d avg)
    def _add_accel(series_key: str, h_type: str, title_prefix: str, min_baseline: float):
        vals = series.get(series_key, [])
        if not vals:
            return
        for i in range(14, len(vals)):
            cur_window = [v for v in vals[i - 6 : i + 1] if v is not None]
            prev_window = [v for v in vals[i - 13 : i - 6] if v is not None]
            if len(cur_window) < 5 or len(prev_window) < 5:
                continue
            cur_avg = mean(cur_window)
            prev_avg = mean(prev_window)
            if prev_avg < min_baseline:
                continue
            ratio = (cur_avg / prev_avg) if prev_avg else 0
            if ratio >= 1.5:
                highlights.append(
                    {
                        "date": dates[i],
                        "type": h_type,
                        "title": f"{title_prefix} accelerating",
                        "details": [f"7d avg rose {ratio:.2f}× vs prior week ({prev_avg:.1f} → {cur_avg:.1f})."],
                    }
                )

    _add_accel("cases", "cases", "Cases", min_baseline=50)
    _add_accel("deaths", "deaths", "Deaths", min_baseline=2)

    # 3) Vaccination milestones (crossing common thresholds)
    vax = series.get("vax_full", [])
    thresholds = [10, 25, 50, 70, 80]
    seen_thr = set()
    for i in range(1, len(vax)):
        a, b = vax[i - 1], vax[i]
        if a is None or b is None:
            continue
        for thr in thresholds:
            if thr in seen_thr:
                continue
            if float(a) < thr <= float(b):
                seen_thr.add(thr)
                highlights.append(
                    {
                        "date": dates[i],
                        "type": "vax",
                        "title": f"Vaccination milestone: {thr}% fully vaccinated",
                        "details": [f"People fully vaccinated reached ≥{thr}%."],
                    }
                )

    # De-duplicate highlights that might repeat on the same day
    dedup = {}
    for h in highlights:
        key = (h["date"], h["type"], h["title"])
        if key in dedup:
            continue
        dedup[key] = h
    highlights = list(dedup.values())
    highlights.sort(key=lambda x: x["date"])

    meta = {
        "policy_labels": POLICY_LABELS,
        "policy_levels": POLICY_LEVELS,
        "stringency_explainer": STRINGENCY_EXPLAINER,
    }

    return JsonResponse(
        {
            "country": {"iso_code": country.iso_code, "name": country.name, "population": country.population},
            "dates": dates,
            "series": series,
            "events": events_payload,
            "highlights": highlights,
            "meta": meta,
        }
    )
