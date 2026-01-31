from django.http import JsonResponse
from django.shortcuts import render
from django.views.decorators.http import require_GET

from .models import Country, DailyMetric, PolicyDaily, PolicyEvent


def index(request):
    return render(request, "dashboard/index.html")


@require_GET
def api_countries(request):
    countries = (
        Country.objects.filter(metrics__isnull=False)
        .distinct()
        .order_by("name")
        .values("iso_code", "name")
    )
    return JsonResponse({"countries": list(countries)})


# --- Policy metadata (plain-language explanations) ---
POLICY_LEVELS = {
    "c1_school_closing": {
        0: "No measures",
        1: "Recommend closing / altered operations",
        2: "Require closing some levels",
        3: "Require closing all levels",
    },
    "c2_workplace_closing": {
        0: "No measures",
        1: "Recommend closing / work from home",
        2: "Require closing for some sectors/categories",
        3: "Require closing for all-but-essential workplaces",
    },
    "c6_stay_at_home": {
        0: "No measures",
        1: "Recommend staying at home",
        2: "Require staying at home with exceptions",
        3: "Require staying at home with minimal exceptions",
    },
    "c8_international_travel_controls": {
        0: "No restrictions",
        1: "Screening arrivals",
        2: "Quarantine arrivals from high-risk regions",
        3: "Ban arrivals from some regions",
        4: "Ban on all regions / total border closure",
    },
    "h6_facial_coverings": {
        0: "No policy",
        1: "Recommended",
        2: "Required in some places/shared spaces",
        3: "Required in all public/shared spaces",
        4: "Required outside the home at all times (rare)",
    },
}

POLICY_LABELS = {
    "stringency_index": "Stringency (0–100)",
    "c1_school_closing": "School closing (C1)",
    "c2_workplace_closing": "Workplace closing (C2)",
    "c6_stay_at_home": "Stay-at-home (C6)",
    "c8_international_travel_controls": "International travel controls (C8)",
    "h6_facial_coverings": "Face coverings (H6)",
}

STRINGENCY_EXPLAINER = (
    "Stringency Index (0–100) describes how strict government restrictions were (composite of multiple policy indicators). "
    "It does NOT measure effectiveness, enforcement, or appropriateness."
)


def _safe_ratio(cur, prev):
    if cur is None or prev is None:
        return None
    if prev <= 0:
        return None
    return cur / prev


@require_GET
def api_timeseries(request, iso_code: str):
    try:
        country = Country.objects.get(iso_code=iso_code)
    except Country.DoesNotExist:
        return JsonResponse({"error": "Country not found"}, status=404)

    metrics = list(
        DailyMetric.objects.filter(country=country)
        .order_by("date")
        .values(
            "date",
            "new_cases_smoothed",
            "new_deaths_smoothed",
            "cases_per_million",
            "deaths_per_million",
            "total_cases",
            "total_deaths",
            "total_vaccinations_per_hundred",
            "people_vaccinated_per_hundred",
            "people_fully_vaccinated_per_hundred",
            "new_vaccinations_smoothed_per_million",
        )
    )

    policies = list(
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

    events = PolicyEvent.objects.filter(country=country).order_by("date").values("date", "text")

    pol_map = {p["date"].isoformat(): p for p in policies}
    ev_map = {}
    for e in events:
        d = e["date"].isoformat()
        ev_map.setdefault(d, []).append(e["text"])

    dates = []
    cases = []
    deaths = []
    cases_pm = []
    deaths_pm = []
    stringency = []
    school = []
    work = []
    stayhome = []
    travel = []
    masks = []
    event_cards = []
    vax_total_ph = []
    vax_one = []
    vax_full = []
    vax_daily_pm = []

    for m in metrics:
        d = m["date"].isoformat()
        dates.append(d)

        cases.append(m["new_cases_smoothed"])
        deaths.append(m["new_deaths_smoothed"])
        cases_pm.append(m["cases_per_million"])
        deaths_pm.append(m["deaths_per_million"])
        vax_total_ph.append(m.get("total_vaccinations_per_hundred"))
        vax_one.append(m.get("people_vaccinated_per_hundred"))
        vax_full.append(m.get("people_fully_vaccinated_per_hundred"))
        vax_daily_pm.append(m.get("new_vaccinations_smoothed_per_million"))

        p = pol_map.get(d)
        stringency.append(p["stringency_index"] if p else None)
        school.append(p["c1_school_closing"] if p else None)
        work.append(p["c2_workplace_closing"] if p else None)
        stayhome.append(p["c6_stay_at_home"] if p else None)
        travel.append(p["c8_international_travel_controls"] if p else None)
        masks.append(p["h6_facial_coverings"] if p else None)

        if d in ev_map:
            event_cards.append({"date": d, "items": ev_map[d]})

    # --- Build Highlights: major policy changes + rapid growth in cases/deaths ---
    highlights = []

    def policy_level_text(key, val):
        if val is None:
            return "—"
        levels = POLICY_LEVELS.get(key)
        if levels:
            return f"{val} ({levels.get(val, 'Level')})"
        return str(val)

    # Policy-based highlights
    prev_pol = None
    for i, d in enumerate(dates):
        p = pol_map.get(d)
        if not p:
            prev_pol = p
            continue

        if prev_pol:
            deltas = []
            # Major Stringency change threshold
            s_prev = prev_pol.get("stringency_index")
            s_cur = p.get("stringency_index")
            if s_prev is not None and s_cur is not None and abs(s_cur - s_prev) >= 10:
                deltas.append(f"{POLICY_LABELS['stringency_index']}: {s_prev:.1f} → {s_cur:.1f} (Δ {s_cur - s_prev:+.1f})")

            # Any change in key ordinal indicators
            for key in ["c1_school_closing", "c2_workplace_closing", "c6_stay_at_home", "c8_international_travel_controls", "h6_facial_coverings"]:
                a = prev_pol.get(key)
                b = p.get(key)
                if a != b and (a is not None or b is not None):
                    deltas.append(f"{POLICY_LABELS[key]}: {policy_level_text(key, a)} → {policy_level_text(key, b)}")

            if deltas:
                highlights.append(
                    {
                        "date": d,
                        "type": "policy",
                        "title": "Major policy change",
                        "details": deltas[:6],
                        "severity": 2 if any("Stringency" in x and "Δ" in x for x in deltas) else 1,
                    }
                )

        prev_pol = p

    # Growth-based highlights (weekly growth ratio on smoothed per-million series when available)
    # Heuristics (tunable):
    # - Cases rapid growth: ratio >= 1.5 and current >= 5 per million
    # - Deaths rapid growth: ratio >= 1.5 and current >= 0.2 per million
    for i in range(len(dates)):
        if i < 7:
            continue
        d = dates[i]

        cur_cases = cases_pm[i] if cases_pm[i] is not None else cases[i]
        prev_cases = cases_pm[i - 7] if cases_pm[i - 7] is not None else cases[i - 7]
        rc = _safe_ratio(cur_cases, prev_cases)
        if rc is not None and rc >= 1.5 and cur_cases is not None and cur_cases >= 5:
            highlights.append(
                {
                    "date": d,
                    "type": "cases",
                    "title": "Rapid growth in cases",
                    "details": [f"7-day avg increased ~{(rc - 1) * 100:.0f}% vs 7 days earlier."],
                    "severity": 2,
                }
            )

        cur_deaths = deaths_pm[i] if deaths_pm[i] is not None else deaths[i]
        prev_deaths = deaths_pm[i - 7] if deaths_pm[i - 7] is not None else deaths[i - 7]
        rd = _safe_ratio(cur_deaths, prev_deaths)
        if rd is not None and rd >= 1.5 and cur_deaths is not None and cur_deaths >= 0.2:
            highlights.append(
                {
                    "date": d,
                    "type": "deaths",
                    "title": "Rapid growth in deaths",
                    "details": [f"7-day avg increased ~{(rd - 1) * 100:.0f}% vs 7 days earlier."],
                    "severity": 3,
                }
            )
    # Vaccination milestone highlights (fully vaccinated %)
    thresholds = [10, 50, 70]
    prev_v = None
    for i, d in enumerate(dates):
        v = vax_full[i]
        if v is None:
            continue
        if prev_v is None:
            prev_v = v
            continue
        for t in thresholds:
            if prev_v < t <= v:
                highlights.append({
                    "date": d,
                    "type": "vax",
                    "title": "Vaccination milestone",
                    "details": [f"Fully vaccinated reached {t}% (per 100 people)."],
                    "severity": 1,
                })
        prev_v = v

    # De-dupe highlights by (date,type,title)
    seen = set()
    uniq = []
    for h in sorted(highlights, key=lambda x: x["date"]):
        k = (h["date"], h["type"], h["title"])
        if k in seen:
            continue
        seen.add(k)
        uniq.append(h)

    return JsonResponse(
        {
            "country": {"iso_code": country.iso_code, "name": country.name},
            "dates": dates,
            "series": {
                "cases": cases,
                "deaths": deaths,
                "cases_pm": cases_pm,
                "deaths_pm": deaths_pm,
                "stringency": stringency,
                "school": school,
                "work": work,
                "stayhome": stayhome,
                "travel": travel,
                "masks": masks,
                "vax_total_ph": vax_total_ph,
                "vax_one": vax_one,
                "vax_full": vax_full,
                "vax_daily_pm": vax_daily_pm,

            },
            "events": event_cards,
            "highlights": uniq,
            "meta": {
                "policy_levels": POLICY_LEVELS,
                "policy_labels": POLICY_LABELS,
                "stringency_explainer": STRINGENCY_EXPLAINER,
            },
            "disclaimer": "Educational playback of reported data + policy indices. Not causal inference.",
        }
    )
