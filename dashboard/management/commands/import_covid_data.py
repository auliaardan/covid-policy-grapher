import csv
import io
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Optional

import requests
from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import transaction

from dashboard.models import Country, DailyMetric, PolicyDaily, PolicyEvent


OWID_URL = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv"

# OxCGRT changed repository structure over time; we try multiple known locations.
OXCGRT_URLS = [
    # Legacy snapshot
    "https://raw.githubusercontent.com/OxCGRT/covid-policy-tracker-legacy/main/legacy_data_202207/OxCGRT_latest.csv",
    # Older (may 404)
    "https://raw.githubusercontent.com/OxCGRT/covid-policy-tracker/master/data/OxCGRT_latest.csv",
]

CACHE_DIR = Path(getattr(settings, "BASE_DIR", Path.cwd())) / "data_cache"


def _to_float(x) -> Optional[float]:
    try:
        if x is None or x == "":
            return None
        return float(x)
    except Exception:
        return None


def _to_int(x) -> Optional[int]:
    try:
        if x is None or x == "":
            return None
        return int(float(x))
    except Exception:
        return None


def _parse_date(s: str):
    """
    Accepts:
    - YYYY-MM-DD (OWID)
    - YYYYMMDD (OxCGRT)
    - YYYY-MM-DD (some OxCGRT exports)
    """
    s = (s or "").strip()
    if not s:
        return None
    if "-" in s:
        return datetime.strptime(s, "%Y-%m-%d").date()
    if len(s) == 8:
        return datetime.strptime(s, "%Y%m%d").date()
    return None


def _download_text(urls: List[str], timeout: int) -> str:
    last_err = None
    for url in urls:
        try:
            resp = requests.get(url, timeout=timeout)
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            last_err = e
    raise RuntimeError(f"All download attempts failed. Last error: {last_err}")


class Command(BaseCommand):
    help = "Import OWID COVID metrics + OxCGRT policy indicators into DB; generate policy events from policy changes."

    def add_arguments(self, parser):
        parser.add_argument("--owid", action="store_true", help="Import OWID metrics")
        parser.add_argument("--oxcgrt", action="store_true", help="Import OxCGRT policy data")
        parser.add_argument("--all", action="store_true", help="Import both OWID and OxCGRT")
        parser.add_argument("--limit_countries", type=int, default=0, help="Optional limit for number of countries imported (debug)")
        parser.add_argument("--timeout", type=int, default=60, help="HTTP timeout seconds")

        # Caching / re-run ergonomics
        parser.add_argument("--no_cache", action="store_true", help="Do not use cache files")
        parser.add_argument("--force_download", action="store_true", help="Force re-download even if cache exists")
        parser.add_argument("--skip_download_owid", action="store_true", help="Skip OWID download and use cache/local file")
        parser.add_argument("--skip_download_oxcgrt", action="store_true", help="Skip OxCGRT download and use cache/local file")

    def handle(self, *args, **opts):
        do_all = opts["all"] or (not opts["owid"] and not opts["oxcgrt"])
        do_owid = do_all or opts["owid"]
        do_oxcgrt = do_all or opts["oxcgrt"]

        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        use_cache = not opts["no_cache"]
        force = bool(opts["force_download"])

        if do_owid:
            self.stdout.write("OWID: preparing data...")
            owid_text = self._get_cached_or_download(
                "owid-covid-data.csv",
                [OWID_URL],
                timeout=opts["timeout"],
                use_cache=use_cache,
                force=force,
                skip_download=bool(opts["skip_download_owid"]),
            )
            self.import_owid(owid_text, limit_countries=opts["limit_countries"])

        if do_oxcgrt:
            self.stdout.write("OxCGRT: preparing data...")
            oxcgrt_text = self._get_cached_or_download(
                "oxcgrt-latest.csv",
                OXCGRT_URLS,
                timeout=opts["timeout"],
                use_cache=use_cache,
                force=force,
                skip_download=bool(opts["skip_download_oxcgrt"]),
            )
            self.import_oxcgrt(oxcgrt_text, limit_countries=opts["limit_countries"])

        self.stdout.write(self.style.SUCCESS("Done."))

    def _get_cached_or_download(
        self,
        cache_name: str,
        urls: List[str],
        *,
        timeout: int,
        use_cache: bool,
        force: bool,
        skip_download: bool,
    ) -> str:
        cache_path = CACHE_DIR / cache_name

        if use_cache and cache_path.exists() and (skip_download or not force):
            self.stdout.write(f"  using cache: {cache_path}")
            return cache_path.read_text(encoding="utf-8", errors="replace")

        if skip_download:
            raise RuntimeError(f"Cache file not found but --skip_download_* was set: {cache_path}")

        self.stdout.write("  downloading...")
        text = _download_text(urls, timeout=timeout)

        if use_cache:
            cache_path.write_text(text, encoding="utf-8")
            self.stdout.write(f"  saved cache: {cache_path}")

        return text

    @transaction.atomic
    def import_owid(self, csv_text: str, limit_countries: int = 0):
        f = io.StringIO(csv_text)
        reader = csv.DictReader(f)

        seen_countries = set()
        count_rows = 0

        for row in reader:
            iso = row.get("iso_code")
            name = row.get("location")

            # Skip aggregates like OWID_WRL, OWID_*
            if not iso or iso.startswith("OWID_"):
                continue

            if limit_countries and iso not in seen_countries and len(seen_countries) >= limit_countries:
                continue

            pop = _to_float(row.get("population"))
            d = _parse_date(row.get("date") or "")
            if not d:
                continue

            country, _ = Country.objects.get_or_create(iso_code=iso, defaults={"name": name or iso})
            changed = False
            if name and country.name != name:
                country.name = name
                changed = True
            if pop and (not country.population or country.population != pop):
                country.population = pop
                changed = True
            if changed:
                country.save()

            seen_countries.add(iso)

            DailyMetric.objects.update_or_create(
                country=country,
                date=d,
                defaults={
                    "new_cases_smoothed": _to_float(row.get("new_cases_smoothed")),
                    "new_deaths_smoothed": _to_float(row.get("new_deaths_smoothed")),
                    "total_cases": _to_float(row.get("total_cases")),
                    "total_deaths": _to_float(row.get("total_deaths")),
                    "cases_per_million": _to_float(row.get("new_cases_smoothed_per_million")),
                    "deaths_per_million": _to_float(row.get("new_deaths_smoothed_per_million")),
                    "total_vaccinations_per_hundred": _to_float(row.get("total_vaccinations_per_hundred")),
                    "people_fully_vaccinated_per_hundred": _to_float(row.get("people_fully_vaccinated_per_hundred")),
                },
            )

            count_rows += 1
            if count_rows % 200000 == 0:
                self.stdout.write(f"  processed {count_rows} rows...")

        self.stdout.write(self.style.SUCCESS(f"OWID import complete. Rows: {count_rows}, Countries: {len(seen_countries)}"))

    @transaction.atomic
    def import_oxcgrt(self, csv_text: str, limit_countries: int = 0):
        f = io.StringIO(csv_text)
        reader = csv.DictReader(f)

        seen_countries = set()
        count_rows = 0

        for row in reader:
            iso = (row.get("CountryCode") or row.get("country_code") or row.get("ISO3") or "").strip()
            name = (row.get("CountryName") or row.get("country_name") or row.get("Country") or "").strip()
            if not name:
                continue

            if not iso:
                iso = f"X_{name[:6].upper()}"

            if limit_countries and iso not in seen_countries and len(seen_countries) >= limit_countries:
                continue

            d = _parse_date(row.get("Date") or row.get("date") or "")
            if not d:
                continue

            country, _ = Country.objects.get_or_create(iso_code=iso, defaults={"name": name})
            if name and country.name != name:
                country.name = name
                country.save()

            seen_countries.add(iso)

            def g(*keys):
                for k in keys:
                    if k in row and row.get(k) not in (None, ""):
                        return row.get(k)
                return None

            PolicyDaily.objects.update_or_create(
                country=country,
                date=d,
                defaults={
                    "stringency_index": _to_float(g("StringencyIndex", "stringency_index")),
                    "c1_school_closing": _to_int(g("C1_School closing", "c1_school_closing", "C1_School_Closing")),
                    "c2_workplace_closing": _to_int(g("C2_Workplace closing", "c2_workplace_closing", "C2_Workplace_Closing")),
                    "c6_stay_at_home": _to_int(g("C6_Stay at home requirements", "c6_stay_at_home", "C6_Stay_at_home_requirements")),
                    "c8_international_travel_controls": _to_int(g("C8_International travel controls", "c8_international_travel_controls", "C8_International_travel_controls")),
                    "h6_facial_coverings": _to_int(g("H6_Facial Coverings", "h6_facial_coverings", "H6_Facial_Coverings")),
                },
            )

            count_rows += 1
            if count_rows % 200000 == 0:
                self.stdout.write(f"  processed {count_rows} rows...")

        self.stdout.write(self.style.SUCCESS(f"OxCGRT import complete. Rows: {count_rows}, Countries: {len(seen_countries)}"))

        self.stdout.write("Generating policy events (diffs)...")
        self.generate_policy_events(seen_countries)

    def generate_policy_events(self, iso_codes: Iterable[str]):
        for iso in iso_codes:
            try:
                country = Country.objects.get(iso_code=iso)
            except Country.DoesNotExist:
                continue

            PolicyEvent.objects.filter(country=country).delete()

            qs = PolicyDaily.objects.filter(country=country).order_by("date")
            prev = None
            events_created = 0

            for p in qs:
                if prev is None:
                    prev = p
                    continue

                changes = []

                def diff(label, a, b, *, float_threshold: float = 0.0):
                    if a == b:
                        return
                    if a is None and b is None:
                        return
                    if float_threshold and a is not None and b is not None:
                        try:
                            if abs(float(b) - float(a)) < float_threshold:
                                return
                        except Exception:
                            pass
                    changes.append(f"{label}: {a} → {b}")

                diff("Stringency", prev.stringency_index, p.stringency_index, float_threshold=2.0)
                diff("School closing", prev.c1_school_closing, p.c1_school_closing)
                diff("Workplace closing", prev.c2_workplace_closing, p.c2_workplace_closing)
                diff("Stay-at-home", prev.c6_stay_at_home, p.c6_stay_at_home)
                diff("Intl travel", prev.c8_international_travel_controls, p.c8_international_travel_controls)
                diff("Face coverings", prev.h6_facial_coverings, p.h6_facial_coverings)

                if changes:
                    text = "; ".join(changes[:3])
                    if len(changes) > 3:
                        text += f" (+{len(changes) - 3} more)"
                    PolicyEvent.objects.create(country=country, date=p.date, text=text)
                    events_created += 1

                prev = p

            self.stdout.write(f"  {country.name}: events {events_created}")
