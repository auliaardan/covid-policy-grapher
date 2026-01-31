import csv
import io
from datetime import datetime

import requests
from django.core.management.base import BaseCommand
from django.db import transaction

from dashboard.models import Country, DailyMetric, PolicyDaily, PolicyEvent

OWID_URL = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv"
# OxCGRT stopped updating in later period, but good for historical COVID policy timeline:
OXCGRT_URL = "https://raw.githubusercontent.com/OxCGRT/covid-policy-tracker-legacy/main/legacy_data_202207/OxCGRT_latest.csv"

def _to_float(x):
    try:
        if x is None or x == "":
            return None
        return float(x)
    except:
        return None

def _to_int(x):
    try:
        if x is None or x == "":
            return None
        return int(float(x))
    except:
        return None

def _to_date_owid(s):
    # YYYY-MM-DD
    return datetime.strptime(s, "%Y-%m-%d").date()

def _to_date_oxcgrt(s):
    # YYYYMMDD
    return datetime.strptime(s, "%Y%m%d").date()

class Command(BaseCommand):
    help = "Import OWID COVID metrics + OxCGRT policies into DB; generate policy events from policy changes."

    def add_arguments(self, parser):
        parser.add_argument("--owid", action="store_true", help="Import OWID metrics")
        parser.add_argument("--oxcgrt", action="store_true", help="Import OxCGRT policy data")
        parser.add_argument("--all", action="store_true", help="Import both OWID and OxCGRT")
        parser.add_argument("--limit_countries", type=int, default=0, help="Optional limit for number of countries imported (debug)")
        parser.add_argument("--timeout", type=int, default=60, help="HTTP timeout seconds")

    def handle(self, *args, **opts):
        do_all = opts["all"] or (not opts["owid"] and not opts["oxcgrt"])
        do_owid = do_all or opts["owid"]
        do_oxcgrt = do_all or opts["oxcgrt"]

        if do_owid:
            self.stdout.write("Downloading OWID CSV...")
            self.import_owid(timeout=opts["timeout"], limit_countries=opts["limit_countries"])

        if do_oxcgrt:
            self.stdout.write("Downloading OxCGRT CSV...")
            self.import_oxcgrt(timeout=opts["timeout"], limit_countries=opts["limit_countries"])

        self.stdout.write(self.style.SUCCESS("Done."))

    @transaction.atomic
    def import_owid(self, timeout=60, limit_countries=0):
        resp = requests.get(OWID_URL, timeout=timeout)
        resp.raise_for_status()

        f = io.StringIO(resp.text)
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
            date = _to_date_owid(row["date"])

            country, _ = Country.objects.get_or_create(iso_code=iso, defaults={"name": name or iso})
            # keep latest population/name if missing
            if name and country.name != name:
                country.name = name
            if pop and (not country.population or country.population != pop):
                country.population = pop
            country.save()

            seen_countries.add(iso)

            new_cases_sm = _to_float(row.get("new_cases_smoothed"))
            new_deaths_sm = _to_float(row.get("new_deaths_smoothed"))
            total_cases = _to_float(row.get("total_cases"))
            total_deaths = _to_float(row.get("total_deaths"))

            cases_pm = _to_float(row.get("new_cases_smoothed_per_million"))
            deaths_pm = _to_float(row.get("new_deaths_smoothed_per_million"))

            total_vax_ph = _to_float(row.get("total_vaccinations_per_hundred"))
            one_dose_ph = _to_float(row.get("people_vaccinated_per_hundred"))
            full_ph = _to_float(row.get("people_fully_vaccinated_per_hundred"))
            new_vax_pm = _to_float(row.get("new_vaccinations_smoothed_per_million"))

            DailyMetric.objects.update_or_create(
                country=country,
                date=date,
                defaults={
                    "new_cases_smoothed": new_cases_sm,  # ✅ ADD THIS
                    "new_deaths_smoothed": new_deaths_sm,
                    "total_cases": total_cases,
                    "total_deaths": total_deaths,
                    "cases_per_million": cases_pm,
                    "deaths_per_million": deaths_pm,

                    # NEW: vaccinations
                    "total_vaccinations_per_hundred": total_vax_ph,
                    "people_vaccinated_per_hundred": one_dose_ph,
                    "people_fully_vaccinated_per_hundred": full_ph,
                    "new_vaccinations_smoothed_per_million": new_vax_pm,
                },

            )

            count_rows += 1
            if count_rows % 200000 == 0:
                self.stdout.write(f"  processed {count_rows} rows...")

        self.stdout.write(self.style.SUCCESS(f"OWID import complete. Rows: {count_rows}, Countries: {len(seen_countries)}"))

    @transaction.atomic
    def import_oxcgrt(self, timeout=60, limit_countries=0):
        resp = requests.get(OXCGRT_URL, timeout=timeout)
        resp.raise_for_status()

        f = io.StringIO(resp.text)
        reader = csv.DictReader(f)

        seen_countries = set()
        count_rows = 0

        for row in reader:
            iso = row.get("CountryCode")  # ISO3 often, sometimes missing
            name = row.get("CountryName")
            if not name:
                continue

            # OxCGRT uses ISO3 codes; OWID uses ISO3 as well for many countries.
            if not iso or iso.strip() == "":
                # fallback: create pseudo iso with name (not ideal)
                iso = f"X_{name[:6].upper()}"

            if limit_countries and iso not in seen_countries and len(seen_countries) >= limit_countries:
                continue

            date_raw = row.get("Date")
            if not date_raw:
                continue
            date = _to_date_oxcgrt(date_raw)

            country, _ = Country.objects.get_or_create(iso_code=iso, defaults={"name": name})
            if name and country.name != name:
                country.name = name
                country.save()

            seen_countries.add(iso)

            PolicyDaily.objects.update_or_create(
                country=country,
                date=date,
                defaults={
                    "stringency_index": _to_float(row.get("StringencyIndex")),
                    "c1_school_closing": _to_int(row.get("C1_School closing")),
                    "c2_workplace_closing": _to_int(row.get("C2_Workplace closing")),
                    "c6_stay_at_home": _to_int(row.get("C6_Stay at home requirements")),
                    "c8_international_travel_controls": _to_int(row.get("C8_International travel controls")),
                    "h6_facial_coverings": _to_int(row.get("H6_Facial Coverings")),
                },
            )

            count_rows += 1
            if count_rows % 200000 == 0:
                self.stdout.write(f"  processed {count_rows} rows...")

        self.stdout.write(self.style.SUCCESS(f"OxCGRT import complete. Rows: {count_rows}, Countries: {len(seen_countries)}"))

        self.stdout.write("Generating policy events (diffs)...")
        self.generate_policy_events(seen_countries)

    def generate_policy_events(self, iso_codes):
        # delete and regenerate for these countries (simple and safe for MVP)
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

                def diff(label, a, b):
                    if a != b and (a is not None or b is not None):
                        changes.append(f"{label}: {a} → {b}")

                diff("Stringency", prev.stringency_index, p.stringency_index)
                diff("School closing", prev.c1_school_closing, p.c1_school_closing)
                diff("Workplace closing", prev.c2_workplace_closing, p.c2_workplace_closing)
                diff("Stay-at-home", prev.c6_stay_at_home, p.c6_stay_at_home)
                diff("Intl travel", prev.c8_international_travel_controls, p.c8_international_travel_controls)
                diff("Face coverings", prev.h6_facial_coverings, p.h6_facial_coverings)

                # Only write events when meaningful changes happen.
                if changes:
                    # Keep text short-ish
                    text = "; ".join(changes[:3])
                    if len(changes) > 3:
                        text += f" (+{len(changes)-3} more)"
                    PolicyEvent.objects.create(country=country, date=p.date, text=text)
                    events_created += 1

                prev = p

            self.stdout.write(f"  {country.name}: events {events_created}")
