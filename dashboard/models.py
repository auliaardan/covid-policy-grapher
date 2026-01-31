from django.db import models


class Country(models.Model):
    iso_code = models.CharField(max_length=10, unique=True)
    name = models.CharField(max_length=200)
    population = models.FloatField(null=True, blank=True)

    def __str__(self):
        return f"{self.name} ({self.iso_code})"


class DailyMetric(models.Model):
    country = models.ForeignKey(Country, on_delete=models.CASCADE, related_name="metrics")
    date = models.DateField(db_index=True)

    # OWID (smoothed)
    new_cases_smoothed = models.FloatField(null=True, blank=True)
    new_deaths_smoothed = models.FloatField(null=True, blank=True)

    # Optional totals
    total_cases = models.FloatField(null=True, blank=True)
    total_deaths = models.FloatField(null=True, blank=True)

    # Per-million (smoothed)
    cases_per_million = models.FloatField(null=True, blank=True)
    deaths_per_million = models.FloatField(null=True, blank=True)

    # Vaccination (% of population)
    total_vaccinations_per_hundred = models.FloatField(null=True, blank=True)
    people_fully_vaccinated_per_hundred = models.FloatField(null=True, blank=True)

    class Meta:
        unique_together = ("country", "date")
        indexes = [models.Index(fields=["country", "date"])]

    def __str__(self):
        return f"{self.country.iso_code} {self.date}"


class PolicyDaily(models.Model):
    country = models.ForeignKey(Country, on_delete=models.CASCADE, related_name="policies")
    date = models.DateField(db_index=True)

    # OxCGRT indicators (selected)
    stringency_index = models.FloatField(null=True, blank=True)
    c1_school_closing = models.IntegerField(null=True, blank=True)
    c2_workplace_closing = models.IntegerField(null=True, blank=True)
    c6_stay_at_home = models.IntegerField(null=True, blank=True)
    c8_international_travel_controls = models.IntegerField(null=True, blank=True)
    h6_facial_coverings = models.IntegerField(null=True, blank=True)

    class Meta:
        unique_together = ("country", "date")
        indexes = [models.Index(fields=["country", "date"])]

    def __str__(self):
        return f"Policy {self.country.iso_code} {self.date}"


class PolicyEvent(models.Model):
    country = models.ForeignKey(Country, on_delete=models.CASCADE, related_name="events")
    date = models.DateField(db_index=True)
    text = models.CharField(max_length=500)

    class Meta:
        indexes = [models.Index(fields=["country", "date"])]

    def __str__(self):
        return f"Event {self.country.iso_code} {self.date}: {self.text}"
