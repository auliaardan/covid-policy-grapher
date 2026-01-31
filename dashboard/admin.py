from django.contrib import admin
from .models import Country, DailyMetric, PolicyDaily, PolicyEvent

@admin.register(Country)
class CountryAdmin(admin.ModelAdmin):
    list_display = ("name", "iso_code", "population")
    search_fields = ("name", "iso_code")

@admin.register(DailyMetric)
class DailyMetricAdmin(admin.ModelAdmin):
    list_display = ("country", "date", "new_cases_smoothed", "new_deaths_smoothed")
    list_filter = ("country",)
    date_hierarchy = "date"

@admin.register(PolicyDaily)
class PolicyDailyAdmin(admin.ModelAdmin):
    list_display = ("country", "date", "stringency_index", "c1_school_closing", "h6_facial_coverings")
    list_filter = ("country",)
    date_hierarchy = "date"

@admin.register(PolicyEvent)
class PolicyEventAdmin(admin.ModelAdmin):
    list_display = ("country", "date", "text")
    list_filter = ("country",)
    date_hierarchy = "date"
