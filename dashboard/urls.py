from django.urls import path
from . import views

urlpatterns = [
    path("", views.index, name="index"),
    path("api/countries/", views.api_countries, name="api_countries"),
    path("api/timeseries/<str:iso_code>/", views.api_timeseries, name="api_timeseries"),
]
