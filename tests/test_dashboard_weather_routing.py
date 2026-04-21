from __future__ import annotations

import pandas as pd

from dashboard.utils import weather_client


def test_route_weather_source_boundaries():
    today = pd.Timestamp("2026-04-20")

    assert weather_client.route_weather_source(pd.Timestamp("2026-04-15"), today=today) == "archive"
    assert weather_client.route_weather_source(pd.Timestamp("2026-05-06"), today=today) == "forecast"
    assert weather_client.route_weather_source(pd.Timestamp("2026-05-07"), today=today) == "prior_year"


def test_get_weather_for_week_routes_correct_provider(monkeypatch):
    calls = []

    def fake_archive(lat, lon, date_str, variables, days_prior):
        calls.append(("archive", date_str))
        return {"ok": 1.0}

    def fake_forecast(lat, lon, date_str, variables, days_prior):
        calls.append(("forecast", date_str))
        return {"ok": 2.0}

    monkeypatch.setattr(weather_client, "get_archive_weather_cached", fake_archive)
    monkeypatch.setattr(weather_client, "get_forecast_weather_cached", fake_forecast)

    today = pd.Timestamp("2026-04-20")

    archive = weather_client.get_weather_for_week(35.0, -76.5, pd.Timestamp("2026-04-10"), today=today)
    forecast = weather_client.get_weather_for_week(35.0, -76.5, pd.Timestamp("2026-04-25"), today=today)
    prior_year = weather_client.get_weather_for_week(35.0, -76.5, pd.Timestamp("2026-06-01"), today=today)

    assert archive["ok"] == 1.0
    assert forecast["ok"] == 2.0
    assert prior_year["ok"] == 1.0
    assert calls[2][0] == "archive"
    assert calls[2][1] == "2025-06-02"
