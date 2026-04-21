from __future__ import annotations

import pandas as pd

from se_coast_strandings.contextual_data import weather


def _forecast(max0: float, min0: float) -> dict:
    return {
        "daily": {
            "time": ["2026-04-14", "2026-04-15"],
            "temperature_2m_max": [max0 - 1.0, max0],
            "temperature_2m_min": [min0 - 1.0, min0],
        }
    }


def test_fetch_weather_context_preserves_non_contiguous_index(monkeypatch):
    def fake_get_weather_data(**kwargs):
        return [_forecast(20.0, 10.0), _forecast(21.0, 11.0)]

    monkeypatch.setattr(weather, "_get_weather_data", fake_get_weather_data)

    df = pd.DataFrame(
        {
            "Latitude": [35.1, 35.2, 35.3],
            "Longitude": [-76.1, -76.2, -76.3],
            "mms_observation_dt": [pd.Timestamp("2026-04-15")] * 3,
        },
        index=[101, 305, 999],
    )

    result = weather.fetch_weather_context(
        df=df,
        lat_column="Latitude",
        lon_column="Longitude",
        date_column="mms_observation_dt",
        daily_variables=["temperature_2m_max", "temperature_2m_min"],
        days_prior=2,
        sleep_interval=0,
    )

    assert list(result.index) == [101, 305, 999]
    assert result.loc[101, "temperature_2m_max_0_days_prior"] == 20.0
    assert result.loc[305, "temperature_2m_max_0_days_prior"] == 21.0
    assert pd.isna(result.loc[999, "temperature_2m_max_0_days_prior"])


def test_fetch_weather_context_short_response_leaves_missing_as_nan(monkeypatch):
    def fake_get_weather_data(**kwargs):
        return [_forecast(19.0, 9.0)]

    monkeypatch.setattr(weather, "_get_weather_data", fake_get_weather_data)

    df = pd.DataFrame(
        {
            "Latitude": [35.0, 35.5],
            "Longitude": [-76.0, -76.5],
            "mms_observation_dt": [pd.Timestamp("2026-04-15")] * 2,
        },
        index=[10, 20],
    )

    result = weather.fetch_weather_context(
        df=df,
        lat_column="Latitude",
        lon_column="Longitude",
        date_column="mms_observation_dt",
        daily_variables=["temperature_2m_max"],
        days_prior=2,
        sleep_interval=0,
    )

    assert result.loc[10, "temperature_2m_max_0_days_prior"] == 19.0
    assert pd.isna(result.loc[20, "temperature_2m_max_0_days_prior"])
