from __future__ import annotations

from datetime import timedelta
from typing import Final, Sequence

import pandas as pd
from requests import Session
try:
    import streamlit as st
except ModuleNotFoundError:  # pragma: no cover - fallback for non-dashboard test envs
    class _StreamlitStub:
        @staticmethod
        def cache_data(*args, **kwargs):
            def decorator(func):
                return func

            return decorator

    st = _StreamlitStub()

from se_coast_strandings.contextual_data import weather as weather_module

DEFAULT_VARIABLES: tuple[str, ...] = ("temperature_2m_max", "temperature_2m_min")
ARCHIVE_URL: Final[str] = "https://archive-api.open-meteo.com/v1/archive"
FORECAST_URL: Final[str] = "https://api.open-meteo.com/v1/forecast"


def _flatten_daily_row(
    daily_values: dict[str, list[float | None]],
    variables: Sequence[str],
    days_prior: int,
) -> dict[str, float | None]:
    row: dict[str, float | None] = {}
    for var_name in variables:
        values = list(daily_values.get(var_name) or [])
        for j, n_days in enumerate(range(days_prior - 1, -1, -1)):
            value: float | None
            if j < len(values) and isinstance(values[j], (int, float)):
                value = float(values[j])
            else:
                value = None
            row[f"{var_name}_{n_days}_days_prior"] = value
    return row


def _fetch_open_meteo_daily(
    endpoint: str,
    lat: float,
    lon: float,
    date: pd.Timestamp,
    variables: Sequence[str],
    days_prior: int,
    tz: str = "America/New_York",
) -> dict[str, float | None]:
    target = pd.Timestamp(date).normalize()
    start_date = (target - timedelta(days=days_prior - 1)).strftime("%Y-%m-%d")
    end_date = target.strftime("%Y-%m-%d")
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "daily": ",".join(variables),
        "timezone": tz,
    }

    with Session() as session:
        response = session.get(endpoint, params=params, timeout=60)
        response.raise_for_status()
        payload = response.json()

    if isinstance(payload, list):
        payload = payload[0] if payload else {}
    if not isinstance(payload, dict):
        return {}
    if payload.get("error"):
        return {}

    daily = payload.get("daily")
    if not isinstance(daily, dict):
        return {}

    daily_values: dict[str, list[float | None]] = {}
    for var in variables:
        series = daily.get(var)
        if isinstance(series, list):
            daily_values[var] = series
        else:
            daily_values[var] = []

    return _flatten_daily_row(
        daily_values=daily_values,
        variables=variables,
        days_prior=days_prior,
    )


def _try_package_weather(
    lat: float,
    lon: float,
    date: pd.Timestamp,
    variables: Sequence[str],
    days_prior: int,
    forecast: bool,
) -> dict[str, float | None] | None:
    func_name = "get_weather_forecast_for_point" if forecast else "get_weather_for_point"
    func = getattr(weather_module, func_name, None)
    if not callable(func):
        return None

    try:
        result = func(
            lat=lat,
            lon=lon,
            date=pd.Timestamp(date),
            variables=tuple(variables),
            days_prior=days_prior,
        )
    except Exception:
        return None

    if isinstance(result, dict):
        return result
    return {}


def route_weather_source(
    week_start: pd.Timestamp,
    today: pd.Timestamp | None = None,
) -> str:
    target = pd.Timestamp(week_start).normalize()
    reference_today = (
        pd.Timestamp(today).normalize()
        if today is not None
        else pd.Timestamp.today().normalize()
    )
    archive_cutoff = reference_today - pd.Timedelta(days=5)
    forecast_cutoff = reference_today + pd.Timedelta(days=16)

    if target <= archive_cutoff:
        return "archive"
    if target <= forecast_cutoff:
        return "forecast"
    return "prior_year"


@st.cache_data(show_spinner=False)
def get_archive_weather_cached(
    lat: float,
    lon: float,
    date_str: str,
    variables: tuple[str, ...],
    days_prior: int,
) -> dict[str, float | None]:
    target_date = pd.Timestamp(date_str)
    package_result = _try_package_weather(
        lat=lat,
        lon=lon,
        date=target_date,
        variables=variables,
        days_prior=days_prior,
        forecast=False,
    )
    if package_result is not None:
        return package_result

    return _fetch_open_meteo_daily(
        endpoint=ARCHIVE_URL,
        lat=lat,
        lon=lon,
        date=target_date,
        variables=variables,
        days_prior=days_prior,
    )


@st.cache_data(show_spinner=False, ttl=3600)
def get_forecast_weather_cached(
    lat: float,
    lon: float,
    date_str: str,
    variables: tuple[str, ...],
    days_prior: int,
) -> dict[str, float | None]:
    target_date = pd.Timestamp(date_str)
    package_result = _try_package_weather(
        lat=lat,
        lon=lon,
        date=target_date,
        variables=variables,
        days_prior=days_prior,
        forecast=True,
    )
    if package_result is not None:
        return package_result

    return _fetch_open_meteo_daily(
        endpoint=FORECAST_URL,
        lat=lat,
        lon=lon,
        date=target_date,
        variables=variables,
        days_prior=days_prior,
    )


def get_weather_for_week(
    lat: float,
    lon: float,
    week_start: pd.Timestamp,
    variables: Sequence[str] = DEFAULT_VARIABLES,
    days_prior: int = 7,
    today: pd.Timestamp | None = None,
) -> dict[str, float | None]:
    target = pd.Timestamp(week_start).normalize()
    variables_tuple = tuple(variables)

    source = route_weather_source(week_start=target, today=today)
    if source == "archive":
        return get_archive_weather_cached(
            lat=lat,
            lon=lon,
            date_str=target.strftime("%Y-%m-%d"),
            variables=variables_tuple,
            days_prior=days_prior,
        )

    if source == "forecast":
        return get_forecast_weather_cached(
            lat=lat,
            lon=lon,
            date_str=target.strftime("%Y-%m-%d"),
            variables=variables_tuple,
            days_prior=days_prior,
        )

    prior_year = target - pd.Timedelta(weeks=52)
    return get_archive_weather_cached(
        lat=lat,
        lon=lon,
        date_str=prior_year.strftime("%Y-%m-%d"),
        variables=variables_tuple,
        days_prior=days_prior,
    )
