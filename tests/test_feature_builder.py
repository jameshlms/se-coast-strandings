from __future__ import annotations

import pandas as pd

from dashboard.utils import feature_builder
from dashboard.utils.data_loader import compare_feature_schemas


def _weekly_history() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "week_start": pd.to_datetime(["2026-03-30", "2026-04-06", "2026-04-13", "2025-04-14"]),
            "region": ["R0", "R0", "R0", "R0"],
            "stranding_count": [2.0, 3.0, 4.0, 1.0],
        }
    )


def _plankton_lookup() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "week_start": pd.to_datetime(["2026-04-14"]),
            "region": ["R0"],
            "plankton_density": [0.75],
        }
    )


def _weather_payload() -> dict[str, float]:
    return {
        "temperature_2m_max_0_days_prior": 20.0,
        "temperature_2m_max_1_days_prior": 18.0,
        "temperature_2m_max_2_days_prior": 17.0,
        "temperature_2m_max_3_days_prior": 15.0,
        "temperature_2m_min_0_days_prior": 10.0,
    }


def test_dayofweek_name_variants_and_weather_aliases(monkeypatch):
    monkeypatch.setattr(feature_builder, "get_weather_for_week", lambda **kwargs: _weather_payload())

    model_features = [
        "dayofweek_sin",
        "dayofweek_cos",
        "dayofweek_sin_mean",
        "dayofweek_cos_mean",
        "temperature_2m_max_0_days_prior_mean",
        "temperature_2m_min_0_days_prior_mean",
        "temperature_2m_max_0_days_prior_max",
        "temp_delta_day0_mean",
        "temp_delta_day1_mean",
        "temp_delta_day2_mean",
        "region_R0",
    ]

    row = feature_builder.build_feature_row(
        week_start=pd.Timestamp("2026-04-20"),
        region="R0",
        all_regions=["R0"],
        weekly_history=_weekly_history(),
        plankton_lookup=_plankton_lookup(),
        model_features=model_features,
        lat_for_weather=34.0,
    )

    assert row["dayofweek_sin"] == row["dayofweek_sin_mean"]
    assert row["dayofweek_cos"] == row["dayofweek_cos_mean"]
    assert row["temperature_2m_max_0_days_prior_mean"] == 20.0
    assert row["temperature_2m_min_0_days_prior_mean"] == 10.0
    assert row["temperature_2m_max_0_days_prior_max"] == 20.0
    assert row["temp_delta_day0_mean"] == 2.0
    assert row["temp_delta_day1_mean"] == 1.0
    assert row["temp_delta_day2_mean"] == 2.0


def test_feature_frame_matches_model_feature_order(monkeypatch):
    monkeypatch.setattr(feature_builder, "get_weather_for_week", lambda **kwargs: _weather_payload())

    model_features = [
        "month_sin",
        "month_cos",
        "dayofyear_sin",
        "dayofyear_cos",
        "season_sin",
        "season_cos",
        "dayofweek_sin_mean",
        "dayofweek_cos_mean",
        "moon_age",
        "temperature_2m_max_0_days_prior_mean",
        "temp_delta_day0_mean",
        "plankton_density",
        "stranding_count_lag_1",
        "stranding_count_lag_52",
        "region_R0",
        "region_R1",
    ]

    frame = feature_builder.build_feature_frame_for_week(
        week_start=pd.Timestamp("2026-04-20"),
        regions=["R0", "R1"],
        weekly_history=_weekly_history(),
        plankton_lookup=_plankton_lookup(),
        model_features=model_features,
    )

    assert list(frame.columns) == model_features
    assert list(frame.index) == ["R0", "R1"]


def test_compare_feature_schemas_detects_mismatch():
    diff = compare_feature_schemas(
        model_features=["a", "b", "c"],
        metrics_features=["a", "b", "d"],
    )

    assert diff["model_only"] == {"c"}
    assert diff["metrics_only"] == {"d"}
