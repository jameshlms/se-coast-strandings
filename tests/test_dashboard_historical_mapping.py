from __future__ import annotations

import pandas as pd

from dashboard.utils import data_loader
from dashboard.utils.data_loader import _standardize_historical_columns
from dashboard.utils.map_helpers import build_historical_map_points


def test_standardize_historical_columns_handles_raw_field_names():
    raw_like = pd.DataFrame(
        {
            "National Database Number": ["MM001"],
            "Common Name": ["Dolphin, bottlenose"],
            "State": ["NC"],
            "Condition at Examination": ["2 - fresh dead"],
            "Year of Observation": [2024],
            "Month of Observation": ["JUL"],
            "Day of Observation": [20],
            "Latitude Actual/Estimate": ["Estimated"],
            "Longitude Actual/Estimate": ["Actual"],
            "Latitude": ["35.14"],
            "Longitude": ["-76.44"],
        }
    )

    result = _standardize_historical_columns(raw_like)

    assert result.loc[0, "common_name"] == "Dolphin, bottlenose"
    assert result.loc[0, "state"] == "NC"
    assert result.loc[0, "condition"] == "2 - fresh dead"
    assert result.loc[0, "event_id"] == "MM001"
    assert str(result.loc[0, "mms_observation_dt"].date()) == "2024-07-20"
    assert bool(result.loc[0, "is_estimated_coordinates"]) is True


def test_build_historical_map_points_returns_st_map_columns():
    events = pd.DataFrame(
        {
            "Latitude": [35.1, None],
            "Longitude": [-76.1, -76.2],
            "common_name": ["A", "B"],
        }
    )

    points = build_historical_map_points(events)

    assert list(points.columns[-4:]) == ["latitude", "longitude", "size", "color"]
    assert len(points) == 1
    assert points.iloc[0]["color"] == "#1b7837"


def test_enrich_historical_weather_from_weekly_backfills_missing_values(tmp_path, monkeypatch):
    weekly_path = tmp_path / "final_dataset.parquet"
    pd.DataFrame(
        {
            "week_start": pd.to_datetime(["2024-04-15"]),
            "region": ["R6"],
            "temperature_2m_max_0_days_prior_mean": [21.5],
            "temperature_2m_min_0_days_prior_mean": [9.5],
        }
    ).to_parquet(weekly_path, index=False)
    monkeypatch.setattr(data_loader, "WEEKLY_DATA_PATH", weekly_path)

    events = pd.DataFrame(
        {
            "mms_observation_dt": pd.to_datetime(["2024-04-15", "2024-04-16"]),
            "Latitude": [35.0, 35.1],
            "Longitude": [-76.5, -76.4],
            "temperature_2m_max_0_days_prior": [pd.NA, 30.0],
            "temperature_2m_min_0_days_prior": [pd.NA, pd.NA],
        }
    )

    result = data_loader._enrich_historical_weather_from_weekly(events)

    assert result.loc[0, "temperature_2m_max_0_days_prior"] == 21.5
    assert result.loc[0, "temperature_2m_min_0_days_prior"] == 9.5
    assert result.loc[1, "temperature_2m_max_0_days_prior"] == 30.0
    assert result.loc[1, "temperature_2m_min_0_days_prior"] == 9.5


def test_enrich_historical_weather_from_weekly_noop_when_weekly_schema_missing(tmp_path, monkeypatch):
    weekly_path = tmp_path / "final_dataset.parquet"
    pd.DataFrame({"week": [pd.Timestamp("2024-04-15")], "band": ["R6"]}).to_parquet(
        weekly_path, index=False
    )
    monkeypatch.setattr(data_loader, "WEEKLY_DATA_PATH", weekly_path)

    events = pd.DataFrame(
        {
            "mms_observation_dt": pd.to_datetime(["2024-04-15"]),
            "Latitude": [35.0],
            "Longitude": [-76.5],
            "temperature_2m_max_0_days_prior": [pd.NA],
        }
    )

    result = data_loader._enrich_historical_weather_from_weekly(events)
    pd.testing.assert_frame_equal(result, events)


def test_enrich_historical_weather_from_weekly_uses_nearest_week_when_exact_missing(
    tmp_path, monkeypatch
):
    weekly_path = tmp_path / "final_dataset.parquet"
    pd.DataFrame(
        {
            "week_start": pd.to_datetime(["2024-05-13", "2024-06-03"]),
            "region": ["R6", "R6"],
            "temperature_2m_max_0_days_prior_mean": [21.0, 25.0],
            "temperature_2m_min_0_days_prior_mean": [10.0, 12.0],
        }
    ).to_parquet(weekly_path, index=False)
    monkeypatch.setattr(data_loader, "WEEKLY_DATA_PATH", weekly_path)

    events = pd.DataFrame(
        {
            "mms_observation_dt": pd.to_datetime(["2024-04-01"]),
            "Latitude": [35.0],
            "Longitude": [-76.5],
            "temperature_2m_max_0_days_prior": [pd.NA],
            "temperature_2m_min_0_days_prior": [pd.NA],
        }
    )

    result = data_loader._enrich_historical_weather_from_weekly(events)
    assert result.loc[0, "temperature_2m_max_0_days_prior"] == 21.0
    assert result.loc[0, "temperature_2m_min_0_days_prior"] == 10.0
