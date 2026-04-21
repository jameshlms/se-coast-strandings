from __future__ import annotations

import pandas as pd

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
