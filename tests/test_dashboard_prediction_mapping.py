from __future__ import annotations

import math
import pandas as pd

from dashboard.utils.map_helpers import build_prediction_map_points


def _hex_to_rgb(hex_color: str) -> tuple[int, int, int]:
    value = hex_color.lstrip("#")
    return tuple(int(value[i : i + 2], 16) for i in (0, 2, 4))


def test_build_prediction_map_points_uses_accuracy_based_color_and_size():
    region_predictions = pd.DataFrame(
        {
            "region": ["R0", "R1"],
            "lat_min": [32.0, 33.0],
            "lat_max": [32.5, 33.5],
            "predicted": [0.8, 0.8],
            "actual": [0.0, 1.0],
        }
    )

    points = build_prediction_map_points(region_predictions).set_index("region")

    assert set(["accuracy", "abs_error", "color", "size"]).issubset(points.columns)
    assert math.isclose(points.loc["R0", "accuracy"], 0.2, rel_tol=1e-9, abs_tol=1e-9)
    assert math.isclose(points.loc["R1", "accuracy"], 0.8, rel_tol=1e-9, abs_tol=1e-9)
    assert points.loc["R1", "size"] > points.loc["R0", "size"]

    r0_red, r0_green, _ = _hex_to_rgb(points.loc["R0", "color"])
    r1_red, r1_green, _ = _hex_to_rgb(points.loc["R1", "color"])
    assert r0_red > r1_red
    assert r1_green > r0_green


def test_build_prediction_map_points_defaults_to_neutral_when_actual_missing():
    region_predictions = pd.DataFrame(
        {
            "region": ["R0", "R1"],
            "lat_min": [32.0, 33.0],
            "lat_max": [32.5, 33.5],
            "predicted": [0.2, 0.8],
            "actual": [float("nan"), float("nan")],
        }
    )

    points = build_prediction_map_points(region_predictions).set_index("region")

    assert math.isclose(points.loc["R0", "accuracy"], 0.2, rel_tol=1e-9, abs_tol=1e-9)
    assert math.isclose(points.loc["R1", "accuracy"], 0.8, rel_tol=1e-9, abs_tol=1e-9)
    assert points.loc["R1", "size"] > points.loc["R0", "size"]
