from __future__ import annotations

import colorsys
import math
import pandas as pd
from typing import Iterable


def _to_float_series(values: pd.Series) -> pd.Series:
    return pd.to_numeric(values, errors="coerce")


def _color_scale(value: float, vmax: float) -> str:
    if vmax <= 0:
        return "#2b8cbe"
    ratio = min(max(value / vmax, 0.0), 1.0)
    red = int(255 * ratio)
    green = int(190 - (125 * ratio))
    blue = int(50 - (35 * ratio))
    return f"#{red:02x}{green:02x}{blue:02x}"


def _clamp01(value: float) -> float:
    return min(max(float(value), 0.0), 1.0)


def _accuracy_score(predicted: float, actual: float) -> float:
    denominator = max(abs(float(actual)), abs(float(predicted)), 1.0)
    relative_error = abs(float(predicted) - float(actual)) / denominator
    return _clamp01(1.0 - relative_error)


def _contrast_score(value: float) -> float:
    # Sigmoid contrast makes 0.2 and 0.8 clearly distinct in hue/size.
    centered = (_clamp01(value) - 0.5) * 5.5
    return 1.0 / (1.0 + math.exp(-centered))


def _score_color(value: float) -> str:
    contrasted = _contrast_score(float(value))
    hue = (130.0 * contrasted) / 360.0
    lightness = 0.42 + (0.10 * contrasted)
    saturation = 0.92
    red, green, blue = colorsys.hls_to_rgb(hue, lightness, saturation)
    return f"#{int(red * 255):02x}{int(green * 255):02x}{int(blue * 255):02x}"


def _score_circle_size(value: float) -> float:
    contrasted = _contrast_score(float(value))
    return 8.0 + (20.0 * (contrasted ** 1.15))


def build_historical_map_points(events: pd.DataFrame) -> pd.DataFrame:
    if events.empty or not {"Latitude", "Longitude"}.issubset(events.columns):
        return pd.DataFrame(columns=["latitude", "longitude", "size", "color"])

    points = events.copy()
    points["latitude"] = _to_float_series(points["Latitude"])
    points["longitude"] = _to_float_series(points["Longitude"])
    points = points.dropna(subset=["latitude", "longitude"]).copy()
    if points.empty:
        return pd.DataFrame(columns=["latitude", "longitude", "size", "color"])

    points["size"] = 5.0
    points["color"] = "#1b7837"
    return points


def build_prediction_map_points(region_predictions: pd.DataFrame) -> pd.DataFrame:
    if region_predictions.empty:
        return pd.DataFrame(
            columns=[
                "latitude",
                "longitude",
                "size",
                "color",
                "abs_error",
                "accuracy",
            ]
        )

    points = region_predictions.copy()
    points["latitude"] = (
        pd.to_numeric(points["lat_min"], errors="coerce")
        + pd.to_numeric(points["lat_max"], errors="coerce")
    ) / 2.0
    points["longitude"] = -76.5
    points["predicted"] = _to_float_series(points["predicted"]).fillna(0.0)
    points["actual"] = _to_float_series(points.get("actual", pd.Series(index=points.index)))

    points["abs_error"] = (points["predicted"] - points["actual"]).abs()
    vmax_predicted = max(float(points["predicted"].max()), 1.0)

    def _visual_score(row: pd.Series) -> float:
        predicted = float(row["predicted"])
        if pd.notna(row["actual"]):
            return _accuracy_score(predicted, float(row["actual"]))
        return _clamp01(predicted / vmax_predicted)

    points["accuracy"] = points.apply(_visual_score, axis=1)
    points["size"] = points["accuracy"].apply(_score_circle_size)
    points["color"] = points["accuracy"].apply(_score_color)
    return points.dropna(subset=["latitude", "longitude"]).copy()


def find_closest_event(
    events: pd.DataFrame,
    click_lat: float,
    click_lon: float,
) -> pd.Series | None:
    if events.empty or not {"Latitude", "Longitude"}.issubset(events.columns):
        return None

    candidates = events.dropna(subset=["Latitude", "Longitude"]).copy()
    if candidates.empty:
        return None

    distances = (candidates["Latitude"] - click_lat).abs() + (candidates["Longitude"] - click_lon).abs()
    return candidates.loc[distances.idxmin()]


def region_from_click(region_predictions: pd.DataFrame, click_lat: float) -> str | None:
    if region_predictions.empty:
        return None

    for _, row in region_predictions.iterrows():
        if float(row["lat_min"]) <= click_lat <= float(row["lat_max"]):
            return str(row["region"])
    return None


def make_region_frame(regions: Iterable[tuple[str, float, float]]) -> pd.DataFrame:
    return pd.DataFrame(regions, columns=["region", "lat_min", "lat_max"])
