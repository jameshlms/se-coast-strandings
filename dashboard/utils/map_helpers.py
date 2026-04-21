from __future__ import annotations

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
        return pd.DataFrame(columns=["latitude", "longitude", "size", "color"])

    points = region_predictions.copy()
    points["latitude"] = (pd.to_numeric(points["lat_min"], errors="coerce") + pd.to_numeric(points["lat_max"], errors="coerce")) / 2.0
    points["longitude"] = -76.5
    points["predicted"] = _to_float_series(points["predicted"]).fillna(0.0)

    vmax = max(float(points["predicted"].max()), 1.0)
    scaled = points["predicted"].clip(lower=0.0).pow(0.5)
    points["size"] = (scaled * 2.0 + 6.0).clip(upper=14.0)
    points["color"] = points["predicted"].apply(lambda value: _color_scale(float(value), vmax))
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
