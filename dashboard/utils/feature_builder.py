from __future__ import annotations

import math
import re
from typing import Sequence

import pandas as pd

from se_coast_strandings.contextual_data.lunar_phases import moon_age
from se_coast_strandings.regions import MAX_LAT, MIN_LAT, make_degrees
from se_coast_strandings.transformations import make_cyclic, make_cyclic_season

try:
    from dashboard.utils.weather_client import get_weather_for_week
except ModuleNotFoundError:
    from utils.weather_client import get_weather_for_week

WEATHER_ALIAS_PATTERN = re.compile(
    r"^(temperature_2m_(?:max|min)_\d+_days_prior)_(mean|max)$"
)


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        if pd.isna(value):
            return default
        return float(value)
    except Exception:
        return default


def _region_lat_lookup(all_regions: Sequence[str]) -> dict[str, float]:
    region_centers = {label: (lat_min + lat_max) / 2.0 for label, lat_min, lat_max in make_degrees(0.5)}
    fallback_lat = (MIN_LAT + MAX_LAT) / 2.0
    return {region: region_centers.get(region, fallback_lat) for region in all_regions}


def _nearest_plankton_density(
    plankton_lookup: pd.DataFrame,
    region: str,
    week_start: pd.Timestamp,
) -> float:
    if plankton_lookup.empty or "region" not in plankton_lookup.columns:
        return 0.0

    subset = plankton_lookup[plankton_lookup["region"] == region]
    if subset.empty or "week_start" not in subset.columns:
        return 0.0

    diffs = (subset["week_start"] - week_start).abs()
    nearest_idx = diffs.idxmin()
    if "plankton_density" not in subset.columns:
        return 0.0
    return _safe_float(subset.loc[nearest_idx, "plankton_density"], default=0.0)


def _lag_feature(
    weekly_history: pd.DataFrame,
    region: str,
    week_start: pd.Timestamp,
    lag_weeks: int,
) -> float:
    if weekly_history.empty:
        return 0.0

    if not {"region", "week_start", "stranding_count"}.issubset(weekly_history.columns):
        return 0.0

    region_history = weekly_history[
        (weekly_history["region"] == region) & (weekly_history["week_start"] < week_start)
    ].sort_values("week_start")
    if region_history.empty:
        return 0.0

    target_date = week_start - pd.Timedelta(weeks=lag_weeks)
    exact = region_history[region_history["week_start"] == target_date]
    if not exact.empty:
        return _safe_float(exact.iloc[-1]["stranding_count"], default=0.0)

    prior = region_history[region_history["week_start"] <= target_date]
    if not prior.empty:
        return _safe_float(prior.iloc[-1]["stranding_count"], default=0.0)

    return _safe_float(region_history["stranding_count"].mean(), default=0.0)


def _add_dayofweek_features(
    row: dict[str, float],
    week_start: pd.Timestamp,
    model_features: Sequence[str],
) -> None:
    thursday = week_start + pd.Timedelta(days=3)
    dow_series = pd.Series([thursday.dayofweek])
    dow_sin, dow_cos = make_cyclic(dow_series, 7, name="dayofweek")
    dow_sin_value = _safe_float(dow_sin.iloc[0])
    dow_cos_value = _safe_float(dow_cos.iloc[0])

    if "dayofweek_sin" in model_features:
        row["dayofweek_sin"] = dow_sin_value
    if "dayofweek_cos" in model_features:
        row["dayofweek_cos"] = dow_cos_value
    if "dayofweek_sin_mean" in model_features:
        row["dayofweek_sin_mean"] = dow_sin_value
    if "dayofweek_cos_mean" in model_features:
        row["dayofweek_cos_mean"] = dow_cos_value


def _apply_weather_aliases(
    row: dict[str, float],
    weather_raw: dict[str, float | None],
    model_features: Sequence[str],
) -> None:
    for key, value in weather_raw.items():
        if key in model_features:
            row[key] = _safe_float(value)

    for feature in model_features:
        match = WEATHER_ALIAS_PATTERN.match(feature)
        if not match:
            continue

        source_key = match.group(1)
        if source_key in weather_raw:
            row[feature] = _safe_float(weather_raw[source_key])

    # Collect per-day max/min values for derived features
    max_vals = {
        d: _safe_float(weather_raw.get(f"temperature_2m_max_{d}_days_prior"), default=math.nan)
        for d in range(7)
    }
    min_vals = {
        d: _safe_float(weather_raw.get(f"temperature_2m_min_{d}_days_prior"), default=math.nan)
        for d in range(7)
    }

    # Per-day delta columns: delta_N = value_N - value_(N+1) (days 1–5; day 6 needs day 7)
    for d in range(1, 7):
        next_max = _safe_float(weather_raw.get(f"temperature_2m_max_{d + 1}_days_prior"), default=math.nan)
        next_min = _safe_float(weather_raw.get(f"temperature_2m_min_{d + 1}_days_prior"), default=math.nan)
        max_delta_col = f"temperature_2m_max_{d}_days_prior_delta_mean"
        min_delta_col = f"temperature_2m_min_{d}_days_prior_delta_mean"
        if max_delta_col in model_features and not pd.isna(max_vals[d]) and not pd.isna(next_max):
            row[max_delta_col] = max_vals[d] - next_max
        if min_delta_col in model_features and not pd.isna(min_vals[d]) and not pd.isna(next_min):
            row[min_delta_col] = min_vals[d] - next_min

    # Legacy delta features
    if "temp_delta_day0_mean" in model_features and not pd.isna(max_vals[0]) and not pd.isna(max_vals[1]):
        row["temp_delta_day0_mean"] = max_vals[0] - max_vals[1]
    if "temp_delta_day1_mean" in model_features and not pd.isna(max_vals[1]) and not pd.isna(max_vals[2]):
        row["temp_delta_day1_mean"] = max_vals[1] - max_vals[2]
    if "temp_delta_day2_mean" in model_features and not pd.isna(max_vals[2]) and not pd.isna(max_vals[3]):
        row["temp_delta_day2_mean"] = max_vals[2] - max_vals[3]

    # 7-day summary features
    valid_max = [v for v in max_vals.values() if not pd.isna(v)]
    valid_min = [v for v in min_vals.values() if not pd.isna(v)]
    if valid_max and valid_min:
        mean_max = sum(valid_max) / len(valid_max)
        max_max = max(valid_max)
        min_min = min(valid_min)
        if "temp_7day_mean_max_mean" in model_features:
            row["temp_7day_mean_max_mean"] = mean_max
        if "temp_7day_max_max_mean" in model_features:
            row["temp_7day_max_max_mean"] = max_max
        if "temp_7day_min_min_mean" in model_features:
            row["temp_7day_min_min_mean"] = min_min
        if "temp_7day_range_mean" in model_features:
            row["temp_7day_range_mean"] = max_max - min_min


def build_feature_row(
    week_start: pd.Timestamp,
    region: str,
    all_regions: Sequence[str],
    weekly_history: pd.DataFrame,
    plankton_lookup: pd.DataFrame,
    model_features: Sequence[str],
    lat_for_weather: float,
    lon_for_weather: float = -76.5,
) -> dict[str, float]:
    target = pd.Timestamp(week_start).normalize()
    row: dict[str, float] = {feature: 0.0 for feature in model_features}

    month_series = pd.Series([target.month])
    day_series = pd.Series([target.dayofyear])
    date_series = pd.Series([target])

    month_sin, month_cos = make_cyclic(month_series, 12, name="month")
    dayofyear_sin, dayofyear_cos = make_cyclic(day_series, 365, name="dayofyear")
    season_sin, season_cos = make_cyclic_season(date_series, name="season")

    if "month_sin" in row:
        row["month_sin"] = _safe_float(month_sin.iloc[0])
    if "month_cos" in row:
        row["month_cos"] = _safe_float(month_cos.iloc[0])
    if "dayofyear_sin" in row:
        row["dayofyear_sin"] = _safe_float(dayofyear_sin.iloc[0])
    if "dayofyear_cos" in row:
        row["dayofyear_cos"] = _safe_float(dayofyear_cos.iloc[0])
    if "season_sin" in row:
        row["season_sin"] = _safe_float(season_sin.iloc[0])
    if "season_cos" in row:
        row["season_cos"] = _safe_float(season_cos.iloc[0])

    _add_dayofweek_features(row=row, week_start=target, model_features=model_features)

    if "moon_age" in row:
        row["moon_age"] = _safe_float(moon_age(target))

    weather_raw = get_weather_for_week(
        lat=lat_for_weather,
        lon=lon_for_weather,
        week_start=target,
        variables=("temperature_2m_max", "temperature_2m_min"),
        days_prior=7,
    )
    _apply_weather_aliases(row=row, weather_raw=weather_raw, model_features=model_features)

    if "plankton_density" in row:
        row["plankton_density"] = _nearest_plankton_density(
            plankton_lookup=plankton_lookup,
            region=region,
            week_start=target,
        )

    if "stranding_count_lag_1" in row:
        row["stranding_count_lag_1"] = _lag_feature(
            weekly_history=weekly_history,
            region=region,
            week_start=target,
            lag_weeks=1,
        )

    if "stranding_count_lag_52" in row:
        row["stranding_count_lag_52"] = _lag_feature(
            weekly_history=weekly_history,
            region=region,
            week_start=target,
            lag_weeks=52,
        )

    for r in all_regions:
        feature_name = f"region_{r}"
        if feature_name in row:
            row[feature_name] = 1.0 if r == region else 0.0

    for feature in model_features:
        row[feature] = _safe_float(row.get(feature, 0.0), default=0.0)

    return row


def build_feature_frame_for_week(
    week_start: pd.Timestamp,
    regions: Sequence[str],
    weekly_history: pd.DataFrame,
    plankton_lookup: pd.DataFrame,
    model_features: Sequence[str],
    lon_for_weather: float = -76.5,
) -> pd.DataFrame:
    lat_lookup = _region_lat_lookup(regions)

    rows: list[dict[str, float]] = []
    for region in regions:
        row = build_feature_row(
            week_start=week_start,
            region=region,
            all_regions=regions,
            weekly_history=weekly_history,
            plankton_lookup=plankton_lookup,
            model_features=model_features,
            lat_for_weather=lat_lookup[region],
            lon_for_weather=lon_for_weather,
        )
        row["region"] = region
        rows.append(row)

    feature_df = pd.DataFrame(rows).set_index("region")
    feature_df = feature_df.reindex(columns=list(model_features), fill_value=0.0)
    return feature_df
