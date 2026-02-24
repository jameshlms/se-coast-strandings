"""
Plankton abundance imputation module.

Uses Facebook Prophet to fit time series models on sparse COPEPOD plankton data
grouped by spatial regions, then generates continuous imputed density values
for arbitrary date-location lookups.
"""

from __future__ import annotations

import logging
from typing import Any, Sequence

import numpy as np
import pandas as pd
from pandas import DataFrame, Series, Timestamp

logger = logging.getLogger(__name__)

# ── Default latitude-band region definitions ────────────────────────────────
# Each region is a (label, lat_min, lat_max) tuple.
# These cover the SE coast from South Carolina through Virginia.
DEFAULT_REGIONS: list[tuple[str, float, float]] = [
    ("SC", 32.0, 33.5),
    ("NC-south", 33.5, 34.75),
    ("NC-north", 34.75, 36.25),
    ("VA", 36.25, 38.0),
]


def assign_region(
    latitudes: Series,
    regions: Sequence[tuple[str, float, float]] | None = None,
) -> Series:
    """Assign a region label to each observation based on its latitude.

    Args:
        latitudes: Series of latitude values (decimal degrees).
        regions: List of (label, lat_min, lat_max) tuples defining each region.
            If None, uses DEFAULT_REGIONS.

    Returns:
        Series of region labels (str). Observations outside all bands get NaN.
    """
    if regions is None:
        regions = DEFAULT_REGIONS

    labels = pd.Series(np.nan, index=latitudes.index, dtype="object")

    for label, lat_min, lat_max in regions:
        mask = (latitudes >= lat_min) & (latitudes < lat_max)
        labels[mask] = label

    return labels


def prepare_prophet_series(
    df: DataFrame,
    region: str,
    region_col: str = "region",
    date_col: str = "date",
    value_col: str = "VALUE-per-volu-F1",
    freq: str = "W",
) -> DataFrame:
    """Filter plankton data to a region and aggregate to a regular time series.

    Args:
        df: Plankton DataFrame with region labels, dates, and value columns.
        region: The region label to filter to.
        region_col: Name of the column containing region labels.
        date_col: Name of the column containing datetime values.
        value_col: Name of the plankton density column to aggregate.
        freq: Pandas frequency string for resampling ('W' = weekly, 'MS' = monthly).

    Returns:
        DataFrame with columns 'ds' (datetime) and 'y' (mean plankton density),
        ready for Prophet. Missing time periods will have NaN in 'y'.
    """
    region_df = df[df[region_col] == region].copy()

    if region_df.empty:
        logger.warning("No data found for region '%s'", region)
        return DataFrame(columns=["ds", "y"])

    # Ensure the date column is datetime
    region_df[date_col] = pd.to_datetime(region_df[date_col], errors="coerce")

    # Ensure the value column is numeric
    region_df[value_col] = pd.to_numeric(region_df[value_col], errors="coerce")

    # Resample to regular intervals, taking the mean
    ts = (
        region_df
        .set_index(date_col)
        .resample(freq)[value_col]
        .mean()
        .reset_index()
    )

    ts.columns = ["ds", "y"]

    return ts


def fit_and_impute(
    prophet_df: DataFrame,
    full_date_range: pd.DatetimeIndex | None = None,
    freq: str = "W",
    prophet_kwargs: dict[str, Any] | None = None,
) -> DataFrame:
    """Fit a Prophet model and generate predictions over a full date range.

    Args:
        prophet_df: DataFrame with 'ds' and 'y' columns (from prepare_prophet_series).
        full_date_range: DatetimeIndex to generate predictions for. If None,
            uses the min/max dates from prophet_df.
        freq: Frequency string for the prediction range (must match aggregation).
        prophet_kwargs: Additional keyword arguments passed to Prophet().

    Returns:
        DataFrame with columns: ds, yhat, yhat_lower, yhat_upper.
    """
    from prophet import Prophet

    if prophet_df.empty or prophet_df["y"].dropna().shape[0] < 2:
        logger.warning("Not enough data points to fit a Prophet model.")
        return DataFrame(columns=["ds", "yhat", "yhat_lower", "yhat_upper"])

    kwargs: dict[str, Any] = {
        "yearly_seasonality": True,
        "weekly_seasonality": False,
        "daily_seasonality": False,
    }
    if prophet_kwargs:
        kwargs.update(prophet_kwargs)

    model = Prophet(**kwargs)

    # Prophet needs non-NaN rows for fitting
    train_df = prophet_df.dropna(subset=["y"]).copy()
    model.fit(train_df)

    # Build the future dataframe
    if full_date_range is not None:
        future = DataFrame({"ds": full_date_range})
    else:
        periods = (
            (prophet_df["ds"].max() - prophet_df["ds"].min())
            / pd.tseries.frequencies.to_offset(freq)
        )
        future = model.make_future_dataframe(periods=int(periods), freq=freq)

    forecast = model.predict(future)

    # Clamp predictions to non-negative (plankton density can't be negative)
    forecast["yhat"] = forecast["yhat"].clip(lower=0)
    forecast["yhat_lower"] = forecast["yhat_lower"].clip(lower=0)

    return forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]]


def build_plankton_lookup(
    df: DataFrame,
    value_col: str = "VALUE-per-volu-F1",
    date_col: str = "date",
    lat_col: str = "LATITUDE",
    regions: Sequence[tuple[str, float, float]] | None = None,
    freq: str = "W",
    prophet_kwargs: dict[str, Any] | None = None,
) -> DataFrame:
    """Build a full imputed plankton density lookup table.

    Orchestrates the pipeline: assign regions → aggregate per region →
    fit Prophet per region → combine into a single lookup table.

    Args:
        df: Cleaned plankton DataFrame (e.g., from copepod_dataset_se_coast.parquet).
        value_col: Plankton density column to model.
        date_col: Date column name.
        lat_col: Latitude column name.
        regions: Region definitions (see assign_region).
        freq: Resampling frequency.
        prophet_kwargs: Additional Prophet configuration.

    Returns:
        DataFrame with columns: ds, region, yhat, yhat_lower, yhat_upper.
    """
    if regions is None:
        regions = DEFAULT_REGIONS

    # Assign regions
    df = df.copy()
    df["region"] = assign_region(df[lat_col], regions)
    df = df.dropna(subset=["region"])

    # Determine the full date range across all data
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    global_min = df[date_col].min()
    global_max = df[date_col].max()
    full_range = pd.date_range(start=global_min, end=global_max, freq=freq)

    all_forecasts: list[DataFrame] = []

    for label, _, _ in regions:
        logger.info("Fitting Prophet model for region: %s", label)

        prophet_series = prepare_prophet_series(
            df,
            region=label,
            date_col=date_col,
            value_col=value_col,
            freq=freq,
        )

        forecast = fit_and_impute(
            prophet_series,
            full_date_range=full_range,
            freq=freq,
            prophet_kwargs=prophet_kwargs,
        )

        if not forecast.empty:
            forecast["region"] = label
            all_forecasts.append(forecast)

    if not all_forecasts:
        logger.warning("No forecasts were generated for any region.")
        return DataFrame(columns=["ds", "region", "yhat", "yhat_lower", "yhat_upper"])

    lookup = pd.concat(all_forecasts, ignore_index=True)
    return lookup


def get_plankton_density(
    lookup: DataFrame,
    date: Timestamp | str,
    lat: float,
    regions: Sequence[tuple[str, float, float]] | None = None,
) -> float | None:
    """Look up the imputed plankton density for a given date and latitude.

    Args:
        lookup: The lookup table from build_plankton_lookup.
        date: The date to look up.
        lat: Latitude of the location.
        regions: Region definitions (must match those used to build the lookup).

    Returns:
        The imputed plankton density (yhat), or None if no match is found.
    """
    if regions is None:
        regions = DEFAULT_REGIONS

    # Find the region for this latitude
    region_label = None
    for label, lat_min, lat_max in regions:
        if lat_min <= lat < lat_max:
            region_label = label
            break

    if region_label is None:
        return None

    date = pd.Timestamp(date)

    # Filter to this region
    region_data = lookup[lookup["region"] == region_label]
    if region_data.empty:
        return None

    # Find the nearest date
    diffs = (region_data["ds"] - date).abs()
    nearest_idx = diffs.idxmin()

    return float(region_data.loc[nearest_idx, "yhat"])


def merge_plankton_to_strandings(
    strandings_df: DataFrame,
    lookup: DataFrame,
    stranding_date_col: str = "mms_observation_dt",
    stranding_lat_col: str = "Latitude",
    regions: Sequence[tuple[str, float, float]] | None = None,
) -> DataFrame:
    """Merge imputed plankton density into a strandings DataFrame.

    For each stranding event, looks up the nearest-date plankton density
    from the corresponding spatial region.

    Args:
        strandings_df: Strandings DataFrame.
        lookup: Plankton lookup table from build_plankton_lookup.
        stranding_date_col: Date column in strandings data.
        stranding_lat_col: Latitude column in strandings data.
        regions: Region definitions.

    Returns:
        Copy of strandings_df with new columns: plankton_region, plankton_density,
        plankton_density_lower, plankton_density_upper.
    """
    result = strandings_df.copy()

    result["plankton_region"] = assign_region(
        result[stranding_lat_col], regions
    )

    densities = []
    lowers = []
    uppers = []

    for _, row in result.iterrows():
        region = row["plankton_region"]
        date = pd.Timestamp(row[stranding_date_col])

        if pd.isna(region):
            densities.append(np.nan)
            lowers.append(np.nan)
            uppers.append(np.nan)
            continue

        region_data = lookup[lookup["region"] == region]
        if region_data.empty:
            densities.append(np.nan)
            lowers.append(np.nan)
            uppers.append(np.nan)
            continue

        diffs = (region_data["ds"] - date).abs()
        nearest_idx = diffs.idxmin()
        densities.append(float(region_data.loc[nearest_idx, "yhat"]))
        lowers.append(float(region_data.loc[nearest_idx, "yhat_lower"]))
        uppers.append(float(region_data.loc[nearest_idx, "yhat_upper"]))

    result["plankton_density"] = densities
    result["plankton_density_lower"] = lowers
    result["plankton_density_upper"] = uppers

    return result
