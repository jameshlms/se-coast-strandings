from __future__ import annotations

import logging

import pandas as pd
import streamlit as st

from dashboard.utils.data_loader import (
    compare_feature_schemas,
    get_metrics_features,
    get_model_features,
    load_baseline_model,
    load_lgbm_model,
    load_metrics,
    load_plankton_lookup,
    load_weekly_data,
)
from dashboard.utils.feature_builder import build_feature_frame_for_week
from dashboard.utils.map_helpers import build_prediction_map_points
from se_coast_strandings.regions import make_degrees

logger = logging.getLogger(__name__)


def _predict(model, feature_frame: pd.DataFrame) -> pd.Series:
    try:
        preds = model.predict(feature_frame)
    except Exception:
        preds = model.predict(feature_frame.values)
    return pd.Series(preds, index=feature_frame.index, dtype="float64")


def _hide_sidebar_for_prediction_map() -> None:
    if st.session_state.get("_navigation_mode") != "navigation":
        return
    st.markdown(
        """
        <style>
        section[data-testid="stSidebar"] { display: none; }
        button[data-testid="collapsedControl"] { display: none; }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _snap_to_week_start(value: pd.Timestamp) -> pd.Timestamp:
    value = pd.Timestamp(value).normalize()
    return value - pd.Timedelta(days=int(value.dayofweek))


def _region_bounds(regions: list[str]) -> pd.DataFrame:
    default = {label: (lat_min, lat_max) for label, lat_min, lat_max in make_degrees(0.5)}
    rows = []
    for i, region in enumerate(regions):
        if region in default:
            lat_min, lat_max = default[region]
        else:
            lat_min = 32.0 + (i * (6.0 / max(len(regions), 1)))
            lat_max = 32.0 + ((i + 1) * (6.0 / max(len(regions), 1)))
        rows.append({"region": region, "lat_min": lat_min, "lat_max": lat_max})
    return pd.DataFrame(rows)


def _weekly_sparkline(
    region: str,
    week_start: pd.Timestamp,
    weekly: pd.DataFrame,
    plankton_lookup: pd.DataFrame,
    model,
    model_features: list[str],
) -> pd.DataFrame:
    weeks = [week_start - pd.Timedelta(weeks=delta) for delta in range(7, -1, -1)]
    rows = []

    region_list = sorted(weekly["region"].dropna().astype(str).unique().tolist())
    if region not in region_list:
        region_list.append(region)

    for week in weeks:
        features = build_feature_frame_for_week(
            week_start=week,
            regions=region_list,
            weekly_history=weekly,
            plankton_lookup=plankton_lookup,
            model_features=model_features,
        )
        pred = float(_predict(model, features.loc[[region], model_features]).iloc[0])

        actual_match = weekly[
            (weekly["region"] == region) & (weekly["week_start"] == week)
        ]
        actual = float(actual_match.iloc[0]["stranding_count"]) if not actual_match.empty else float("nan")
        rows.append({"week_start": week, "predicted": pred, "actual": actual})

    return pd.DataFrame(rows).set_index("week_start")


def main() -> None:
    _hide_sidebar_for_prediction_map()
    st.title("Model Prediction Map")

    try:
        weekly = load_weekly_data()
        plankton_lookup = load_plankton_lookup()
    except FileNotFoundError as exc:
        logger.warning("Prediction input artifact(s) missing: %s", exc)
        st.error(str(exc))
        st.info(
            "Run `python3 scripts/run_notebooks_end_to_end.py` to regenerate data/model artifacts."
        )
        return

    try:
        model = load_lgbm_model()
    except FileNotFoundError as exc:
        logger.warning("LightGBM model artifact missing: %s", exc)
        st.error(str(exc))
        st.info(
            "Run notebook `09_full_model.ipynb` (or `python3 scripts/run_notebooks_end_to_end.py`) "
            "to generate `models/lgbm_model.pkl`."
        )
        return

    baseline_model = None
    try:
        baseline_model = load_baseline_model()
    except FileNotFoundError as exc:
        logger.warning("Baseline model artifact missing: %s", exc)
        st.info(
            "Baseline model artifact missing; baseline overlay is disabled for this session."
        )
    try:
        metrics = load_metrics()
    except FileNotFoundError as exc:
        logger.warning("Metrics artifact(s) missing: %s", exc)
        st.info("Model metrics JSON artifacts were not found. Continuing without schema cross-check.")
        metrics = {}

    model_features = get_model_features(model)
    metrics_features = get_metrics_features(metrics)
    schema_diff = compare_feature_schemas(model_features, metrics_features)

    if schema_diff["model_only"] or schema_diff["metrics_only"]:
        logger.warning("Feature schema mismatch between model and metrics JSON.")
        st.warning(
            "Feature schema mismatch detected between model and full_model_metrics.json. "
            "Continuing with model.feature_name() as canonical order."
        )

    if not {"region", "week_start", "stranding_count"}.issubset(weekly.columns):
        st.error("final_dataset.parquet must contain region, week_start, and stranding_count columns.")
        return

    weekly["week_start"] = pd.to_datetime(weekly["week_start"])
    regions = sorted(weekly["region"].dropna().astype(str).unique().tolist())
    if not regions:
        regions = [label for label, _, _ in make_degrees(0.5)]

    min_week = _snap_to_week_start(weekly["week_start"].min())
    max_week = _snap_to_week_start(weekly["week_start"].max())
    default_week = _snap_to_week_start(max_week + pd.Timedelta(weeks=1))

    selected_date = st.date_input(
        "Select week",
        value=default_week.date(),
        min_value=min_week.date(),
        max_value=(max_week + pd.Timedelta(weeks=26)).date(),
    )
    week_start = _snap_to_week_start(pd.Timestamp(selected_date))

    show_baseline = st.toggle(
        "Show baseline prediction",
        value=baseline_model is not None,
        disabled=baseline_model is None,
    )

    feature_df = build_feature_frame_for_week(
        week_start=week_start,
        regions=regions,
        weekly_history=weekly,
        plankton_lookup=plankton_lookup,
        model_features=model_features,
    )

    predictions = _predict(model, feature_df[model_features])
    if baseline_model is not None:
        baseline_predictions = _predict(baseline_model, feature_df[model_features])
    else:
        baseline_predictions = pd.Series(float("nan"), index=predictions.index, dtype="float64")

    region_frame = _region_bounds(regions)
    region_frame["predicted"] = region_frame["region"].map(predictions)
    region_frame["baseline"] = region_frame["region"].map(baseline_predictions)

    actual = (
        weekly[weekly["week_start"] == week_start]
        .groupby("region", as_index=True)["stranding_count"]
        .sum()
    )
    region_frame["actual"] = region_frame["region"].map(actual)

    map_points = build_prediction_map_points(region_frame)
    if map_points.empty:
        st.warning("No mapped regions are available.")
        return
    st.map(
        map_points,
        latitude="latitude",
        longitude="longitude",
        size="size",
        color="color",
        zoom=6,
        height=550,
    )

    selected_region = st.selectbox(
        "Drill down region",
        options=region_frame["region"].astype(str).tolist(),
        index=0,
    )

    detail = region_frame[region_frame["region"] == selected_region].iloc[0]
    st.subheader(f"Region {selected_region} details")
    st.write(f"LightGBM prediction: **{float(detail['predicted']):.2f}**")
    if show_baseline:
        st.write(f"Baseline prediction: **{float(detail['baseline']):.2f}**")

    if pd.notna(detail["actual"]):
        abs_error = abs(float(detail["predicted"]) - float(detail["actual"]))
        st.write(f"Actual count: **{float(detail['actual']):.2f}** | Absolute error: **{abs_error:.2f}**")

    st.caption("Input feature vector used for this region/week")
    st.dataframe(
        feature_df.loc[[selected_region], model_features].T.rename(columns={selected_region: "value"}),
        use_container_width=True,
    )

    sparkline = _weekly_sparkline(
        region=selected_region,
        week_start=week_start,
        weekly=weekly,
        plankton_lookup=plankton_lookup,
        model=model,
        model_features=model_features,
    )
    st.caption("8-week prediction vs. actual")
    st.line_chart(sparkline)


if __name__ == "__main__":
    main()
