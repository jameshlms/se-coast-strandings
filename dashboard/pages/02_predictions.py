from __future__ import annotations

import logging

import pandas as pd
import pydeck as pdk
import streamlit as st

try:
    from dashboard.utils.data_loader import (
        compare_feature_schemas,
        get_baseline_features,
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
except ModuleNotFoundError:
    from utils.data_loader import (
        compare_feature_schemas,
        get_baseline_features,
        get_metrics_features,
        get_model_features,
        load_baseline_model,
        load_lgbm_model,
        load_metrics,
        load_plankton_lookup,
        load_weekly_data,
    )
    from utils.feature_builder import build_feature_frame_for_week
    from utils.map_helpers import build_prediction_map_points
from se_coast_strandings.regions import make_degrees

logger = logging.getLogger(__name__)


def _predict(model, feature_frame: pd.DataFrame) -> pd.Series:
    try:
        preds = model.predict(feature_frame)
    except Exception:
        preds = model.predict(feature_frame.values)
    return pd.Series(preds, index=feature_frame.index, dtype="float64")


def _hex_to_rgb(value: str) -> list[int]:
    color = str(value).strip().lstrip("#")
    if len(color) != 6:
        return [27, 120, 55]
    try:
        return [int(color[i : i + 2], 16) for i in (0, 2, 4)]
    except ValueError:
        return [27, 120, 55]


def _current_theme_type() -> str:
    try:
        runtime_theme = st.context.theme
        runtime_type = str(runtime_theme.get("type") or "").strip().lower()
        if runtime_type in {"light", "dark"}:
            return runtime_type
    except Exception:
        pass
    config_theme = str(st.get_option("theme.base") or "").strip().lower()
    return "dark" if config_theme == "dark" else "light"


def _map_style_for_theme() -> str:
    return "dark" if _current_theme_type() == "dark" else "light"


def _tooltip_style_for_theme() -> dict[str, str]:
    if _current_theme_type() == "dark":
        return {"backgroundColor": "#0f172a", "color": "#f8fafc"}
    return {"backgroundColor": "#f8fafc", "color": "#0f172a"}


def _fmt_float(value: object) -> str:
    if pd.isna(value):
        return "N/A"
    return f"{float(value):.2f}"


def _format_mdy(value: pd.Timestamp) -> str:
    ts = pd.Timestamp(value)
    return f"{ts.month}/{ts.day}/{ts.year}"


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
    st.subheader("Model Prediction Map")

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
    baseline_features = get_baseline_features(metrics) or model_features
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

    with st.sidebar:
        st.subheader("Prediction Controls")
        selected_date = st.date_input(
            "Select week",
            value=default_week.date(),
            min_value=min_week.date(),
            max_value=(max_week + pd.Timedelta(weeks=26)).date(),
        )
        week_start = _snap_to_week_start(pd.Timestamp(selected_date))
        st.caption(f"Week start: {_format_mdy(week_start)}")
        show_baseline = st.toggle(
            "Show baseline prediction",
            value=baseline_model is not None,
            disabled=baseline_model is None,
        )

    all_features = list(dict.fromkeys([*model_features, *baseline_features]))
    feature_df = build_feature_frame_for_week(
        week_start=week_start,
        regions=regions,
        weekly_history=weekly,
        plankton_lookup=plankton_lookup,
        model_features=all_features,
    )

    predictions = _predict(model, feature_df[model_features])
    if baseline_model is not None:
        baseline_predictions = _predict(baseline_model, feature_df[baseline_features])
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
    map_points = map_points.reset_index(drop=True).copy()
    map_points["color_rgb"] = map_points["color"].apply(_hex_to_rgb)
    map_points["radius_px"] = pd.to_numeric(map_points["size"], errors="coerce").fillna(10.0)
    map_points["region_display"] = map_points["region"].astype(str)
    map_points["predicted_display"] = map_points["predicted"].apply(_fmt_float)
    map_points["baseline_display"] = map_points["baseline"].apply(_fmt_float)
    map_points["actual_display"] = map_points["actual"].apply(_fmt_float)
    map_points["abs_error_display"] = map_points["abs_error"].apply(_fmt_float)
    map_points["lat_band_display"] = map_points.apply(
        lambda row: (
            f"{float(row['lat_min']):.2f} to {float(row['lat_max']):.2f}"
            if pd.notna(row["lat_min"]) and pd.notna(row["lat_max"])
            else "N/A"
        ),
        axis=1,
    )

    map_layer = pdk.Layer(
        "ScatterplotLayer",
        data=map_points,
        get_position=["longitude", "latitude"],
        get_fill_color="color_rgb",
        get_radius="radius_px",
        radius_units="pixels",
        radius_min_pixels=6,
        radius_max_pixels=34,
        pickable=True,
        auto_highlight=True,
        stroked=True,
        get_line_color=[17, 24, 39, 220],
        line_width_min_pixels=1,
    )
    center_lat = float(pd.to_numeric(map_points["latitude"], errors="coerce").mean())
    deck = pdk.Deck(
        layers=[map_layer],
        initial_view_state=pdk.ViewState(
            latitude=center_lat,
            longitude=-76.5,
            zoom=6,
            pitch=0,
        ),
        map_style=_map_style_for_theme(),
        tooltip={
            "html": (
                "<div style='padding:8px 10px'>"
                "<div style='font-size:14px;font-weight:700;margin-bottom:6px'>{region_display}</div>"
                "<div><b>Prediction:</b> {predicted_display}</div>"
                "<div><b>Baseline:</b> {baseline_display}</div>"
                "<div><b>Actual:</b> {actual_display}</div>"
                "<div><b>Absolute error:</b> {abs_error_display}</div>"
                "<div><b>Latitude band:</b> {lat_band_display}</div>"
                "</div>"
            ),
            "style": {
                **_tooltip_style_for_theme(),
                "maxWidth": "320px",
                "fontSize": "13px",
                "lineHeight": "1.35",
            },
        },
    )
    st.pydeck_chart(deck, height=550)

    with st.sidebar:
        st.markdown("---")
        st.subheader("Region Drill Down")
        selected_region = st.selectbox(
            "Region",
            options=region_frame["region"].astype(str).tolist(),
            index=0,
        )

        detail = region_frame[region_frame["region"] == selected_region].iloc[0]
        st.write(f"LightGBM prediction: **{float(detail['predicted']):.2f}**")
        if show_baseline:
            st.write(f"Baseline prediction: **{float(detail['baseline']):.2f}**")
        if pd.notna(detail["actual"]):
            abs_error = abs(float(detail["predicted"]) - float(detail["actual"]))
            st.write(f"Actual count: **{float(detail['actual']):.2f}**")
            st.write(f"Absolute error: **{abs_error:.2f}**")

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
