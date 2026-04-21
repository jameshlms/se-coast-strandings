from __future__ import annotations

from html import escape

import pandas as pd
import pydeck as pdk
import streamlit as st

try:
    from dashboard.utils.data_loader import load_historical_events
    from dashboard.utils.map_helpers import build_historical_map_points
except ModuleNotFoundError:
    from utils.data_loader import load_historical_events
    from utils.map_helpers import build_historical_map_points
from se_coast_strandings.contextual_data.lunar_phases import moon_age, moon_phase
from se_coast_strandings.regions import make_degrees

MAP_LAYER_ID = "historical-points"
LIGHT_MAP_STYLE = "light"
DARK_MAP_STYLE = "dark"
MIN_ZOOM = 5.0
MAX_ZOOM = 10.8
DEFAULT_COAST_CENTER_LAT = 34.9
DEFAULT_COAST_CENTER_LON = -78.2
DEFAULT_COAST_ZOOM = 5.8
ZOOM_IN_TRANSITION_MS = 700
ZOOM_OUT_TRANSITION_MS = 850


def _pick_column(df: pd.DataFrame, choices: list[str]) -> str | None:
    for col in choices:
        if col in df.columns:
            return col
    return None


def _infer_temperature_unit(series: pd.Series) -> str:
    values = pd.to_numeric(series, errors="coerce").dropna()
    if values.empty:
        return "celsius"
    return "fahrenheit" if float(values.quantile(0.95)) > 60 else "celsius"


def _series_to_fahrenheit(values: pd.Series, source_unit: str) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce")
    if source_unit == "fahrenheit":
        return numeric
    return (numeric * 9.0 / 5.0) + 32.0


def _hex_to_rgb(value: str) -> list[int]:
    color = str(value).strip().lstrip("#")
    if len(color) != 6:
        return [27, 120, 55]
    try:
        return [int(color[i : i + 2], 16) for i in (0, 2, 4)]
    except ValueError:
        return [27, 120, 55]


def _region_label(lat: float) -> str | None:
    for label, lat_min, lat_max in make_degrees(0.5):
        if lat_min <= lat < lat_max:
            return label
    return None


def _tooltip_text(value: object, fallback: str = "Unknown") -> str:
    if value is None:
        return fallback
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return fallback
    return escape(text)


def _tooltip_temp(values: pd.Series) -> pd.Series:
    return values.map(lambda v: f"{float(v):.1f} F" if pd.notna(v) else "N/A")


def _default_zoom_for_extent(points: pd.DataFrame) -> float:
    lat = pd.to_numeric(points["latitude"], errors="coerce")
    lon = pd.to_numeric(points["longitude"], errors="coerce")
    lat_span = float((lat.max() - lat.min()) if not lat.empty else 0.0)
    lon_span = float((lon.max() - lon.min()) if not lon.empty else 0.0)
    span = max(lat_span, lon_span, 0.001)
    zoom = 7.0 - (span * 0.35)
    zoom = max(zoom, DEFAULT_COAST_ZOOM)
    return float(min(max(zoom, MIN_ZOOM + 0.2), 7.2))


def _default_center_for_extent(points: pd.DataFrame) -> tuple[float, float]:
    lat = pd.to_numeric(points["latitude"], errors="coerce").dropna()
    lon = pd.to_numeric(points["longitude"], errors="coerce").dropna()
    if lat.empty or lon.empty:
        return DEFAULT_COAST_CENTER_LAT, DEFAULT_COAST_CENTER_LON
    center_lat = float(lat.clip(lower=31.0, upper=38.5).mean())
    center_lon = float(lon.clip(lower=-82.5, upper=-74.0).mean())
    return center_lat, center_lon


def _map_style_for_theme() -> str:
    return DARK_MAP_STYLE if _current_theme_type() == "dark" else LIGHT_MAP_STYLE


def _tooltip_style_for_theme() -> dict[str, str]:
    if _current_theme_type() == "dark":
        return {"backgroundColor": "#0f172a", "color": "#f8fafc"}
    return {"backgroundColor": "#f8fafc", "color": "#0f172a"}


def _current_theme_type() -> str:
    # Streamlit user theme is runtime/session-specific; prefer context over config.
    try:
        runtime_theme = st.context.theme
        runtime_type = str(runtime_theme.get("type") or "").strip().lower()
        if runtime_type in {"light", "dark"}:
            return runtime_type
    except Exception:
        pass
    config_theme = str(st.get_option("theme.base") or "").strip().lower()
    return "dark" if config_theme == "dark" else "light"


def _format_mdy(value: object) -> str:
    ts = pd.Timestamp(value)
    return f"{ts.month}/{ts.day}/{ts.year}"


def _selected_index_from_event(event_state: object) -> int | None:
    try:
        selection = event_state.get("selection", {})
        indices_by_layer = selection.get("indices", {})
        layer_indices = indices_by_layer.get(MAP_LAYER_ID, [])
        if layer_indices:
            return int(layer_indices[0])
    except Exception:
        return None
    return None


def _sticky_card_html(selected: pd.Series, theme_type: str) -> str:
    if theme_type == "dark":
        card_bg = "rgba(15, 23, 42, 0.94)"
        card_fg = "#f8fafc"
        card_border = "rgba(148, 163, 184, 0.35)"
    else:
        card_bg = "rgba(248, 250, 252, 0.96)"
        card_fg = "#0f172a"
        card_border = "rgba(15, 23, 42, 0.2)"
    return (
        "<div style='margin-top:-230px; position:relative; z-index:25; padding-left:14px; pointer-events:none;'>"
        f"<div style='pointer-events:auto; width:min(360px, 92%); border-radius:12px; border:1px solid {card_border}; "
        f"background:{card_bg}; color:{card_fg}; box-shadow:0 10px 28px rgba(2, 6, 23, 0.35); "
        "padding:12px 14px; font-size:13px; line-height:1.35;'>"
        f"<div style='font-size:16px; font-weight:700; margin-bottom:8px;'>{selected['point_species']}</div>"
        f"<div><b>Date:</b> {selected['point_date']}</div>"
        f"<div><b>State:</b> {selected['point_state']}</div>"
        f"<div><b>Condition:</b> {selected['point_condition']}</div>"
        f"<div><b>Coordinates:</b> {selected['point_latitude']}, {selected['point_longitude']}</div>"
        f"<div><b>Moon phase:</b> {selected['point_moon_phase']}</div>"
        f"<div><b>Moon age:</b> {selected['point_moon_age']}</div>"
        f"<div><b>Max temp:</b> {selected['point_weather_max_f']}</div>"
        f"<div><b>Min temp:</b> {selected['point_weather_min_f']}</div>"
        f"<div><b>Region band:</b> {selected['point_region']}</div>"
        "</div></div>"
    )


def main() -> None:
    st.markdown(
        "<h3 style='margin:0 0 0.25rem 0; font-size:1.55rem;'>Historical Event Explorer</h3>",
        unsafe_allow_html=True,
    )

    try:
        events = load_historical_events()
    except FileNotFoundError as exc:
        st.error(str(exc))
        st.info(
            "To generate artifacts, run `python3 scripts/run_notebooks_end_to_end.py` from the repo root. "
            "If raw source data is private/unavailable, place "
            "`UNC-DataRequest-01302026.xlsx` in `data/raw/` first."
        )
        return
    if events.empty:
        st.warning("No historical events were loaded.")
        return

    date_col = _pick_column(events, ["mms_observation_dt", "date", "observation_date"])
    species_col = _pick_column(events, ["common_name", "Common Name", "species", "Species", "species_name"])
    state_col = _pick_column(events, ["state", "State", "st", "STATE"])
    condition_col = _pick_column(events, ["condition", "Condition at Examination", "ccode"])
    estimated_col = _pick_column(
        events,
        ["is_estimated_coordinates", "estimated_coordinates", "estimated_coords"],
    )
    moon_phase_col = _pick_column(events, ["moon_phase", "Moon Phase"])
    weather_max_col = _pick_column(events, ["temperature_2m_max_0_days_prior"])
    weather_min_col = _pick_column(events, ["temperature_2m_min_0_days_prior"])

    temperature_source_unit = (
        _infer_temperature_unit(events[weather_max_col])
        if weather_max_col is not None
        else "celsius"
    )

    filtered = events.copy()
    date_filter_signature: tuple[str, str] | None = None
    with st.sidebar:
        st.subheader("Filters")

        if date_col is not None:
            filtered[date_col] = pd.to_datetime(filtered[date_col], errors="coerce")
            min_date = filtered[date_col].min()
            max_date = filtered[date_col].max()
            if pd.notna(min_date) and pd.notna(max_date):
                min_date_only = min_date.date()
                max_date_only = max_date.date()
                st.caption(
                    f"Available dates: {_format_mdy(min_date_only)} - {_format_mdy(max_date_only)}"
                )
                default_end = max_date_only
                default_start = min_date_only
                quick_range = st.selectbox(
                    "Quick date range",
                    options=["All dates", "Last 12 months", "Last 3 years", "Custom"],
                    index=0,
                )
                if quick_range == "Last 12 months":
                    default_start = max((max_date - pd.DateOffset(months=12)).date(), min_date_only)
                elif quick_range == "Last 3 years":
                    default_start = max((max_date - pd.DateOffset(years=3)).date(), min_date_only)

                start_col, end_col = st.columns(2)
                selected_start = start_col.date_input(
                    "Start date",
                    value=default_start,
                    min_value=min_date_only,
                    max_value=max_date_only,
                    format="MM/DD/YYYY",
                )
                selected_end = end_col.date_input(
                    "End date",
                    value=default_end,
                    min_value=min_date_only,
                    max_value=max_date_only,
                    format="MM/DD/YYYY",
                )
                if selected_start > selected_end:
                    selected_start, selected_end = selected_end, selected_start
                date_filter_signature = (
                    selected_start.isoformat(),
                    selected_end.isoformat(),
                )
                start_date = pd.Timestamp(selected_start).normalize()
                end_date = pd.Timestamp(selected_end).normalize() + pd.Timedelta(days=1)
                filtered = filtered[
                    (filtered[date_col] >= start_date) & (filtered[date_col] < end_date)
                ]

        if state_col is not None:
            state_opts = sorted(filtered[state_col].dropna().astype(str).unique().tolist())
            selected_states = st.multiselect(
                "State",
                options=state_opts,
                placeholder="All states",
            )
            if selected_states:
                filtered = filtered[filtered[state_col].astype(str).isin(selected_states)]

        if species_col is not None:
            species_opts = sorted(filtered[species_col].dropna().astype(str).unique().tolist())
            selected_species = st.multiselect(
                "Common name",
                options=species_opts,
                placeholder="All species",
            )
            if selected_species:
                filtered = filtered[filtered[species_col].astype(str).isin(selected_species)]

        if moon_phase_col is not None:
            moon_opts = sorted(filtered[moon_phase_col].dropna().astype(str).unique().tolist())
            selected_moon = st.multiselect(
                "Moon phase",
                options=moon_opts,
                placeholder="All phases",
            )
            if selected_moon:
                filtered = filtered[filtered[moon_phase_col].astype(str).isin(selected_moon)]

        if weather_max_col is not None:
            max_series_f = _series_to_fahrenheit(
                filtered[weather_max_col],
                source_unit=temperature_source_unit,
            ).dropna()
            if not max_series_f.empty:
                max_low = float(max_series_f.min())
                max_high = float(max_series_f.max())
                weather_max_range = st.slider(
                    "Max temp on event date (F)",
                    min_value=max_low,
                    max_value=max_high,
                    value=(max_low, max_high),
                )
                filtered_max_f = _series_to_fahrenheit(
                    filtered[weather_max_col],
                    source_unit=temperature_source_unit,
                )
                filtered = filtered[
                    filtered_max_f.between(weather_max_range[0], weather_max_range[1])
                ]

        if weather_min_col is not None:
            min_series_f = _series_to_fahrenheit(
                filtered[weather_min_col],
                source_unit=temperature_source_unit,
            ).dropna()
            if not min_series_f.empty:
                min_low = float(min_series_f.min())
                min_high = float(min_series_f.max())
                weather_min_range = st.slider(
                    "Min temp on event date (F)",
                    min_value=min_low,
                    max_value=min_high,
                    value=(min_low, min_high),
                )
                filtered_min_f = _series_to_fahrenheit(
                    filtered[weather_min_col],
                    source_unit=temperature_source_unit,
                )
                filtered = filtered[
                    filtered_min_f.between(weather_min_range[0], weather_min_range[1])
                ]

        if condition_col is not None:
            cond_opts = sorted(filtered[condition_col].dropna().astype(str).unique().tolist())
            selected_conditions = st.multiselect(
                "Condition",
                options=cond_opts,
                placeholder="All conditions",
            )
            if selected_conditions:
                filtered = filtered[filtered[condition_col].astype(str).isin(selected_conditions)]

        only_actual = st.checkbox("Only events with actual coordinates", value=False)
        if only_actual and estimated_col is not None:
            filtered = filtered[~filtered[estimated_col].fillna(False)]

    st.caption(f"Showing {len(filtered):,} events")
    map_points = build_historical_map_points(filtered)
    if map_points.empty:
        st.warning("No points available for the selected filters.")
        return

    drilldown = map_points.reset_index(drop=True).copy()
    event_dates = (
        pd.to_datetime(drilldown[date_col], errors="coerce")
        if date_col is not None
        else pd.Series(pd.NaT, index=drilldown.index)
    )

    drilldown["point_species"] = "Unknown"
    if species_col is not None:
        drilldown["point_species"] = drilldown[species_col].fillna("Unknown").astype(str)
    drilldown["point_species"] = drilldown["point_species"].map(_tooltip_text)

    drilldown["point_date"] = event_dates.dt.strftime("%Y-%m-%d").fillna("Unknown").map(_tooltip_text)

    drilldown["point_state"] = "Unknown"
    if state_col is not None:
        drilldown["point_state"] = drilldown[state_col].fillna("Unknown").astype(str)
    drilldown["point_state"] = drilldown["point_state"].map(_tooltip_text)

    drilldown["point_condition"] = "Unknown"
    if condition_col is not None:
        drilldown["point_condition"] = drilldown[condition_col].fillna("Unknown").astype(str)
    drilldown["point_condition"] = drilldown["point_condition"].map(_tooltip_text)

    drilldown["point_latitude"] = pd.to_numeric(
        drilldown["latitude"],
        errors="coerce",
    ).map(lambda lat: f"{float(lat):.4f}" if pd.notna(lat) else "N/A")
    drilldown["point_longitude"] = pd.to_numeric(
        drilldown["longitude"],
        errors="coerce",
    ).map(lambda lon: f"{float(lon):.4f}" if pd.notna(lon) else "N/A")

    moon_age_days = (
        pd.to_numeric(drilldown["moon_age"], errors="coerce")
        if "moon_age" in drilldown.columns
        else pd.Series(float("nan"), index=drilldown.index)
    )
    missing_moon_age = moon_age_days.isna() & event_dates.notna()
    if bool(missing_moon_age.any()):
        moon_age_fallback = event_dates.loc[missing_moon_age].apply(
            lambda d: float(moon_age(pd.Timestamp(d).normalize()))
        )
        moon_age_days.loc[missing_moon_age] = pd.to_numeric(
            moon_age_fallback,
            errors="coerce",
        ).to_numpy(dtype="float64")
    drilldown["point_moon_age"] = moon_age_days.map(
        lambda value: f"{float(value):.2f} days" if pd.notna(value) else "N/A"
    )

    moon_phase_values = pd.Series("Unknown", index=drilldown.index)
    if moon_phase_col is not None:
        moon_phase_values = drilldown[moon_phase_col].fillna("").astype(str)
    moon_phase_fallback = moon_age_days.map(
        lambda value: moon_phase(float(value)) if pd.notna(value) else "Unknown"
    )
    moon_phase_values = moon_phase_values.where(moon_phase_values.str.strip() != "", moon_phase_fallback)
    drilldown["point_moon_phase"] = moon_phase_values.map(_tooltip_text)

    weather_max_f = pd.Series(float("nan"), index=drilldown.index)
    if weather_max_col is not None:
        weather_max_f = _series_to_fahrenheit(
            drilldown[weather_max_col],
            source_unit=temperature_source_unit,
        )
    weather_min_f = pd.Series(float("nan"), index=drilldown.index)
    if weather_min_col is not None:
        weather_min_f = _series_to_fahrenheit(
            drilldown[weather_min_col],
            source_unit=temperature_source_unit,
        )

    drilldown["point_weather_max_f"] = _tooltip_temp(weather_max_f)
    drilldown["point_weather_min_f"] = _tooltip_temp(weather_min_f)
    drilldown["point_region"] = pd.to_numeric(drilldown["latitude"], errors="coerce").map(
        lambda lat: _region_label(float(lat)) if pd.notna(lat) else None
    )
    drilldown["point_region"] = drilldown["point_region"].fillna("Unknown").map(_tooltip_text)

    drilldown["color_rgb"] = drilldown["color"].apply(_hex_to_rgb)
    drilldown["radius"] = pd.to_numeric(drilldown["size"], errors="coerce").fillna(5.0) * 90.0
    drilldown["point_index"] = drilldown.index.astype(int)

    if "historical_selected_index" not in st.session_state:
        st.session_state["historical_selected_index"] = None
    previous_date_signature = st.session_state.get("historical_date_filter_signature")
    if previous_date_signature != date_filter_signature:
        st.session_state["historical_selected_index"] = None
    st.session_state["historical_date_filter_signature"] = date_filter_signature

    selected_index = st.session_state["historical_selected_index"]
    if selected_index is not None and not (0 <= int(selected_index) < len(drilldown)):
        selected_index = None
        st.session_state["historical_selected_index"] = None

    controls_col, _ = st.columns([1.1, 6])
    if controls_col.button("Reset map view", use_container_width=True):
        st.session_state["historical_selected_index"] = None
        st.rerun()

    base_layer = pdk.Layer(
        "ScatterplotLayer",
        data=drilldown,
        id=MAP_LAYER_ID,
        get_position=["longitude", "latitude"],
        get_fill_color="color_rgb",
        get_radius="radius",
        pickable=True,
        auto_highlight=True,
        radius_units="meters",
        radius_scale=0.8,
        radius_min_pixels=1,
        radius_max_pixels=3,
    )
    layers = [base_layer]

    if selected_index is not None:
        selected_row = drilldown.iloc[[int(selected_index)]].copy()
        selected_layer = pdk.Layer(
            "ScatterplotLayer",
            data=selected_row,
            id=f"{MAP_LAYER_ID}-selected",
            get_position=["longitude", "latitude"],
            get_fill_color=[220, 38, 38, 255],
            get_line_color=[127, 29, 29, 255],
            stroked=True,
            line_width_min_pixels=1,
            get_radius=8,
            radius_units="pixels",
            radius_min_pixels=8,
            radius_max_pixels=12,
            pickable=False,
        )
        layers.append(selected_layer)

    if selected_index is None:
        default_zoom = _default_zoom_for_extent(drilldown)
        center_lat, center_lon = _default_center_for_extent(drilldown)
        transition_duration_ms = ZOOM_OUT_TRANSITION_MS
    else:
        selected_row = drilldown.iloc[int(selected_index)]
        center_lat = float(pd.to_numeric(selected_row["latitude"], errors="coerce"))
        center_lon = float(pd.to_numeric(selected_row["longitude"], errors="coerce"))
        default_zoom = MAX_ZOOM
        transition_duration_ms = ZOOM_IN_TRANSITION_MS

    view_state = pdk.ViewState(
        latitude=center_lat,
        longitude=center_lon,
        zoom=default_zoom,
        min_zoom=MIN_ZOOM,
        max_zoom=MAX_ZOOM,
        pitch=0,
        transition_duration=transition_duration_ms,
        transition_interpolator={
            "@@type": "LinearInterpolator",
            "transitionProps": ["longitude", "latitude", "zoom"],
        },
    )
    tooltip_style = _tooltip_style_for_theme()
    deck = pdk.Deck(
        layers=layers,
        initial_view_state=view_state,
        map_style=_map_style_for_theme(),
        tooltip={
            "html": (
                "<div style='padding:8px 10px'>"
                "<div style='font-size:14px;font-weight:700;margin-bottom:6px'>{point_species}</div>"
                "<div><b>Date:</b> {point_date}</div>"
                "<div><b>State:</b> {point_state}</div>"
                "<div><b>Condition:</b> {point_condition}</div>"
                "<div><b>Coordinates:</b> {point_latitude}, {point_longitude}</div>"
                "<div><b>Moon phase:</b> {point_moon_phase}</div>"
                "<div><b>Moon age:</b> {point_moon_age}</div>"
                "<div><b>Max temp:</b> {point_weather_max_f}</div>"
                "<div><b>Min temp:</b> {point_weather_min_f}</div>"
                "<div><b>Region band:</b> {point_region}</div>"
                "</div>"
            ),
            "style": {
                **tooltip_style,
                "maxWidth": "340px",
                "maxHeight": "320px",
                "overflowY": "auto",
                "pointerEvents": "auto",
                "fontSize": "13px",
                "lineHeight": "1.35",
                "zIndex": "20",
            },
        },
    )
    deck.view_state = view_state
    map_key_suffix = "all-dates"
    if date_filter_signature is not None:
        map_key_suffix = f"{date_filter_signature[0]}-{date_filter_signature[1]}"
    chart_state = st.pydeck_chart(
        deck,
        height=660,
        on_select="rerun",
        selection_mode="single-object",
        key=f"historical-map-{_current_theme_type()}-{map_key_suffix}",
    )

    selected_from_event = _selected_index_from_event(chart_state)
    if selected_from_event is not None and selected_from_event != st.session_state["historical_selected_index"]:
        st.session_state["historical_selected_index"] = selected_from_event
        st.rerun()

    current_selected = st.session_state["historical_selected_index"]
    if current_selected is not None:
        selected = drilldown.iloc[int(current_selected)]
        st.markdown(
            _sticky_card_html(selected, _current_theme_type()),
            unsafe_allow_html=True,
        )
        # Restore normal layout flow after negative-margin overlay card.
        st.markdown("<div style='height:170px'></div>", unsafe_allow_html=True)


if __name__ == "__main__":
    main()
