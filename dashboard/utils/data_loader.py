from __future__ import annotations

import json
import pickle
from pathlib import Path
from typing import Any

import pandas as pd
try:
    import streamlit as st
except ModuleNotFoundError:  # pragma: no cover - fallback for non-dashboard test envs
    class _StreamlitStub:
        @staticmethod
        def cache_data(*args, **kwargs):
            def decorator(func):
                return func

            return decorator

        @staticmethod
        def cache_resource(*args, **kwargs):
            def decorator(func):
                return func

            return decorator

    st = _StreamlitStub()

ROOT_DIR = Path(__file__).resolve().parents[2]
PROCESSED_DIR = ROOT_DIR / "data" / "processed"
RAW_DIR = ROOT_DIR / "data" / "raw"
MODELS_DIR = ROOT_DIR / "models"
NOTEBOOKS_DIR = ROOT_DIR / "notebooks"

HISTORICAL_EVENTS_PATH = PROCESSED_DIR / "strandings_with_moon.parquet"
RAW_HISTORICAL_EVENTS_PATH = RAW_DIR / "UNC-DataRequest-01302026.xlsx"
WEEKLY_DATA_PATH = PROCESSED_DIR / "final_dataset.parquet"
PLANKTON_LOOKUP_PATH = PROCESSED_DIR / "plankton_imputed_lookup.parquet"
LGBM_MODEL_PATH = MODELS_DIR / "lgbm_model.pkl"
BASELINE_MODEL_PATH = MODELS_DIR / "baseline_model.pkl"
FULL_METRICS_PATH = PROCESSED_DIR / "full_model_metrics.json"
BASELINE_METRICS_PATH = PROCESSED_DIR / "baseline_metrics.json"
TEST_METRICS_PATH = PROCESSED_DIR / "test_metrics.json"
PIPELINE_NOTEBOOKS = (
    "01_a_strandings_data_clean.ipynb",
    "03_weather_data.ipynb",
    "04_moon_phases.ipynb",
    "05_plankton_imputation.ipynb",
    "06_final_dataset_assembly.ipynb",
    "08_baseline_model.ipynb",
    "09_full_model.ipynb",
    "11_model_evaluation.ipynb",
)


def _require_file(path: Path) -> Path:
    if not path.exists():
        raise FileNotFoundError(
            f"Missing required artifact: {path}. "
            "Run the training/data notebooks to generate this file."
        )
    return path


def _standardize_historical_columns(df: pd.DataFrame) -> pd.DataFrame:
    frame = df.copy()

    rename_map = {
        "Common Name": "common_name",
        "State": "state",
        "Condition at Examination": "condition",
        "National Database Number": "event_id",
    }
    for source, target in rename_map.items():
        if source in frame.columns and target not in frame.columns:
            frame[target] = frame[source]

    if "mms_observation_dt" not in frame.columns:
        date_parts = {"year": None, "month": None, "day": None}
        if "Year of Observation" in frame.columns:
            date_parts["year"] = pd.to_numeric(frame["Year of Observation"], errors="coerce")
        if "Month of Observation" in frame.columns:
            month_str = frame["Month of Observation"].astype(str).str.slice(0, 3).str.upper()
            month_num = pd.to_datetime(month_str, format="%b", errors="coerce").dt.month
            date_parts["month"] = month_num
        if "Day of Observation" in frame.columns:
            date_parts["day"] = pd.to_numeric(frame["Day of Observation"], errors="coerce")

        if all(value is not None for value in date_parts.values()):
            frame["mms_observation_dt"] = pd.to_datetime(date_parts, errors="coerce")

    if "mms_observation_dt" in frame.columns:
        frame["mms_observation_dt"] = pd.to_datetime(frame["mms_observation_dt"], errors="coerce")

    if "is_estimated_coordinates" not in frame.columns:
        est = pd.Series(False, index=frame.index, dtype="bool")
        for est_col in ("Latitude Actual/Estimate", "Longitude Actual/Estimate"):
            if est_col in frame.columns:
                est = est | frame[est_col].astype(str).str.contains("est", case=False, na=False)
        frame["is_estimated_coordinates"] = est

    if "Latitude" in frame.columns:
        frame["Latitude"] = pd.to_numeric(frame["Latitude"], errors="coerce")
    if "Longitude" in frame.columns:
        frame["Longitude"] = pd.to_numeric(frame["Longitude"], errors="coerce")

    return frame


@st.cache_data(show_spinner=False)
def load_historical_events() -> pd.DataFrame:
    if HISTORICAL_EVENTS_PATH.exists():
        df = pd.read_parquet(HISTORICAL_EVENTS_PATH)
        return _standardize_historical_columns(df)

    df = pd.read_excel(_require_file(RAW_HISTORICAL_EVENTS_PATH))
    return _standardize_historical_columns(df)


@st.cache_data(show_spinner=False)
def load_weekly_data() -> pd.DataFrame:
    path = _require_file(WEEKLY_DATA_PATH)
    df = pd.read_parquet(path)
    if "week_start" in df.columns:
        df["week_start"] = pd.to_datetime(df["week_start"])
    return df


@st.cache_data(show_spinner=False)
def load_plankton_lookup() -> pd.DataFrame:
    path = _require_file(PLANKTON_LOOKUP_PATH)
    df = pd.read_parquet(path)

    if "ds" in df.columns and "week_start" not in df.columns:
        df = df.rename(columns={"ds": "week_start"})
    if "yhat" in df.columns and "plankton_density" not in df.columns:
        df = df.rename(columns={"yhat": "plankton_density"})

    if "week_start" in df.columns:
        df["week_start"] = pd.to_datetime(df["week_start"])
    return df


@st.cache_resource(show_spinner=False)
def load_lgbm_model() -> Any:
    path = _require_file(LGBM_MODEL_PATH)
    with path.open("rb") as f:
        return pickle.load(f)


@st.cache_resource(show_spinner=False)
def load_baseline_model() -> Any:
    path = _require_file(BASELINE_MODEL_PATH)
    with path.open("rb") as f:
        return pickle.load(f)


@st.cache_data(show_spinner=False)
def _load_json(path: Path) -> dict[str, Any]:
    with _require_file(path).open("r", encoding="utf-8") as f:
        return json.load(f)


@st.cache_data(show_spinner=False)
def load_metrics() -> dict[str, dict[str, Any]]:
    return {
        "full": _load_json(FULL_METRICS_PATH),
        "baseline": _load_json(BASELINE_METRICS_PATH),
        "test": _load_json(TEST_METRICS_PATH),
    }


def get_model_features(model: Any) -> list[str]:
    if hasattr(model, "feature_name"):
        features = model.feature_name()
        return [str(f) for f in features]
    raise ValueError("Loaded model does not expose feature_name().")


def get_metrics_features(metrics: dict[str, dict[str, Any]]) -> list[str]:
    features = metrics.get("full", {}).get("features")
    if not isinstance(features, list):
        return []
    return [str(f) for f in features]


def compare_feature_schemas(
    model_features: list[str],
    metrics_features: list[str],
) -> dict[str, set[str]]:
    model_set = set(model_features)
    metrics_set = set(metrics_features)
    return {
        "model_only": model_set - metrics_set,
        "metrics_only": metrics_set - model_set,
    }


@st.cache_data(show_spinner=False)
def get_pipeline_artifact_status() -> dict[str, Any]:
    notebook_status: dict[str, bool] = {}
    for notebook_name in PIPELINE_NOTEBOOKS:
        notebook_path = NOTEBOOKS_DIR / notebook_name
        notebook_status[notebook_name] = notebook_path.exists()

    artifacts = {
        "historical": HISTORICAL_EVENTS_PATH.exists(),
        "weekly": WEEKLY_DATA_PATH.exists(),
        "plankton": PLANKTON_LOOKUP_PATH.exists(),
        "model_lgbm": LGBM_MODEL_PATH.exists(),
        "model_baseline": BASELINE_MODEL_PATH.exists(),
        "metrics_full": FULL_METRICS_PATH.exists(),
        "raw_source": RAW_HISTORICAL_EVENTS_PATH.exists(),
    }
    path_lookup = {
        "historical": HISTORICAL_EVENTS_PATH,
        "weekly": WEEKLY_DATA_PATH,
        "plankton": PLANKTON_LOOKUP_PATH,
        "model_lgbm": LGBM_MODEL_PATH,
        "model_baseline": BASELINE_MODEL_PATH,
        "metrics_full": FULL_METRICS_PATH,
        "raw_source": RAW_HISTORICAL_EVENTS_PATH,
    }

    stale_threshold = pd.Timestamp.utcnow() - pd.Timedelta(days=14)
    stale_artifacts: list[str] = []
    for name, exists in artifacts.items():
        if not exists:
            continue
        path = path_lookup[name]
        modified = pd.Timestamp(path.stat().st_mtime, unit="s", tz="UTC")
        if modified < stale_threshold:
            stale_artifacts.append(name)

    return {
        "notebooks": notebook_status,
        "artifacts": artifacts,
        "stale_artifacts": stale_artifacts,
    }
