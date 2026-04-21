# Dashboard Implementation Guide

## 1. Run the dashboard locally (start here)

From the repo root:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -r dashboard/requirements_dashboard.txt
pip install jupyter nbconvert nbclient jupyter_core
pip install -e .
```

Run the app:

```bash
streamlit run dashboard/app.py
```

Then open the local URL Streamlit prints (usually `http://localhost:8501`).

## 2. What must exist before it works

The dashboard reads local artifacts that are not committed to git (`data/` and model `.pkl` files are ignored).

### Historical map needs

- `data/processed/strandings_with_moon.parquet` (preferred), or
- fallback raw file: `data/raw/UNC-DataRequest-01302026.xlsx`

### Prediction map needs

- `data/processed/final_dataset.parquet`
- `data/processed/plankton_imputed_lookup.parquet`
- `models/lgbm_model.pkl` (required)
- `models/baseline_model.pkl` (optional, enables baseline toggle)
- `data/processed/full_model_metrics.json` / `baseline_metrics.json` / `test_metrics.json` (optional but recommended)

Quick check:

```bash
ls data/raw/UNC-DataRequest-01302026.xlsx \
   data/processed/strandings_with_moon.parquet \
   data/processed/final_dataset.parquet \
   data/processed/plankton_imputed_lookup.parquet \
   models/lgbm_model.pkl
```

## 3. Notebook/data pipeline to generate artifacts

### Option A: fastest for teammates

Copy prebuilt `data/` and `models/*.pkl` artifacts from your shared location, then run Streamlit.

### Option B: rebuild locally from notebooks

Required raw input:

- `data/raw/UNC-DataRequest-01302026.xlsx`

Run notebooks in this order:

1. `notebooks/01_c_plankton_data_clean.ipynb` (creates `copepod_dataset_se_coast.parquet`, required by notebook 05)
2. `python3 scripts/run_notebooks_end_to_end.py`

The end-to-end script executes:

1. `01_a_strandings_data_clean.ipynb`
2. `03_weather_data.ipynb`
3. `04_moon_phases.ipynb`
4. `05_plankton_imputation.ipynb`
5. `06_final_dataset_assembly.ipynb`
6. `08_baseline_model.ipynb`
7. `09_full_model.ipynb`
8. `11_model_evaluation.ipynb`

Outputs used by dashboard pages:

- Historical: `strandings_with_moon.parquet`
- Prediction: `final_dataset.parquet`, `plankton_imputed_lookup.parquet`, `lgbm_model.pkl` (and optional baseline/metrics)

## 4. Where each page pulls information from

### Historical Explorer (`dashboard/pages/01_historical.py`)

- Loads events through `dashboard/utils/data_loader.py::load_historical_events()`
- Source priority:
  1. `data/processed/strandings_with_moon.parquet`
  2. fallback `data/raw/UNC-DataRequest-01302026.xlsx`
- Map points are built in `dashboard/utils/map_helpers.py::build_historical_map_points()`
- Uses stored per-event weather/moon columns when available; computes moon fallback fields if missing

### Prediction Map (`dashboard/pages/02_predictions.py`)

- Loads static artifacts:
  - `data/processed/final_dataset.parquet`
  - `data/processed/plankton_imputed_lookup.parquet`
  - `models/lgbm_model.pkl` (+ optional `models/baseline_model.pkl`)
- Builds features per region/week in `dashboard/utils/feature_builder.py`
- Pulls weather dynamically via `dashboard/utils/weather_client.py`:
  - Open-Meteo archive API for past dates
  - Open-Meteo forecast API for near-future dates
  - prior-year archive fallback for far-future selection
- Predicts with the loaded model and renders region markers on the map

## 5. Useful troubleshooting

- If Prediction Map shows missing artifact errors, regenerate model/data artifacts (`01_c` + end-to-end script) or copy them from shared storage.
- If notebook execution fails on weather steps, verify internet access (Open-Meteo endpoints are called during weather feature generation and prediction-time feature building).
- If Streamlit command fails, verify the active venv and installed deps from Sections 1 and 3.
