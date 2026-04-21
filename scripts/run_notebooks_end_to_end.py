#!/usr/bin/env python3
from __future__ import annotations

import shutil
import subprocess
import sys
from importlib.util import find_spec
import os
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
NOTEBOOK_DIR = ROOT_DIR / "notebooks"
NOTEBOOK_CHAIN = (
    "01_a_strandings_data_clean.ipynb",
    "03_weather_data.ipynb",
    "04_moon_phases.ipynb",
    "05_plankton_imputation.ipynb",
    "06_final_dataset_assembly.ipynb",
    "08_baseline_model.ipynb",
    "09_full_model.ipynb",
    "11_model_evaluation.ipynb",
)


def _require_jupyter() -> list[str]:
    if not shutil.which("jupyter") and find_spec("jupyter") is None:
        raise RuntimeError(
            "jupyter is not installed in this environment. "
            "Install notebook tooling first: pip install jupyter nbconvert nbclient"
        )
    if find_spec("jupyter_core") is None or find_spec("nbconvert") is None:
        raise RuntimeError(
            "jupyter is present but incomplete in this interpreter. "
            "Install notebook tooling first: pip install jupyter nbconvert nbclient jupyter_core"
        )
    return [sys.executable, "-m", "nbconvert"]


def _run_notebook(jupyter_cmd: list[str], notebook_name: str) -> None:
    notebook_path = NOTEBOOK_DIR / notebook_name
    if not notebook_path.exists():
        raise FileNotFoundError(f"Notebook not found: {notebook_path}")

    cmd = [
        *jupyter_cmd,
        "--to",
        "notebook",
        "--execute",
        "--inplace",
        "--ExecutePreprocessor.timeout=3600",
        notebook_name,
    ]
    print(f"\n=== Executing {notebook_name} ===")
    env = os.environ.copy()
    src_path = str(ROOT_DIR / "src")
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = f"{src_path}:{existing}" if existing else src_path
    subprocess.run(cmd, cwd=NOTEBOOK_DIR, env=env, check=True)


def main() -> int:
    try:
        jupyter_cmd = _require_jupyter()
        for notebook_name in NOTEBOOK_CHAIN:
            _run_notebook(jupyter_cmd, notebook_name)
    except Exception as exc:
        print(f"Notebook run failed: {exc}", file=sys.stderr)
        return 1

    print("\nNotebook execution complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
