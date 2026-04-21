from __future__ import annotations

import importlib

import streamlit as st

st.set_page_config(page_title="SE Coast Strandings Dashboard", layout="wide")

st.title("SE Coast Marine Mammal Strandings")

st.session_state["_navigation_mode"] = "navigation" if hasattr(st, "navigation") else "fallback"

if hasattr(st, "navigation"):
    pages = [
        st.Page("pages/01_historical.py", title="Historical Explorer", icon=":material/travel_explore:"),
        st.Page("pages/02_predictions.py", title="Prediction Map", icon=":material/map:"),
    ]
    nav = st.navigation(pages)
    nav.run()
else:
    # Fallback for older streamlit versions
    choice = st.sidebar.radio("Page", ["Historical Explorer", "Prediction Map"])
    module_name = (
        "dashboard.pages.01_historical"
        if choice == "Historical Explorer"
        else "dashboard.pages.02_predictions"
    )
    page_mod = importlib.import_module(module_name)
    page_mod.main()
