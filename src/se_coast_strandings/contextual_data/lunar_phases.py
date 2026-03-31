"""Lunar phase feature engineering for marine mammal strandings."""

import pandas as pd
from pandas import DataFrame, Series, Timestamp

LUNAR_CYCLE_DAYS: float = 29.530588
FIRST_NEW_MOON: Timestamp = Timestamp("2016-01-10")


def moon_age(date) -> float:
    """Return moon age in days (0–29.53) for a given date."""
    days_since = (Timestamp(date) - FIRST_NEW_MOON).days
    return float(days_since % LUNAR_CYCLE_DAYS)


def moon_phase(age: float) -> str:
    """Return categorical moon phase for a given moon age."""
    if age < 1.84566:
        return "New Moon"
    elif age < 5.53699:
        return "Waxing Crescent"
    elif age < 9.22831:
        return "First Quarter"
    elif age < 12.91963:
        return "Waxing Gibbous"
    elif age < 16.61096:
        return "Full Moon"
    elif age < 20.30228:
        return "Waning Gibbous"
    elif age < 23.99361:
        return "Last Quarter"
    elif age < 27.68493:
        return "Waning Crescent"
    else:
        return "New Moon"


def add_moon_features(df: DataFrame, date_col: str) -> DataFrame:
    """Add moon_age (float) and moon_phase (str) columns to a DataFrame."""
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col])
    days_since = (df[date_col] - FIRST_NEW_MOON).dt.days
    df["moon_age"] = days_since % LUNAR_CYCLE_DAYS
    df["moon_phase"] = df["moon_age"].apply(moon_phase)
    return df
