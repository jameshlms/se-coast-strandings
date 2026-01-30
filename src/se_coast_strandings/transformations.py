from datetime import datetime
from typing import Tuple

from numpy import cos, pi, sin
from pandas import DataFrame, Series, to_datetime


def make_dt_col(day_col: Series, month_col: Series, year_col: Series) -> Series:
    """Combine day, month, and year columns into a single datetime Series.

    Args:
        day_col (pd.Series): Series containing day values.
        month_col (pd.Series): Series containing month values (abbreviated names).
        year_col (pd.Series): Series containing year values.

    Returns:
        pd.Series: A Series of datetime objects.
    """
    month_col = month_col.apply(lambda m: datetime.strptime(m, "%b").month)
    dates = to_datetime(
        dict(
            year=year_col,
            month=month_col,
            day=day_col,
        ),
        errors="coerce",
    )
    return dates


def make_cyclic(
    series: Series,
    period: int,
    name: str | None = None,
    as_dataframe: bool = False,
) -> DataFrame | Tuple[Series, Series]:
    """Transform a pandas Series into its cyclic representation.

    Args:
        series (pd.Series): The input series to be transformed.
        period (int): The period of the cycle (e.g., 12 for months, 24 for hours).
        name (str | None): Optional name for the resulting cyclic features.
        as_dataframe (bool): Whether to return the result as a DataFrame. If False, returns a tuple of Series.
    Returns:
        pd.DataFrame | Tuple[pd.Series, pd.Series]: A DataFrame with two columns representing the cyclic components or a tuple of two Series.
    """
    if not isinstance(period, (int, float)):
        try:
            period = int(period)

        except ValueError as e:
            raise TypeError("Period must be an integer.") from e

    if not isinstance(series, Series):
        raise TypeError("Input must be a pandas Series.")

    feature_sin_name = f"{name or series.name or 'unnamed'}_sin"
    feature_cos_name = f"{name or series.name or 'unnamed'}_cos"

    radians = 2 * pi * series / period

    feature_sin = Series(data=sin(radians), name=feature_sin_name, dtype="float32")
    feature_cos = Series(data=cos(radians), name=feature_cos_name, dtype="float32")

    if bool(as_dataframe):
        return DataFrame({feature_sin_name: feature_sin, feature_cos_name: feature_cos})

    return feature_sin, feature_cos
