from datetime import datetime
from typing import Tuple

from numpy import cos, pi, sin
from pandas import DataFrame, Series, Timestamp, to_datetime


def _get_season(dt: Timestamp) -> int:
    y = dt.year
    spring = Timestamp(y, 3, 21)
    summer = Timestamp(y, 6, 21)
    autumn = Timestamp(y, 9, 21)
    winter = Timestamp(y, 12, 21)
    if spring <= dt < summer:
        return 0
    elif summer <= dt < autumn:
        return 1
    elif autumn <= dt < winter:
        return 2
    else:
        return 3


def make_season_col(date_col: Series) -> Series:
    """Create a season column from a datetime Series.

    Args:
        date_col (pd.Series): Series containing datetime objects.
    Returns:
        pd.Series: A Series indicating the season (0: Spring, 1: Summer, 2: Autumn, 3: Winter).
    """
    date_col = to_datetime(date_col, errors="coerce")
    seasons = date_col.apply(_get_season).apply(
        {0: "spring", 1: "summer", 2: "autumn", 3: "winter"}.get
    )
    return seasons


def make_dt_col(day_col: Series, month_col: Series, year_col: Series) -> Series:
    """Combine day, month, and year columns into a single datetime Series.

    Args:
        day_col (pd.Series): Series containing day values.
        month_col (pd.Series): Series containing month values (abbreviated names).
        year_col (pd.Series): Series containing year values.

    Returns:
        pd.Series: A Series of datetime objects.
    """
    if month_col.dtype in ("object", "string", "category"):
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


def make_cyclic_season(
    date_series: Series,
    name: str | None = None,
    as_dataframe: bool = False,
) -> DataFrame | Tuple[Series, Series]:
    """Transform a pandas Series of dates into its cyclic season representation.

    Args:
        date_series (pd.Series): The input series of dates to be transformed.
        name (str | None): Optional name for the resulting cyclic features.
        as_dataframe (bool): Whether to return the result as a DataFrame. If False, returns a tuple of Series.
    Returns:
        pd.DataFrame | Tuple[pd.Series, pd.Series]: A DataFrame with two columns representing the cyclic season components or a tuple of two Series.
    """
    seasons = make_season_col(date_series)

    return make_cyclic(
        series=seasons,
        period=4,
        name=name,
        as_dataframe=as_dataframe,
    )
