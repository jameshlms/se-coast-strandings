from collections.abc import Mapping, Sequence
from datetime import timedelta
from functools import partial
from typing import Final, TypedDict

from numpy import empty
from pandas import DataFrame, Series, Timestamp, concat
from requests import Session

URL: Final[str] = "https://historical-forecast-api.open-meteo.com/v1/forecast"


class WeatherAPIResponse(TypedDict):
    latitude: float
    longitude: float
    generationtime_ms: float
    utc_offset_seconds: int
    timezone: str
    timezone_abbreviation: str
    elevation: float
    hourly_units: Mapping[str, str]
    hourly: Mapping[str, Sequence[float | str | None]]
    daily_units: Mapping[str, str]
    daily: Mapping[str, Sequence[float | str | None]]


def _get_weather_data(
    session: Session,
    lat: float,
    lon: float,
    start_date: str,
    end_date: str,
    hourly_variables: str | None = None,
    daily_variables: str | None = None,
    tz: str = "America/New_York",
) -> WeatherAPIResponse:
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "timezone": tz,
    }

    if hourly_variables is not None:
        params["hourly"] = hourly_variables

    if daily_variables is not None:
        params["daily"] = daily_variables

    response = session.get(URL, params=params, timeout=20)
    data: WeatherAPIResponse = response.json()
    return data


def _convert_coords(coords: Series[float]) -> Series[float]:
    return coords.round(5).astype(float)


def _convert_dates(dates: Series[Timestamp]) -> Series[str]:
    return dates.astype("datetime64[ns]").dt.strftime("%Y-%m-%d")


def _convert_variables(variables: Sequence[str]) -> str:
    return ",".join(variables)


def append_weather_context(
    df: DataFrame,
    lat_column: str,
    lon_column: str,
    date_column: str,
    hourly_variables: Sequence[str],
    daily_variables: Sequence[str],
    tz: str = "America/New_York",
    days_prior: int = 7,
    include_deltas: bool = False,
    inplace: bool = False,
) -> DataFrame | None:
    """
    Appends weather context data to the given dataframe based on latitude, longitude, and date columns.

    :param df: The dataframe to append weather context to
    :type df: DataFrame
    :param lat_column: The name of the column containing latitude values (In decimal degrees as floats)
    :type lat_column: str
    :param lon_column: The name of the column containing longitude values (In decimal degrees as floats)
    :type lon_column: str
    :param date_column: The name of the column containing date values (as Timestamps)
    :type date_column: str
    :param hourly_variables: The Sequence of hourly weather variables to retrieve (e.g. temperature_2m, precipitation)
    :type hourly_variables: Sequence[str]
    :param daily_variables: The Sequence of daily weather variables to retrieve (e.g. temperature_2m_max, precipitation_sum)
    :type daily_variables: Sequence[str]
    :param tz: The timezone for the weather data (default is "America/New_York")
    :type tz: str
    :param days_prior: The number of days to subtract from the date for the start date calculation (default is 7)
    :type days_prior: int
    :param include_deltas: Whether to include delta columns for the weather variables (default is False)
    :type include_deltas: bool
    :param inplace: Whether to modify the dataframe in place or return a new one
    :type inplace: bool

    :return: The modified dataframe if not inplace, else None
    :rtype: DataFrame | None
    """
    raise NotImplementedError(
        "Function 'append_weather_context' is not yet implemented."
    )
    latitudes: Series[float] = _convert_coords(df[str(lat_column)])
    longitudes: Series[float] = _convert_coords(df[str(lon_column)])

    end_dates: Series[str] = _convert_dates(
        df[str(date_column)].astype("datetime64[ns]")
    )
    start_dates: Series[str] = _convert_dates(
        df[str(date_column)].astype("datetime64[ns]") - timedelta(days=days_prior)
    )

    hourly_vars: str = _convert_variables(hourly_variables)
    daily_vars: str = _convert_variables(daily_variables)

    session = Session()

    get_weather_data_partial = partial(
        _get_weather_data,
        session,
        hourly_variables=hourly_vars,
        daily_variables=daily_vars,
        tz=tz,
    )

    rows = empty(df.shape[0], dtype=object)

    for idx in range(df.shape[0]):
        data: WeatherAPIResponse = get_weather_data_partial(
            latitudes.iloc[idx],
            longitudes.iloc[idx],
            start_dates.iloc[idx],
            end_dates.iloc[idx],
        )

        daily_data = data.get("daily") or {}

        row = {}
        for var in daily_variables:
            vals = daily_data.get(var) or []

            for i, val in enumerate(vals):
                row[f"{var}_{i}_days_prior"] = val

                if include_deltas and i > 0:
                    prev = vals[i - 1]
                    curr = val

                    if isinstance(prev, float) and isinstance(curr, float):
                        row[f"{var}_{i}_days_prior_delta"] = curr - prev
                    else:
                        row[f"{var}_{i}_days_prior_delta"] = None

        rows[idx] = row

    results = DataFrame(rows)

    if bool(inplace):
        for col in results.columns:
            df[col] = results[col]
        return None

    else:
        return concat([df, results], axis=1)
