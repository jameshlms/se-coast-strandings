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
    hourly_variables: str,
    daily_variables: str,
    tz: str = "America/New_York",
) -> WeatherAPIResponse:
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": hourly_variables,
        "daily": daily_variables,
        "timezone": tz,
    }

    response = session.get(URL, params=params, timeout=20)
    data: WeatherAPIResponse = response.json()
    return data


def _convert_coords(coords: Series[float]) -> Series[float]:
    return coords.round(5).astype(float)


def _convert_dates(dates: Series[Timestamp]) -> Series[str]:
    return dates.astype("datetime64[ns]").dt.strftime("%Y-%m-%d")


def _convert_variables(variables: list[str]) -> str:
    return ",".join(variables)


def append_weather_context(
    df: DataFrame,
    lat_column: str,
    lon_column: str,
    date_column: str,
    hourly_variables: list[str],
    daily_variables: list[str],
    tz: str = "America/New_York",
    week_delta: int = 1,
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
    :param hourly_variables: The list of hourly weather variables to retrieve (e.g. temperature_2m, precipitation)
    :type hourly_variables: list[str]
    :param daily_variables: The list of daily weather variables to retrieve (e.g. temperature_2m_max, precipitation_sum)
    :type daily_variables: list[str]
    :param tz: The timezone for the weather data (default is "America/New_York")
    :type tz: str
    :param week_delta: The number of weeks to subtract from the date for the end date calculation (default is 1)
    :type week_delta: int
    :param inplace: Whether to modify the dataframe in place or return a new one
    :type inplace: bool

    :return: The modified dataframe if not inplace, else None
    :rtype: DataFrame | None
    """
    latitudes: Series[float] = _convert_coords(df[lat_column])
    longitudes: Series[float] = _convert_coords(df[lon_column])

    start_dates: Series[str] = _convert_dates(df[date_column])
    end_dates: Series[str] = _convert_dates(
        df[date_column].astype("datetime64[ns]") - timedelta(weeks=week_delta)
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

    for i, (lat, lon, start_date, end_date) in enumerate(
        zip(latitudes, longitudes, start_dates, end_dates)
    ):
        data: WeatherAPIResponse = get_weather_data_partial(
            lat,
            lon,
            start_date,
            end_date,
        )

        daily_data = data.get("daily", {})
        row_data = {
            f"weather_{key}": daily_data.get(key, [None])[-1] for key in daily_variables
        }

        rows[i] = row_data

    results = DataFrame(rows)

    if bool(inplace):
        for col in results.columns:
            df[col] = results[col]
        return None

    else:
        return concat([df, results], axis=1)
