from collections.abc import Mapping, Sequence
from datetime import timedelta
from functools import partial
from time import sleep
from typing import Final, Tuple, TypedDict

from numpy import empty
from pandas import DataFrame, Series, Timestamp
from requests import Session

URL: Final[str] = "https://historical-forecast-api.open-meteo.com/v1/forecast"
DAILY_VARIABLES_KEY: Final[str] = "daily"


class DailyValues(TypedDict, total=False):
    time: Sequence[str]


class WeatherAPIResponse(TypedDict):
    latitude: float
    longitude: float
    generationtime_ms: float
    utc_offset_seconds: int
    timezone: str
    timezone_abbreviation: str
    elevation: float
    daily_units: Mapping[str, str]
    daily: DailyValues


def _get_weather_data(
    session: Session,
    latitudes: Sequence[float],
    longitudes: Sequence[float],
    start_date: str,
    end_date: str,
    hourly_variables: str | None = None,
    daily_variables: str | None = None,
    tz: str = "America/New_York",
) -> Sequence[WeatherAPIResponse] | WeatherAPIResponse:
    params = {
        "latitude": latitudes,
        "longitude": longitudes,
        "start_date": start_date,
        "end_date": end_date,
        "timezone": tz,
    }

    if hourly_variables is not None:
        params["hourly"] = hourly_variables

    if daily_variables is not None:
        params["daily"] = daily_variables

    response = session.get(URL, params=params, timeout=60)
    data: Sequence[WeatherAPIResponse] | WeatherAPIResponse = response.json()
    return data


def _convert_coords(coords: Series) -> Series:
    return coords.round(3).astype(float)


def _get_daily_times_and_values(
    forecast: WeatherAPIResponse,
    daily_variables: Sequence[str],
) -> Tuple[list[str], dict[str, list[float | None]]]:
    daily_data = forecast.get(DAILY_VARIABLES_KEY) or {}
    daily_times = list(daily_data.get("time")) or []
    daily_values = {var: list(daily_data.get(var)) or [] for var in daily_variables}
    return daily_times, daily_values


def fetch_weather_context(
    df: DataFrame,
    lat_column: str,
    lon_column: str,
    date_column: str,
    daily_variables: Sequence[str],
    tz: str = "America/New_York",
    days_prior: int = 7,
    include_deltas: bool = False,
    batch_size: int = 50,
    sleep_interval: int = 30,
) -> DataFrame:
    """
    fetch weather context data to the given dataframe based on latitude, longitude, and date columns.

    :param df: The dataframe to fetch weather context data for
    :type df: DataFrame
    :param lat_column: The name of the column containing latitude values (In decimal degrees as floats)
    :type lat_column: str
    :param lon_column: The name of the column containing longitude values (In decimal degrees as floats)
    :type lon_column: str
    :param date_column: The name of the column containing date values (as Timestamps)
    :type date_column: str
    :param daily_variables: The Sequence of daily weather variables to retrieve (e.g. temperature_2m_max, precipitation_sum)
    :type daily_variables: Sequence[str]
    :param tz: The timezone for the weather data (default is "America/New_York")
    :type tz: str
    :param days_prior: The number of days to subtract from the date for the start date calculation (default is 7)
    :type days_prior: int
    :param include_deltas: Whether to include delta columns for the weather variables (default is False)
    :type include_deltas: bool

    :return: The dataframe with the fetched weather context data
    :rtype: DataFrame
    """
    groups = df.groupby(date_column)

    n: int = df.shape[0]
    rows = empty(n, dtype=object)

    session = Session()

    get_weather_data_partial = partial(
        _get_weather_data,
        session=session,
        hourly_variables=None,
        daily_variables=daily_variables,
        tz=tz,
    )

    for group_date, group_df in groups:
        latitudes: list[float] = _convert_coords(group_df[lat_column]).to_list()
        longitudes: list[float] = _convert_coords(group_df[lon_column]).to_list()

        start_date = (Timestamp(group_date) - timedelta(days=days_prior - 1)).strftime(
            "%Y-%m-%d"
        )
        end_date = Timestamp(group_date).strftime("%Y-%m-%d")

        response: Sequence[WeatherAPIResponse] | WeatherAPIResponse = (
            get_weather_data_partial(
                latitudes=latitudes,
                longitudes=longitudes,
                start_date=start_date,
                end_date=end_date,
            )
        )

        if "error" in response:
            raise RuntimeError(response["reason"])

        if isinstance(response, dict):
            response = [response]

        for i, forecast in zip(map(int, group_df.index), response):
            row: dict[str, float] = {}

            _, variables = _get_daily_times_and_values(forecast, daily_variables)
            for var_name, var_values in variables.items():
                for j, n_days in enumerate(range(days_prior - 1, -1, -1)):
                    value = var_values[j]
                    row[f"{var_name}_{n_days}_days_prior"] = value

                    if include_deltas and n_days > 0:
                        curr = value
                        prev = var_values[j - 1]

                        if isinstance(prev, float) and isinstance(curr, float):
                            row[f"{var_name}_{n_days}_days_prior_delta"] = curr - prev

                        else:
                            row[f"{var_name}_{n_days}_days_prior_delta"] = None

            print(row)
            rows[i] = row

        sleep(sleep_interval)

    results = DataFrame(list(rows))

    return results
