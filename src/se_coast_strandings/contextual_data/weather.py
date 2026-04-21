from collections.abc import Mapping, Sequence
from datetime import timedelta
from functools import partial
from time import sleep
from typing import Final, Tuple, TypedDict

from pandas import DataFrame, Series, Timestamp
from requests import Session

ARCHIVE_URL: Final[str] = "https://archive-api.open-meteo.com/v1/archive"
FORECAST_URL: Final[str] = "https://api.open-meteo.com/v1/forecast"
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
    latitudes: Sequence[float] | float,
    longitudes: Sequence[float] | float,
    start_date: str,
    end_date: str,
    hourly_variables: str | None = None,
    daily_variables: str | None = None,
    tz: str = "America/New_York",
    endpoint: str = ARCHIVE_URL,
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

    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = session.get(endpoint, params=params, timeout=60)
            if response.status_code == 200:
                data: Sequence[WeatherAPIResponse] | WeatherAPIResponse = response.json()
                return data
            if attempt < max_retries - 1:
                sleep(2**attempt)  # 1s, 2s, 4s
                continue
            response.raise_for_status()
        except Exception:
            if attempt < max_retries - 1:
                sleep(2**attempt)
                continue
            raise

    # Fallback (should not reach here)
    response = session.get(endpoint, params=params, timeout=60)
    data = response.json()
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


def _coerce_response_to_list(
    response: Sequence[WeatherAPIResponse] | WeatherAPIResponse,
) -> list[WeatherAPIResponse]:
    if isinstance(response, Mapping):
        if bool(response.get("error")):
            raise RuntimeError(str(response.get("reason", "Weather API request failed")))
        return [response]

    return list(response)


def _flatten_daily_row(
    daily_values: Mapping[str, Sequence[float | None]],
    daily_variables: Sequence[str],
    days_prior: int,
    include_deltas: bool = False,
) -> dict[str, float | None]:
    row: dict[str, float | None] = {}
    for var_name in daily_variables:
        var_values = list(daily_values.get(var_name) or [])
        for j, n_days in enumerate(range(days_prior - 1, -1, -1)):
            value: float | None
            if j < len(var_values):
                raw_value = var_values[j]
                value = float(raw_value) if isinstance(raw_value, (int, float)) else None
            else:
                value = None

            row[f"{var_name}_{n_days}_days_prior"] = value

            if include_deltas and n_days > 0:
                if j > 0 and j - 1 < len(var_values):
                    prev_raw = var_values[j - 1]
                    prev = (
                        float(prev_raw)
                        if isinstance(prev_raw, (int, float))
                        else None
                    )
                else:
                    prev = None

                if isinstance(value, float) and isinstance(prev, float):
                    row[f"{var_name}_{n_days}_days_prior_delta"] = value - prev
                else:
                    row[f"{var_name}_{n_days}_days_prior_delta"] = None

    return row


def _prepare_variables_query(variables: Sequence[str]) -> str:
    return ",".join(variables)


def get_weather_for_point(
    lat: float,
    lon: float,
    date: Timestamp,
    variables: Sequence[str] = ("temperature_2m_max", "temperature_2m_min"),
    days_prior: int = 7,
    tz: str = "America/New_York",
) -> dict[str, float | None]:
    target_date = Timestamp(date).normalize()
    start_date = (target_date - timedelta(days=days_prior - 1)).strftime("%Y-%m-%d")
    end_date = target_date.strftime("%Y-%m-%d")

    with Session() as session:
        response = _get_weather_data(
            session=session,
            latitudes=lat,
            longitudes=lon,
            start_date=start_date,
            end_date=end_date,
            daily_variables=_prepare_variables_query(variables),
            tz=tz,
            endpoint=ARCHIVE_URL,
        )

    forecasts = _coerce_response_to_list(response)
    if not forecasts:
        return {}

    forecast = forecasts[0]
    _, daily_values = _get_daily_times_and_values(forecast, variables)
    return _flatten_daily_row(
        daily_values=daily_values,
        daily_variables=variables,
        days_prior=days_prior,
        include_deltas=False,
    )


def get_weather_forecast_for_point(
    lat: float,
    lon: float,
    date: Timestamp,
    variables: Sequence[str] = ("temperature_2m_max", "temperature_2m_min"),
    days_prior: int = 7,
    tz: str = "America/New_York",
) -> dict[str, float | None]:
    target_date = Timestamp(date).normalize()
    start_date = (target_date - timedelta(days=days_prior - 1)).strftime("%Y-%m-%d")
    end_date = target_date.strftime("%Y-%m-%d")

    with Session() as session:
        response = _get_weather_data(
            session=session,
            latitudes=lat,
            longitudes=lon,
            start_date=start_date,
            end_date=end_date,
            daily_variables=_prepare_variables_query(variables),
            tz=tz,
            endpoint=FORECAST_URL,
        )

    forecasts = _coerce_response_to_list(response)
    if not forecasts:
        return {}

    forecast = forecasts[0]
    _, daily_values = _get_daily_times_and_values(forecast, variables)
    return _flatten_daily_row(
        daily_values=daily_values,
        daily_variables=variables,
        days_prior=days_prior,
        include_deltas=False,
    )


def fetch_weather_context(
    df: DataFrame,
    lat_column: str,
    lon_column: str,
    date_column: str,
    daily_variables: Sequence[str],
    tz: str = "America/New_York",
    days_prior: int = 7,
    include_deltas: bool = False,
    sleep_interval: int = 10,
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

    rows_by_label: dict[object, dict[str, float | None]] = {}

    with Session() as session:
        get_weather_data_partial = partial(
            _get_weather_data,
            session=session,
            hourly_variables=None,
            daily_variables=_prepare_variables_query(daily_variables),
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

            forecasts = _coerce_response_to_list(response)

            for idx_label, forecast in zip(group_df.index.tolist(), forecasts):
                _, variables = _get_daily_times_and_values(forecast, daily_variables)
                row = _flatten_daily_row(
                    daily_values=variables,
                    daily_variables=daily_variables,
                    days_prior=days_prior,
                    include_deltas=include_deltas,
                )
                rows_by_label[idx_label] = row

            if sleep_interval > 0:
                sleep(sleep_interval)

    results = DataFrame.from_dict(rows_by_label, orient="index").reindex(df.index)

    return results
