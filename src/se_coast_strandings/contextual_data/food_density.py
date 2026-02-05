from pandas import DataFrame
from requests import Session


def fetch_food_density_context(
    df: DataFrame,
    lat_column: str,
    lon_column: str,
    date_column: str,
):

    session = Session()
    pass
