# Latitude-band region definitions for the SE coast.
# Each entry is a (label, lat_min, lat_max) tuple in decimal degrees.
DEFAULT_REGIONS: list[tuple[str, float, float]] = [
    ("SC", 32.0, 33.5),
    ("NC-south", 33.5, 34.75),
    ("NC-north", 34.75, 36.25),
    ("VA", 36.25, 38.0),
]

MAX_LAT = 38
MIN_LAT = 32


def make_region(length_per_coast: int) -> list[tuple[str, float, float]]:
    """Make a list of regions with the specified number of points per coast."""
    latitudes = [
        MIN_LAT + i * (MAX_LAT - MIN_LAT) / length_per_coast
        for i in range(length_per_coast + 1)
    ]
    return [(f"R{i}", latitudes[i], latitudes[i + 1]) for i in range(length_per_coast)]


def make_degrees(degrees_per_band: float) -> list[tuple[str, float, float]]:
    """Make regions by specifying the width of each latitude band in decimal degrees.

    The SE coast range (MIN_LAT to MAX_LAT) is divided into equal-width bands.
    The last band is clamped to MAX_LAT if the range doesn't divide evenly.

    Args:
        degrees_per_band: Width of each latitude band in decimal degrees.

    Returns:
        List of (label, lat_min, lat_max) tuples.
    """
    import math

    n = math.ceil((MAX_LAT - MIN_LAT) / degrees_per_band)
    return [
        (
            f"R{i}",
            MIN_LAT + i * degrees_per_band,
            min(MIN_LAT + (i + 1) * degrees_per_band, MAX_LAT),
        )
        for i in range(n)
    ]
