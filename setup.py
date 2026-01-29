from pathlib import Path

import requests


def main():
    r = requests.get(
        "https://www.fisheries.noaa.gov/s3/2023-06/EastCoast-Bottlenose-2017-2019.xlsx"
    )

    Path("data/raw").mkdir(parents=True, exist_ok=True)

    with open("data/raw/EastCoast-Bottlenose-2017-2019.xlsx", "wb") as f:
        f.write(r.content)

    r.close()


if __name__ == "__main__":
    main()
