# South-East Coast Marine Mammal Strandings Prediction

This research ...

## Setup
1. Create a python virtual environment at the project root and activate it.

    **Windows (Powershell)**
    ```powershell
    python -m venv .venv
    .venv/Scripts/activate
    ```
    **macOS / Linux (bash)**
    ```bash
    python3 -m venv .venv
    source .venv/bin/activate
    ```

    Then install the required packages from `requirements.txt`

    ```bash
    pip install -r requirements.txt
    ```

2. Get `pre-commit` using pip and install it to clean up notebooks when commiting them.

    ```bash
    pip install pre-commit
    pre-commit install
    pre-commit run --all-files
    ```

3. Create a folder at the root of this project called '_data_' with two more folders inside called '_raw_' and '_processed_'.

    ```bash
    mkdir data
    cd data
    mkdir raw
    mkdir processed
    ```
    Contributors of this project should retrieve the marine mammal strandings dataset from the shared drive. Then move the dataset into the `data/raw` directory.

4. Install the project package using pip.
    ```bash
    pip install -e .
    ```

## Usage

...

## Data

### Coverage & Notes

...

### Acknowledgements

The data used in this project were collected by the [U.S. Marine Mammal Stranding Network](https://www.fisheries.noaa.gov/national/marine-life-distress/national-marine-mammal-stranding-response-network). The [U.S. National Marine Mammal Stranding Database](https://www.fisheries.noaa.gov/national/marine-life-distress/national-stranding-database-public-access) was queried on January 30, 2026; data may contain errors and/or be incomplete.

The findings and conclusions in this paper are those of the author(s) and do not necessarily represent the views of the [Marine Mammal Health and Stranding Response Program](https://www.fisheries.noaa.gov/national/marine-life-distress/marine-mammal-health-and-stranding-response-program) and/or [NOAA](https://www.noaa.gov/) and/or all the members of the [National Marine Mammal Stranding Response Network](https://www.fisheries.noaa.gov/national/marine-life-distress/national-marine-mammal-stranding-response-network).


## Contributors
- James Holmes ([@jameshlms](https://github.com/jameshlms))
- Daivik Nambiar ([@daiviknambiar](https://github.com/daiviknambiar))
- Calvin Zheng ([@Calvinzheng123](https://github.com/Calvinzheng123))
- Aditri Mohanty ([@AditriM](https://github.com/AditriM))
- Tanushri Ravada ([@travada12](https://github.com/travada12))
- Jennifer Cotto ([@jencotto](https://github.com/jencotto))
- Shefali Aswal ([@saswal1](https://github.com/saswal1))

