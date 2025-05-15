from __future__ import annotations

import os
import time
import pandas as pd
import requests
from datetime import datetime
from pathlib import Path
from typing import Union

# Start timer for the entire script
beg = datetime.now()

def extract_data_coord(
    params: str = "IMERG_PRECTOT",
    coords: tuple[float, float] = (-47.81, -21.17),
    start: str = "20061001",
    end: str = "20250430",
    base_url: str = "https://power.larc.nasa.gov/api/temporal/daily/point",
    fmt: str = "CSV",
    time_standard: str = "UTC",
) -> pd.DataFrame:
    """
    Query the NASA POWER API (or an equivalent service) and return a
    daily DataFrame for the specified coordinates and period.

    Parameters
    ----------
    params : str, optional
        Name of the parameter(s) to be requested (e.g., ``"IMERG_PRECTOT"``).
    coords : tuple[float, float], optional
        Pair of (longitude, latitude) in degrees (°E, °N).
        Example: ``(-47.81, -21.17)`` for Uberlândia–MG, Brazil.
    start, end : str, optional
        Start and end dates in ``"YYYYMMDD"`` format.
    base_url : str, optional
        Base endpoint of the API. Kept as an argument to allow testing or
        future version changes.
    fmt : str, optional
        Response format of the API. Default is ``"CSV"``.
    time_standard : str, optional
        Time standard used by the API (default ``"UTC"``).

    Returns
    -------
    pd.DataFrame
        DataFrame indexed by ``datetime`` with columns converted to
        appropriate data types.
    """

    # 1) Build query parameters
    query_params = {
        "parameters": params,
        "community": "AG",
        "longitude": coords[0],
        "latitude": coords[1],
        "start": start,
        "end": end,
        "format": fmt,
        "time-standard": time_standard,
    }

    # 2) GET request
    response = requests.get(base_url, params=query_params)

    if response.status_code != 200:
        print(f"Request error: {response.status_code}")
        print(response.text)
        return pd.DataFrame()  # return empty DataFrame on failure

    csv_text = response.text

    # 3) Convert CSV to DataFrame
    lines = [row.split(",") for row in csv_text.splitlines()]

    # Locate the header line ("YEAR,DOY,...")
    for i, row in enumerate(lines):
        if "YEAR" in row:
            header_pos = i
            break

    table = lines[header_pos:]
    columns = table[0]   # header
    rows = table[1:]     # data

    df = pd.DataFrame(rows, columns=columns)

    # Type casting: YEAR, DOY → int | others → float
    for idx, col in enumerate(df.columns):
        df[col] = df[col].astype(int if idx < 2 else float)

    # 4) Create datetime index
    df["datetime"] = pd.to_datetime(
        df.iloc[:, 0].astype(str) + df.iloc[:, 1].astype(str),  # YEAR + DOY
        format="%Y%j",
    )
    df.set_index("datetime", inplace=True)
    df.drop(df.columns[[0, 1]], axis=1, inplace=True)  # remove YEAR, DOY

    # 5) Handle missing values
    df.replace(-999, pd.NA, inplace=True)

    return df


def extract_data_multiple_coordinates(
    log,
    start: str = "20060101",
    end: str = "20250514",
    base_path: Union[str, Path] = Path(
        "C:/Users/raulp/FG A/FG A - Área de Mercado/Extrações/NASA/"
        "extrair_dados_nasa_power/src/extrair_dados_nasa_power/"
    ),
    valid_states: list[str] | None = None,
    history: bool = True,
) -> None:
    """
    Download NASA POWER data for INMET automatic weather stations.

    Creates a *history* or *update* sub-folder (depending on ``history``)
    inside *base_path* and writes one CSV per station. Coastal stations and
    states not listed in ``valid_states`` are ignored.

    Parameters
    ----------
    log
        Logger or any object with a ``.info``/``.debug`` signature;
        unused here but kept for backward compatibility.
    start, end
        Date strings in YYYYMMDD format.
    base_path
        Root directory that contains *metadata* and output sub-folders.
    valid_states
        List of Brazilian state abbreviations to keep.  ``None`` → default set.
    history
        ``True`` → write to *history/*, ``False`` → *update/*.
    """
    if valid_states is None:
        valid_states = [
            "DF", "GO", "MG",
            "MS", "MT", "PR",
            "RJ", "RS", "SC",
            "SP",
        ]

    # Paths
    base_path = Path(base_path)
    nasa_dir = base_path / ("history" if history else "update")
    nasa_dir.mkdir(parents=True, exist_ok=True)

    catalogue_path = base_path / "metadata" / "catalogue.csv"
    coastal_path = base_path / "metadata" / "coastal.csv"

    # Read metadata 
    inmet_catalogue = pd.read_csv(catalogue_path)[[
        "DC_NOME", "SG_ESTADO", "VL_LATITUDE",
        "VL_LONGITUDE", "CD_ESTACAO"
    ]]
    coastal_codes = set(pd.read_csv(coastal_path)["CD_ESTACAO"])

    eligible = inmet_catalogue[
        (inmet_catalogue["SG_ESTADO"].isin(valid_states)) &
        (~inmet_catalogue["CD_ESTACAO"].isin(coastal_codes))
    ].reset_index(drop=True)

    total_valid = len(eligible)
    if total_valid == 0:
        print("No eligible stations found – aborting.")
        return

    total_data_by_state = {uf: 0 for uf in valid_states}
    routine_start = datetime.now()

    # Main loop
    for idx, station in eligible.iterrows():
        save_path = nasa_dir / f"{station['CD_ESTACAO']}_{start}_{end}.csv"
        if save_path.exists():
            continue

        consecutive_errors = 0
        while True:
            try:
                print(f"({idx + 1}/{total_valid}) Station {station['CD_ESTACAO']}")
                station_start = datetime.now()

                df = extract_data_coord(
                    coords=(station["VL_LATITUDE"], station["VL_LONGITUDE"]),
                    start=start,
                    end=end,
                )

                # Retry logic for empty frames
                if df.empty:
                    consecutive_errors += 1
                    if consecutive_errors >= 4:
                        print(
                            f"   ❌ station {station['CD_ESTACAO']} yielded empty data "
                            "four times in a row – skipping."
                        )
                        break
                    print(
                        f"   ⚠️  station {station['CD_ESTACAO']} returned no data – "
                        f"retry {consecutive_errors}/4 in 40 s…"
                    )
                    time.sleep(40)
                    continue

                # Success
                consecutive_errors = 0
                df.to_csv(save_path, index=False)
                total_data_by_state[station["SG_ESTADO"]] += 1
                print(
                    f"   ✓ completed in {datetime.now() - station_start}  "
                    f"(elapsed {datetime.now() - routine_start})"
                )

                # Sleep ~3 s per year requested
                optimal_sleep_time = 3 + (int(end[:4]) - int(start[:4]))/4
                time.sleep(optimal_sleep_time)
                break

            except Exception as err:  # noqa: BLE001
                consecutive_errors += 1
                if consecutive_errors >= 4:
                    print(
                        f"   ❌ station {station['CD_ESTACAO']} hit the same error "
                        f"four times in a row – skipping. Last error: {err}"
                    )
                    break
                print(f"   ⚠️  error ({err}); retry {consecutive_errors}/4 in 40 s…")
                time.sleep(40)

    # 4) Summary
    print(f"\nTotal time: {datetime.now() - routine_start}")
    print("Stations downloaded per state:")
    for uf, qty in total_data_by_state.items():
        print(f" • {uf}: {qty}")

print(extract_data_coord(params="IMERG_PRECTOT,T2M"))