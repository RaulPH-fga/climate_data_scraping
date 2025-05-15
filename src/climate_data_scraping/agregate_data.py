from pathlib import Path
from typing import Union

import pandas as pd

from find_dates import get_random_filename


def build_state_mean_tables(base_path: Union[str, Path]) -> None:
    """
    Build daily state-average tables from NASA-POWER station CSVs and export
    one Excel file per parameter.

    Workflow
    --------
    1. Read the station catalogue to map codes (``CD_ESTACAO``) to their state
       abbreviations (``SG_ESTADO``).
    2. Pick a single random historical file—via :pyfunc:`get_random_filename`—
       to discover which parameter columns exist.
    3. For each parameter:
       • loop through every file in *history/*;  
       • extract the series for that parameter;  
       • tag each observation with its host state;  
       • store the records in a dictionary.
    4. Convert the dictionary to a ``DataFrame``, average by date across
       stations belonging to the same state, and write the result to
       ``treated_data/<parameter>.xlsx``.

    Parameters
    ----------
    base_path
        Directory that contains ``metadata/``, ``history/`` and
        ``treated_data/`` sub-folders.
    """

    base_path = Path(base_path)
    history_dir = base_path / "history"
    treated_dir = base_path / "treated_data"
    treated_dir.mkdir(parents=True, exist_ok=True)

    catalogue = pd.read_csv(base_path / "metadata" / "catalogue.csv")
    code_to_state = (
        catalogue.set_index("CD_ESTACAO")["SG_ESTADO"].astype(str).to_dict()
    )

    sample_path = history_dir / get_random_filename(str(history_dir), extension_filter=".csv")
    parameters = pd.read_csv(sample_path, nrows=0).columns[1:]

    states = ["DF", "GO", "MG", "MS", "MT", "PR", "RJ", "RS", "SC", "SP"]

    for param in parameters:
        records: dict[int, tuple] = {}
        rec_id = 0

        for csv_path in history_dir.glob("*.csv"):
            station_code = csv_path.stem.split("_")[0]
            state = code_to_state.get(station_code)

            if state not in states:
                continue

            df = (
                pd.read_csv(
                    csv_path,
                    usecols=["datetime", param],
                    parse_dates=["datetime"]
                )
                .set_index("datetime")
            )

            for ts, value in df[param].items():
                row = {uf: None for uf in states}
                row[state] = float(value)
                records[rec_id] = (ts, *row.values())
                rec_id += 1

        if not records:
            print(f"No data found for parameter '{param}'. Skipping export.")
            continue

        table = pd.DataFrame.from_dict(
            records,
            orient="index",
            columns=["date", *states],
        )

        table["date"] = pd.to_datetime(table["date"])
        table.set_index("date", inplace=True)
        table = table.groupby("date").mean(numeric_only=True)

        out_path = treated_dir / f"{param}.xlsx"
        table.to_excel(out_path)
        print(f"Exported {out_path.name}")

build_state_mean_tables("C:/Users/raulp/FG A/FG A - Área de Mercado/Extrações/NASA/extrair_dados_nasa_power/src/extrair_dados_nasa_power/")