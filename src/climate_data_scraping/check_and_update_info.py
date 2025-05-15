from datetime import datetime, timedelta
import os
from pathlib import Path
from typing import Union

import pandas as pd

from concatenate import concat_preserving_second
from delete_file import safe_delete
from extract_data import extract_data_multiple_coordinates
from find_dates import find_start_and_end_date

def check_up_to_date(base_path: Union[str, Path]) -> bool:
    """
    Determine whether the dataset stored in *base_path/history* is already
    up-to-date.

    The helper ``find_start_and_end_date`` reads the date metadata from files
    in the *history* folder and returns four dates:

    1. ``orig_start`` : first date present in the existing consolidated file  
    2. ``orig_end``   : last   date present in the existing consolidated file  
    3. ``new_start``  : first date in the freshly scraped data waiting to be
       appended  
    4. ``new_end``    : last   date in the freshly scraped data waiting to be
       appended  

    If ``new_end`` is equal to ``orig_end`` there is no new data to add, so
    the function returns ``True``; otherwise it returns ``False``.

    Parameters
    ----------
    base_path : str | pathlib.Path
        Root directory of the project. The function expects a sub-directory
        named **history** inside ``base_path``.

    Returns
    -------
    bool
        ``True`` when the dataset is current, ``False`` when updates are
        required.
    """
    # Ensure we are working with a Path object
    base = Path(base_path)

    # Location of historical files (e.g. CSVs, Parquet) containing date stamps
    history_dir = base / "history"

    # Unpack the four relevant dates
    orig_start, orig_end, new_start, new_end = find_start_and_end_date(history_dir)

    # Dataset is current only when the most recent end dates match
    return new_end == orig_end

def update_history(base_path: str) -> None:
    """Update historical NASA POWER station files with the latest *update* set.

    The function performs three high‑level steps:

    1. **Determine date window** using :pyfunc:`find_start_and_end_date`.
    2. **Download** the update set (delegated to
       :pyfunc:`extract_data_multiple_coordinates`).
    3. **Merge** each update CSV with its matching historical CSV, letting the
       update override overlaps, and save a new file whose end‑date reflects
       the latest data.

    Parameters
    ----------
    base_path : str
        Root path where the sub‑folders ``history/`` and ``update/`` live.

    Notes
    -----
    *Historical filenames must follow* ``<code>_<start>_<end>.csv`` *pattern.*
    """

    base = Path(base_path)
    history_dir = base / "history"
    update_dir = base / "update"

    # 1) Date window
    orig_start, orig_end, new_start, new_end = find_start_and_end_date(history_dir)

    # 2) Download new data (optional)
    extract_data_multiple_coordinates(
        log=None,
        start=new_start,
        end=new_end,
        base_path=str(base),
        history=False,
    )

    # 3) Merge each update file with its historical counterpart
    for upd_file in update_dir.glob("*.csv"):
        update_df = (
            pd.read_csv(upd_file, parse_dates=["datetime"]).set_index("datetime")
        )
        code = upd_file.stem.split("_")[0]

        # find the first matching historical file for this code
        try:
            hist_file = next(f for f in history_dir.glob(f"{code}_*_*.csv"))
        except StopIteration:
            print(f"No historical file found for station code {code}; skipping.")
            continue

        history_df = (
            pd.read_csv(hist_file, parse_dates=["datetime"]).set_index("datetime")
        )

        # remove the old historical file
        safe_delete(hist_file)

        # concatenate and save with updated end‑date
        out_path = history_dir / f"{code}_{orig_start}_{new_end}.csv"
        concat_preserving_second(history_df, update_df).to_csv(out_path)
        print(f"Updated history for {code} → {out_path.name}")

base = "C:/Users/raulp/FG A/FG A - Área de Mercado/Extrações/NASA/extrair_dados_nasa_power/src/extrair_dados_nasa_power/"

if not check_up_to_date(base):
    update_history(base)
else:
    print("Dados já atualizados")