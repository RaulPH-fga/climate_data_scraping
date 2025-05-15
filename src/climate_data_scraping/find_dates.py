import os
from datetime import datetime, timedelta, UTC
import random
from typing import Optional, Tuple

beg = datetime.now()


def get_random_filename(folder_path: str, *, extension_filter: Optional[str] = None) -> str:
    """Return the *name* of a random file located inside *folder_path*.

    Parameters
    ----------
    folder_path : str
        Absolute or relative path to the directory to sample from.
    extension_filter : str, optional
        If provided (e.g. ``".csv"``), only files ending with that extension
        are considered. Case-insensitive.

    Returns
    -------
    str
        The filename (not the full path) of the randomly chosen file.

    Raises
    ------
    FileNotFoundError
        If *folder_path* contains no eligible files.
    """

    if not os.path.isdir(folder_path):
        raise FileNotFoundError(f"Directory not found: {folder_path}")

    files = [
        f for f in os.listdir(folder_path)
        if os.path.isfile(os.path.join(folder_path, f))
    ]

    if extension_filter:
        ext = extension_filter.lower()
        files = [f for f in files if f.lower().endswith(ext)]

    if not files:
        raise FileNotFoundError("No files matching the criteria were found in the directory.")

    return random.choice(files)

def find_start_and_end_date(history_path: str) -> Tuple[str, str]:
    """Derive *start* and *end* dates for incremental downloads.

    The function picks **one random file** from *history_path* to infer the last
    available date (`original_end`) embedded in the filename – it assumes the
    filename ends with an eight‑digit string in ``YYYYMMDD`` format (just
    before the extension).

    * **start** is defined as two days *before* this `original_end`.
    * **end**   is defined as two days *before* *today* (UTC‑now).

    Both dates are returned as compact strings (``"YYYYMMDD"``), which can be
    used directly in the NASA POWER API.

    Parameters
    ----------
    history_path : str
        Folder containing historical CSV files previously downloaded.

    Returns
    -------
    tuple[str, str]
        A pair ``(start, end)`` in ``YYYYMMDD`` format.

    Notes
    -----
    The two‑day buffer reduces the risk of overlapping partial data when the
    source updates with a slight delay. Adjust the offset if your workflow
    changes.
    """

    # Pick a random file from the history folder and extract the trailing date
    random_file = get_random_filename(history_path)
    original_end = random_file.split("_")[2][:-4]  # grab 'YYYYMMDD'
    original_start = random_file.split("_")[1]  # grab 'YYYYMMDD'

    # Compute the new start date (two days before the last stored date)
    start = (
        datetime.strptime(original_end, "%Y%m%d") - timedelta(days=2)
    ).strftime("%Y%m%d")

    # Compute the new end date (two days before *today*, timezone‑aware)
    end = (datetime.now(UTC) - timedelta(days=2)).strftime("%Y%m%d")

    return original_start, original_end, start, end