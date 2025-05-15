import pandas as pd

def concat_preserving_second(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    *,
    sort_index: bool = False,
) -> pd.DataFrame:
    """Concatenate *df1* and *df2* keeping values from **both** DataFrames,
    but letting *df2* **override** rows that exist in both.

    The function assumes *df1* and *df2* share exactly the same columns and use
    a meaningful index (e.g. a date index or an ID). Rows present only in one
    of the DataFrames are kept as‑is; rows present in **both** are taken *only*
    from *df2*.

    Parameters
    ----------
    df1, df2 : pandas.DataFrame
        DataFrames to combine. Columns must be identical and have matching
        dtypes.
    sort_index : bool, optional
        If *True* (default *False*), the resulting DataFrame is returned with
        its index sorted.

    Returns
    -------
    pandas.DataFrame
        Combined DataFrame where overlapping indices come from *df2*.

    Examples
    --------
    >>> merged = concat_preserving_second(old_df, new_df)
    >>> merged = concat_preserving_second(old_df, new_df, sort_index=True)
    """

    # 1) Validate columns ----------------------------------------------------
    if not df1.columns.equals(df2.columns):
        raise ValueError("DataFrames must have identical column sets.")

    # 2) Filter rows unique to df1, then concatenate with df2 ---------------
    df1_unique = df1.loc[~df1.index.isin(df2.index)]
    combined = pd.concat([df1_unique, df2])

    # 3) Optional: sort the resulting index ---------------------------------
    if sort_index:
        combined = combined.sort_index()

    return combined