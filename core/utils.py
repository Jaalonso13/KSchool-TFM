from __future__ import annotations

import json
import pandas as pd

def read_json_file(path: str) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as err:
        raise RuntimeError(err)
    
def cast_df(df: pd.DataFrame, cast: dict) -> pd.DataFrame:
    for key, value in cast.items():
        df[key] = df[key].astype(value)
    return df