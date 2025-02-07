"""Utilities for handling both Pandas and Polars DataFrames"""
import polars as pl
import pandas as pd
import toml
from typing import Union
from glaciers import get_config

DataFrameType = Union[pl.DataFrame, pd.DataFrame]

def to_polars(df: DataFrameType) -> pl.DataFrame:
    """Convert any supported DataFrame type to Polars"""
    if isinstance(df, pl.DataFrame):
        return df
    elif isinstance(df, pd.DataFrame):
        return pl.from_pandas(df)
    else:
        raise ValueError("Input must be either a Polars or Pandas DataFrame")

def to_prefered_type(df: pl.DataFrame) -> DataFrameType:
    """Convert Polars DataFrame back to prefered type"""
    preferred_dataframe_type = toml.loads(get_config())["glaciers"]["preferred_dataframe_type"]

    if preferred_dataframe_type == "Polars":
        return df
    elif preferred_dataframe_type == "Pandas":
        return df.to_pandas()
    else:
        raise ValueError("Invalid preferred_dataframe_type specified") 