import polars as pl
from ._dataframe_utils import DataFrameType, to_prefered_type, to_polars
from . import _glaciers_python

def polars_decode_logs(
    logs_df: DataFrameType,
    abi_df: DataFrameType,
) -> DataFrameType:
    logs_df_pl = to_polars(logs_df)
    abi_df_pl = to_polars(abi_df)
    result: pl.DataFrame = _glaciers_python.polars_decode_logs(logs_df_pl, abi_df_pl)
    return to_prefered_type(result)