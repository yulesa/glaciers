import polars as pl

import polars as pl
import toml
from ._dataframe_utils import DataFrameType, to_polars, to_prefered_type
from . import _glaciers_python
from glaciers import get_config

async def async_decode_df(
    decoder_type: str,
    df: DataFrameType,
    abi_db_path = None,
) -> DataFrameType:
    """
    Asynchronously decode blockchain data from a DataFrame and an the path to the ABI DB file.

    Args:
        decoder_type (str): Type of decoder to use. Must be either "log" or "trace".
        df (DataFrameType): DataFrame (polars or pandas) containing the raw blockchain data.
        abi_db_path (str, optional): Path to the ABI database file. If None, uses the path set in the config.

    Returns:
        DataFrameType: Decoded DataFrame (polars or pandas according to the config) with the results.

    Example:
        ```python
        decoded_df = await async_decode_df(
            "log",
            raw_logs_df,
            "ABIs/ethereum__events_abis.parquet"
        )
        ```
    """
    valid_decoder_types = ["log", "trace"]
    if decoder_type not in valid_decoder_types:
        raise ValueError(f"Decoder type must be one of {valid_decoder_types}")
    
    if abi_db_path is None:
        if decoder_type == "log":
            abi_db_path = toml.loads(get_config())["main"]["events_abi_db_file_path"]
        elif decoder_type == "trace":
            abi_db_path = toml.loads(get_config())["main"]["functions_abi_db_file_path"]

    df_pl = to_polars(df)
    result: pl.DataFrame = await _glaciers_python.decode_df(decoder_type, df_pl, abi_db_path)
    return to_prefered_type(result)

def decode_df(
    decoder_type: str,
    df: DataFrameType,
    abi_db_path = None,
) -> DataFrameType:
    """
    Decode blockchain data from a DataFrame and an the path to the ABI DB file.

    This is a synchronous wrapper around async_decode_df.

    Args:
        decoder_type (str): Type of decoder to use. Must be either "log" or "trace".
        df (DataFrameType): DataFrame (polars or pandas) containing the raw blockchain data.
        abi_db_path (str, optional): Path to the ABI database file. If None, uses the path set in the config.

    Returns:
        DataFrameType: Decoded DataFrame (polars or pandas according to the config) with the results.

    Example:
        ```python
        decoded_df = decode_df(
            "log",
            raw_logs_df,
            "ABIs/ethereum__events_abis.parquet"
        )
        ```
    """
    if abi_db_path is None:
        if decoder_type == "log":
            abi_db_path = toml.loads(get_config())["main"]["events_abi_db_file_path"]
        elif decoder_type == "trace":
            abi_db_path = toml.loads(get_config())["main"]["functions_abi_db_file_path"]

    import asyncio
    coroutine = async_decode_df(decoder_type, df, abi_db_path)

    try:
        import concurrent.futures

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(loop.run_until_complete, coroutine)
            result = future.result()
    except RuntimeError:
        result = asyncio.run(coroutine)

    return result