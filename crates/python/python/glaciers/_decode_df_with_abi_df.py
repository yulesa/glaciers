import polars as pl
import polars as pl
from ._dataframe_utils import DataFrameType, to_polars, to_prefered_type
from . import _glaciers_python

async def async_decode_df_with_abi_df(
    decoder_type: str,  
    df: DataFrameType,
    abi_df: DataFrameType,
) -> DataFrameType:
    """
    Asynchronously decode blockchain data from a DataFrame and an ABI DataFrame.

    Args:
        decoder_type (str): Type of decoder to use. Must be either "log" or "trace".
        df (DataFrameType): DataFrame (polars or pandas) containing the raw blockchain data.
        abi_df (DataFrameType): DataFrame (polars or pandas) containing the ABI DB items.

    Returns:
        DataFrameType: Decoded DataFrame (polars or pandas according to the config) with the results.

    Example:
        ```python
        decoded_df = await async_decode_df_with_abi_df(
            "log",
            raw_logs_df,
            abi_db_df
        )
        ```
    """
    valid_decoder_types = ["log", "trace"]
    if decoder_type not in valid_decoder_types:
        raise ValueError(f"Decoder type must be one of {valid_decoder_types}")
    
    df_pl = to_polars(df)
    abi_df_pl = to_polars(abi_df)   
    result_pl: pl.DataFrame = await _glaciers_python.decode_df_with_abi_df(decoder_type, df_pl, abi_df_pl)
    return to_prefered_type(result_pl)

def decode_df_with_abi_df(
    decoder_type: str,
    df: DataFrameType,
    abi_df: DataFrameType,
) -> DataFrameType:
    """
    Decode blockchain data from a DataFrame and an ABI DataFrame.

    This is a synchronous wrapper around async_decode_df_with_abi_df.

    Args:
        decoder_type (str): Type of decoder to use. Must be either "log" or "trace".
        df (DataFrameType): DataFrame (polars or pandas) containing the raw blockchain data.
        abi_df (DataFrameType): DataFrame (polars or pandas) containing the ABI DB items.

    Returns:
        DataFrameType: Decoded DataFrame (polars or pandas according to the config) with the results.

    Example:
        ```python
        decoded_df = decode_df_with_abi_df(
            "log",
            raw_logs_df,
            abi_db_df
        )
        ```
    """
    import asyncio
    coroutine = async_decode_df_with_abi_df(decoder_type, df, abi_df)

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