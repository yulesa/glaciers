import polars as pl
from ._dataframe_utils import DataFrameType, to_polars, to_prefered_type
from . import _glaciers_python

async def async_decode_df_with_abi_df(
    df: DataFrameType,
    abi_df: DataFrameType,
    decoder_type: str,  
) -> DataFrameType:
    valid_decoder_types = ["log", "trace"]
    if decoder_type not in valid_decoder_types:
        raise ValueError(f"Decoder type must be one of {valid_decoder_types}")
    
    df_pl = to_polars(df)
    abi_df_pl = to_polars(abi_df)   
    is_log_type = decoder_type == "log"
    result_pl: pl.DataFrame = await _glaciers_python.decode_df_with_abi_df(df_pl, abi_df_pl, is_log_type)
    return to_prefered_type(result_pl)

def decode_df_with_abi_df(
    df: DataFrameType,
    abi_df: DataFrameType,
    decoder_type: str,
) -> DataFrameType:
    import asyncio
    coroutine = async_decode_df_with_abi_df(df, abi_df, decoder_type)

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