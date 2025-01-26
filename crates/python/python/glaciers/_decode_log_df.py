import polars as pl
from ._dataframe_utils import DataFrameType, to_polars, to_prefered_type
from . import _glaciers_python

async def async_decode_log_df(
    logs_df: DataFrameType,
    abi_df_path: str,
) -> DataFrameType:
    logs_df_pl = to_polars(logs_df)
    result: pl.DataFrame = await _glaciers_python.decode_log_df(logs_df_pl, abi_df_path)
    return to_prefered_type(result)

def decode_log_df(
    logs_df: DataFrameType,
    abi_df_path: str,
) -> DataFrameType:

    import asyncio
    coroutine = async_decode_log_df(logs_df, abi_df_path)

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