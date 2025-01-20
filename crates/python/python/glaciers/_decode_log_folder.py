import polars as pl
from ._dataframe_utils import DataFrameType, to_prefered_type

async def async_decode_log_folder(
    log_folder_path: str,
    abi_df_path: str,
) -> DataFrameType:
    from . import _glaciers_python
    result: pl.DataFrame = await _glaciers_python.decode_log_folder(log_folder_path, abi_df_path)
    return to_prefered_type(result)

def decode_log_folder(
    log_folder_path: str,
    abi_df_path: str,
) -> DataFrameType:

    import asyncio
    coroutine = async_decode_log_folder(log_folder_path, abi_df_path)

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