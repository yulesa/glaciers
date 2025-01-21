import polars as pl
from ._dataframe_utils import DataFrameType, to_polars, to_prefered_type
from . import _glaciers_python

async def async_decode_log_df_using_single_contract(
    log_df: DataFrameType,
    contract_address: str,
) -> DataFrameType:
    log_df_pl = to_polars(log_df)
    result_pl: pl.DataFrame = await _glaciers_python.decode_log_df_using_single_contract(log_df_pl, contract_address)
    return to_prefered_type(result_pl)

def decode_log_df_using_single_contract(
    log_df: DataFrameType,
    contract_address: str,
) -> DataFrameType:
    import asyncio
    coroutine = async_decode_log_df_using_single_contract(log_df, contract_address)
    
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