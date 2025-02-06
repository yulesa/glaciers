import polars as pl
from ._dataframe_utils import DataFrameType, to_polars, to_prefered_type
from . import _glaciers_python

async def async_decode_df_using_single_contract(
    decoder_type: str,
    df: DataFrameType,
    contract_address: str,
) -> DataFrameType:
    valid_decoder_types = ["log", "trace"]
    if decoder_type not in valid_decoder_types:
        raise ValueError(f"Decoder type must be one of {valid_decoder_types}")
    
    df_pl = to_polars(df)
    result_pl: pl.DataFrame = await _glaciers_python.decode_df_using_single_contract(decoder_type, df_pl, contract_address)
    return to_prefered_type(result_pl)

def decode_df_using_single_contract(
    decoder_type: str,
    df: DataFrameType,
    contract_address: str,
) -> DataFrameType:
    import asyncio
    coroutine = async_decode_df_using_single_contract(decoder_type, df, contract_address)
    
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