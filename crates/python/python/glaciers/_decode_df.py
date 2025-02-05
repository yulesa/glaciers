import polars as pl
import toml
from ._dataframe_utils import DataFrameType, to_polars, to_prefered_type
from . import _glaciers_python
from glaciers import get_config

async def async_decode_df(
    df: DataFrameType,
    decoder_type: str,
    abi_df_path = None,
) -> DataFrameType:
    valid_decoder_types = ["log", "trace"]
    if decoder_type not in valid_decoder_types:
        raise ValueError(f"Decoder type must be one of {valid_decoder_types}")
    
    if abi_df_path is None:
        abi_df_path = toml.loads(get_config())["main"]["abi_df_file_path"]

    df_pl = to_polars(df)
    is_log_type = decoder_type == "log"
    result: pl.DataFrame = await _glaciers_python.decode_df(df_pl, abi_df_path, is_log_type)
    return to_prefered_type(result)

def decode_df(
    df: DataFrameType,
    decoder_type: str,
    abi_df_path = None,
) -> DataFrameType:
    
    if abi_df_path is None:
        abi_df_path = toml.loads(get_config())["main"]["abi_df_file_path"]

    import asyncio
    coroutine = async_decode_df(df, decoder_type, abi_df_path)

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