import polars as pl
import toml
from ._dataframe_utils import DataFrameType, to_prefered_type
from . import _glaciers_python
from glaciers import get_config

async def async_decode_file(
    decoder_type: str,
    file_path: str,
    abi_db_path = None,
) -> DataFrameType:
    valid_decoder_types = ["log", "trace"]
    if decoder_type not in valid_decoder_types:
        raise ValueError(f"Decoder type must be one of {valid_decoder_types}")
    
    if abi_db_path is None:
        if decoder_type == "log":
            abi_db_path = toml.loads(get_config())["main"]["events_abi_db_file_path"]
        elif decoder_type == "trace":
            abi_db_path = toml.loads(get_config())["main"]["functions_abi_db_file_path"]

    result: pl.DataFrame = await _glaciers_python.decode_file(decoder_type, file_path, abi_db_path)
    return to_prefered_type(result)

def decode_file(
    decoder_type: str,
    file_path: str,
    abi_db_path= None,
) -> DataFrameType:
    if abi_db_path is None:
        if decoder_type == "log":
            abi_db_path = toml.loads(get_config())["main"]["events_abi_db_file_path"]
        elif decoder_type == "trace":
            abi_db_path = toml.loads(get_config())["main"]["functions_abi_db_file_path"]

    import asyncio
    coroutine = async_decode_file(decoder_type, file_path, abi_db_path)

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