import polars as pl
import toml
from ._dataframe_utils import DataFrameType, to_prefered_type
from glaciers import get_config

async def async_decode_folder(
    decoder_type: str,
    folder_path = None,   
    abi_db_path = None,
) -> None:
    """
    Asynchronously decode blockchain data from all files in a folder, provided the path to the folder and the path to the ABI DB file.
    Decoded files are saved in a "decoded" folder, in the parent folder of the raw data.
    The file name is the same as the raw file name, but with the "logs" or "traces" replaced with "decoded_logs" or "decoded_traces".
    It spawns a task for each file to parallelize the decoding process.

    Args:
        decoder_type (str): Type of decoder to use. Must be either "log" or "trace".
        folder_path (str, optional): Path to folder containing raw blockchain data. If None, uses the path set in the config.
        abi_db_path (str, optional): Path to the ABI database file. If None, uses the path set in the config.

    Returns:
        None

    Note:
        This function gets the max_concurrent_files_decoding from the config and uses it to limit the number of concurrent files that can be decoded at the same time.

    Example:
        ```python
        await async_decode_folder(
            "log",
            "data/logs",
            "ABIs/ethereum__events_abis.parquet"
        )
        ```
    """
    valid_decoder_types = ["log", "trace"]
    if decoder_type not in valid_decoder_types:
        raise ValueError(f"Decoder type must be one of {valid_decoder_types}")
    
    from . import _glaciers_python
    if folder_path is None:
        if decoder_type == "log":
            folder_path = toml.loads(get_config())["main"]["raw_logs_folder_path"]
        elif decoder_type == "trace":
            folder_path = toml.loads(get_config())["main"]["raw_traces_folder_path"]
    
    if abi_db_path is None:
        if decoder_type == "log":
            abi_db_path = toml.loads(get_config())["main"]["events_abi_db_file_path"]
        elif decoder_type == "trace":
            abi_db_path = toml.loads(get_config())["main"]["functions_abi_db_file_path"]

    result: pl.DataFrame = await _glaciers_python.decode_folder(decoder_type, folder_path, abi_db_path)
    return to_prefered_type(result)

def decode_folder(
    decoder_type: str,
    folder_path = None,   
    abi_db_path = None,
) -> None:
    """
    Decode blockchain data from all files in a folder, provided the path to the folder and the path to the ABI DB file.
    Decoded files are saved in a "decoded" folder, in the parent folder of the raw data.
    The file name is the same as the raw file name, but with the "logs" or "traces" replaced with "decoded_logs" or "decoded_traces".
    It spawns a task for each file to parallelize the decoding process.
    This is a synchronous wrapper around async_decode_folder.

    Args:
        decoder_type (str): Type of decoder to use. Must be either "log" or "trace".
        folder_path (str, optional): Path to folder containing raw blockchain data. If None, uses the path set in the config.
        abi_db_path (str, optional): Path to the ABI database file. If None, uses the path set in the config.

    Returns:
        None

    Note:
        This function gets the max_concurrent_files_decoding from the config and uses it to limit the number of concurrent files that can be decoded at the same time.

    Example:
        ```python
        decode_folder(
            "log",
            "data/logs",
            "ABIs/ethereum__events_abis.parquet"
        )
        ```
    """
    valid_decoder_types = ["log", "trace"]
    if decoder_type not in valid_decoder_types:
        raise ValueError(f"Decoder type must be one of {valid_decoder_types}")
    
    if folder_path is None:
        if decoder_type == "log":
            folder_path = toml.loads(get_config())["main"]["raw_logs_folder_path"]
        elif decoder_type == "trace":
            folder_path = toml.loads(get_config())["main"]["raw_traces_folder_path"]

    if abi_db_path is None:
        if decoder_type == "log":
            abi_db_path = toml.loads(get_config())["main"]["events_abi_db_file_path"]
        elif decoder_type == "trace":
            abi_db_path = toml.loads(get_config())["main"]["functions_abi_db_file_path"]

    import asyncio
    coroutine = async_decode_folder(decoder_type, folder_path, abi_db_path)

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