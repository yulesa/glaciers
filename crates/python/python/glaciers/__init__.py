"""glaciers is a tool to decode EVM logs"""
from ._glaciers_python import get_config
from ._glaciers_python import set_config
from ._glaciers_python import set_config_toml
from ._decode_log_folder import async_decode_log_folder
from ._decode_log_folder import decode_log_folder
from ._decode_log_file import async_decode_log_file
from ._decode_log_file import decode_log_file
from ._decode_log_df import async_decode_log_df
from ._decode_log_df import decode_log_df
from ._decode_log_df_with_abi_df import async_decode_log_df_with_abi_df
from ._decode_log_df_with_abi_df import decode_log_df_with_abi_df
from ._polars_decode_logs import polars_decode_logs
from ._decode_log_df_using_single_contract import decode_log_df_using_single_contract
from ._dataframe_utils import to_prefered_type, DataFrameType
from ._unnest import unnest_event

def update_abi_df(path: str, folder: str) -> DataFrameType:
    df = _glaciers_python.update_abi_df(path, folder)
    return to_prefered_type(df)

def read_new_abi_folder(folder: str) -> DataFrameType:
    df = _glaciers_python.read_new_abi_folder(folder)
    return to_prefered_type(df)

def read_new_abi_file(file: str) -> DataFrameType:
    df = _glaciers_python.read_new_abi_file(file)
    return to_prefered_type(df)

def read_new_abi_json(file: str, address: str) -> DataFrameType:
    df = _glaciers_python.read_new_abi_json(file, address)
    return to_prefered_type(df)

__all__ = [
    'update_abi_df',
    'read_new_abi_folder',
    'read_new_abi_file',
    'read_new_abi_json',
    'async_decode_log_folder',
    'decode_log_folder',
    'async_decode_log_file',
    'decode_log_file',
    'async_decode_log_df',
    'decode_log_df',
    'async_decode_log_df_with_abi_df',
    'decode_log_df_with_abi_df',
    'polars_decode_logs',
    'decode_log_df_using_single_contract',
    'get_config',
    'set_config',
    'set_config_toml',
    'unnest_event'
]
