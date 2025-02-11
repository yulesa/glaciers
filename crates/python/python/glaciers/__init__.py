"""Glaciers: Glaciers is a tool for batch decoding EVM (Ethereum Virtual Machine) 
raw logs and traces files, producing respective decoded tables. 

This package provides utilities for working with EVM logs and traces files, 
including ABI management, log decoding, and configuration handling.

Functions:
    Configuration:
        get_config(): Get the current configuration as a TOML string
        set_config(key: str, value: str): Set a specific configuration item
        set_config_toml(config: str): Set configuration using a TOML string

    ABI Management:
        update_abi_db(): Update an ABI database file with new ABIs from a folder
        read_new_abi_folder(): Read ABIs from a folder and return a DataFrame
        read_new_abi_file(): Read ABI from a file and return a DataFrame
        read_new_abi_json(): Parse ABI from JSON string and return a DataFrame

    Log/Traces Decoding:
        async_decode_folder(): Asynchronously decode logs from a folder
        decode_folder(): Decode logs from a folder
        async_decode_file(): Asynchronously decode logs from a file
        decode_file(): Decode logs from a file
        async_decode_df(): Asynchronously decode logs in a DataFrame
        decode_df(): Decode logs in a DataFrame
        async_decode_df_with_abi_df(): Decode logs using custom ABI DataFrame
        decode_df_with_abi_df(): Decode logs using custom ABI DataFrame
        decode_df_using_single_contract(): Decode logs for a specific contract
        unnest_event(): Unnest decoded event data
"""

from ._glaciers_python import get_config
from ._glaciers_python import set_config
from ._glaciers_python import set_config_toml
from ._abi_reader import update_abi_db
from ._abi_reader import read_new_abi_folder
from ._abi_reader import read_new_abi_file
from ._abi_reader import read_new_abi_json
from ._decode_folder import async_decode_folder
from ._decode_folder import decode_folder
from ._decode_file import async_decode_file
from ._decode_file import decode_file
from ._decode_df import async_decode_df
from ._decode_df import decode_df
from ._decode_df_with_abi_df import async_decode_df_with_abi_df
from ._decode_df_with_abi_df import decode_df_with_abi_df
from ._decode_df_using_single_contract import decode_df_using_single_contract
from ._unnest import unnest_event

def get_config() -> str:
    """Get the current Glaciers configuration as a TOML string.

    Returns:
        str: The current configuration in TOML format.

    Example:
        ```python
        config = glaciers.get_config()
        print(config)
        # [main]
        # abi_folder_path = "ABIs/abi_database"
        # events_abi_db_file_path = "ABIs/ethereum__events__abis.parquet"
        ```
    """
    return _glaciers_python.get_config()

def set_config(key: str, value: str) -> None:
    """Set a specific configuration item.

    Args:
        key (str): The configuration key in dot notation (e.g., "main.abi_folder_path")
        value (str): The value to set for the configuration key

    Raises:
        ValueError: If the key or value is invalid or the section doesn't exist
        
    Example:
        ```python
        glaciers.set_config("main.abi_folder_path", "ABIs/abi_database")
        ```
    """
    return _glaciers_python.set_config(key, value)

def set_config_toml(config: str) -> None:
    """Set multiple configuration items using a TOML string.

    Args:
        config (str): The path to a TOML-formatted file containing the configuration that will be updated

    Raises:
        ValueError: If the file does not exist or is invalid, if the key or value being set is invalid
        
    Example:
        ```python
        config_toml_path = "path/to/config.toml"
        glaciers.set_config_toml(config_toml_path)
        ```
    """
    return _glaciers_python.set_config_toml(config)

__all__ = [
    'update_abi_db',
    'read_new_abi_folder',
    'read_new_abi_file',
    'read_new_abi_json',
    'async_decode_folder',
    'decode_folder',
    'async_decode_file',
    'decode_file',
    'async_decode_df',
    'decode_df',
    'async_decode_df_with_abi_df',
    'decode_df_with_abi_df',
    'decode_df_using_single_contract',
    'get_config',
    'set_config',
    'set_config_toml',
    'unnest_event'
]
