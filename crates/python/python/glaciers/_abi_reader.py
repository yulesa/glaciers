import toml 
from ._dataframe_utils import DataFrameType, to_prefered_type
from . import _glaciers_python
from glaciers import get_config

def update_abi_db(abi_db_path = None, abi_folder_path = None) -> DataFrameType:
    if abi_db_path is None:
        abi_db_path = toml.loads(get_config())["main"]["events_abi_db_file_path"]
    if abi_folder_path is None:
        abi_folder_path = toml.loads(get_config())["main"]["abi_folder_path"]
    df = _glaciers_python.update_abi_db(abi_db_path, abi_folder_path)
    return to_prefered_type(df)

def read_new_abi_folder(abi_folder_path = None) -> DataFrameType:
    if abi_folder_path is None:
        abi_folder_path = toml.loads(get_config())["main"]["abi_folder_path"]
        print(f"Reading ABI DataFrame from {abi_folder_path}")
    df = _glaciers_python.read_new_abi_folder(abi_folder_path)
    return to_prefered_type(df)

def read_new_abi_file(path: str) -> DataFrameType:
    df = _glaciers_python.read_new_abi_file(path)
    return to_prefered_type(df)

def read_new_abi_json(abi: str, address: str) -> DataFrameType:
    df = _glaciers_python.read_new_abi_json(abi, address)
    return to_prefered_type(df)