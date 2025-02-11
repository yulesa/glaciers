import toml 
from ._dataframe_utils import DataFrameType, to_prefered_type
from . import _glaciers_python
from glaciers import get_config

def update_abi_db(abi_db_path: str | None = None, 
                 abi_folder_path: str | None = None) -> DataFrameType:
    """Updates the ABI DB file with new ABIs from the specified folder.

    Args:
        abi_db_path (str | None, optional): Path to the ABI database file. If None,
            uses the path set in the config file. Defaults to None.
        abi_folder_path (str | None, optional): Path to the folder containing ABI files.
            If None, uses the path set in the config file. Defaults to None.

    Returns:
        DataFrameType: A DataFrame containing the updated ABI items.

    Note:
        The ABI DB file is updated with the new ABIs from the specified folder.

    Examples:
        ```python
        # Update using default paths from config
        df = update_abi_db()
        
        # Update using custom paths
        df = update_abi_db("ABIs/ethereum__events__abis.parquet", "ABIs/abi_database")
        ```
    """
    if abi_db_path is None:
        abi_db_path = toml.loads(get_config())["main"]["events_abi_db_file_path"]
    if abi_folder_path is None:
        abi_folder_path = toml.loads(get_config())["main"]["abi_folder_path"]
    df = _glaciers_python.update_abi_db(abi_db_path, abi_folder_path)
    return to_prefered_type(df)

def read_new_abi_folder(abi_folder_path: str | None = None) -> DataFrameType:
    """Reads all ABI files from a specified folder.

    Args:
        abi_folder_path (str | None, optional): Path to the folder containing ABI files.
            If None, uses the path set in the config file. Defaults to None.

    Returns:
        DataFrameType: A DataFrame containing the parsed ABI items from all files
            in the specified folder.

    Examples:
        ```python
        # Read from default folder specified in config
        df = read_new_abi_folder()
        
        # Read from custom folder
        df = read_new_abi_folder("ABIs/abi_database")
        ```
    """
    if abi_folder_path is None:
        abi_folder_path = toml.loads(get_config())["main"]["abi_folder_path"]
        print(f"Reading ABI DataFrame from {abi_folder_path}")
    df = _glaciers_python.read_new_abi_folder(abi_folder_path)
    return to_prefered_type(df)

def read_new_abi_file(path: str) -> DataFrameType:
    """Reads and parses a single ABI file.

    Args:
        path (str): Path to the ABI JSON file.

    Returns:
        DataFrameType: A DataFrame containing the parsed ABI items from the file.

    Examples:
        ```python
        df = read_new_abi_file("ABIs/abi_database/0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2.json")
        ```
    """
    df = _glaciers_python.read_new_abi_file(path)
    return to_prefered_type(df)

def read_new_abi_json(abi: str, address: str) -> DataFrameType:
    """Reads and parses an ABI from a JSON string.

    Args:
        abi (str): The ABI JSON string to parse.
        address (str): The contract address associated with this ABI.

    Returns:
        DataFrameType: A DataFrame containing the parsed ABI items.

    Examples:
        ```python
        abi_json = '{"inputs": [], "name": "totalSupply", ...}'
        address = "0x123..."
        df = read_new_abi_json(abi_json, address)
        ```
    """
    df = _glaciers_python.read_new_abi_json(abi, address)
    return to_prefered_type(df)