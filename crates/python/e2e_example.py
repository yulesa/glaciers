import os
from os.path import dirname
import polars as pl
import glaciers as gl
import toml
from io import StringIO

LOGS_FILE_NAME = "ethereum__logs__blocks__18426253_to_18426303_example.parquet"

python_dir = os.path.dirname(os.path.abspath(__file__))
project_dir =  dirname(dirname(python_dir))+"/"
print(f"Project dir: {project_dir}")

gl.set_config("decoder.decoded_chunk_size", 500_000)
gl.set_config_toml(project_dir+"glaciers_config_edit_example.toml")
config = gl.get_config()
print(f"Glaciers config:\n{config}")
config = toml.load(StringIO(config))

abi_file_path = project_dir+config['main']['abi_df_file_path']
abis_folder_path = project_dir+config['main']['abi_folder_path']
logs_folder_path = project_dir+config['main']['raw_logs_folder_path']

logs_df = pl.read_parquet(f"{logs_folder_path}/{LOGS_FILE_NAME}")
print(f"Raw logs file: \n{logs_df.head()}\n\n")




######## Test update_abi_df ########
# Reads ABIs in a folder and append to the abi parquet file
#
# This function loads each ABI definitions from a ABI folder and append the new itens (events and functions)
# into an existing abi DF (parquet file) the new unique entries. Function also output the dataframe.
#
# # Arguments
# - `abi_file_path`: Path to the parquet file containing the existing DataFrame.
# - `abi_folder_path`: Path to the folder containing ABI JSON files
#
# # Returns
# A `PyResult` containing a `PyDataFrame` with all unique topic0 and event signatures
#
# # Errors
# Returns a `PyValueError` if there are issues reading or processing the ABIs
abis_df = gl.update_abi_df(abi_file_path, abis_folder_path)
print(f"\nFirst 5 rows of updatedABIs DataFrame:\n{abis_df.head()}\n\n")

######## Test read_new_abi_folder ########
# Reads ABIs in a folder
#
# This function loads ABI definitions from a folder and creates a DataFrame containing
# all functions and events found in the ABI files.
#
# # Arguments
# - `abi_folder_path`: Path to the folder containing ABI JSON files
#
# # Returns
# A `PyResult` containing a `PyDataFrame` with all functions and events
#
# # Errors
# Returns a `PyValueError` if there are issues reading or processing the ABIs
folder_df = gl.read_new_abi_folder(abis_folder_path)
print(f"\nABIs DataFrame from folder:\n{folder_df.head()}\n\n")


######## Test read_new_abi_file ########
# Reads a single ABI file
#
# This function loads ABI definitions from a file and creates a DataFrame containing
# all functions and events found in the ABI file.
#
# # Arguments
# - `path`: Path to the ABI JSON file
#
# # Returns
# A `PyResult` containing a `PyDataFrame` with all functions and events
#
# # Errors
# Returns a `PyValueError` if there are issues reading or processing the ABI
abi_file = os.path.join(abis_folder_path, os.listdir(abis_folder_path)[0])  # Get first ABI file
file_df = gl.read_new_abi_file(abi_file)
print(f"\nABIs DataFrame from single file:\n{file_df.head()}\n\n")


######## Test read_new_abi_json ########
# Reads a single ABI JSON string
#
# This function loads ABI definitions from a JSON string and creates a DataFrame containing
# all functions and events found in the ABI.
#
# # Arguments
# - `abi`: ABI JSON string
# - `address`: Contract address as string
#
# # Returns
# A `PyResult` containing a `PyDataFrame` with all functions and events
#
# # Errors
# Returns a `PyValueError` if there are issues reading or processing the ABI
with open(abi_file, 'r') as f:
    abi_json = """
        [{
            "anonymous": false,
            "inputs": [
                {
                    "indexed": true,
                    "internalType": "address",
                    "name": "from",
                    "type": "address"
                },
                {
                    "indexed": true,
                    "internalType": "address",
                    "name": "to",
                    "type": "address"
                },
                {
                    "indexed": false,
                    "internalType": "uint256",
                    "name": "value",
                    "type": "uint256"
                }
            ],
            "name": "Transfer",
            "type": "event"
        }]
    """
    address = "0xE672E0E0101A7F58d728751E2a5e6Da5Ff1FDa64"
    item_df = gl.read_new_abi_json(abi_json, address)
    print(f"\nABIs DataFrame from JSON string:\n{item_df.head()}\n\n")



######## Test decode_log_folder ########
# Decode a folder of logs.
#
# This function takes a logs folder path and a abi parquet file path. It iterate through 
# logs files, decode, and save them into decoded logs' files.
#
# # Arguments
# - `log_folder_path`: Path to a folder containing the logs files
# - `abi_file_path`: Path to the abi parquet file
#
# # Returns
# No Return
#
# # Errors
# Returns a `PyValueError` if there are issues processing the logs
gl.decode_log_folder(logs_folder_path, abi_file_path)
print(f"\n Decoded logs saved in the decoded folder.\n\n")


######## Test decode_log_file ########
# Decode a log file
#
# This function takes a log file path and a abi parquet file path and decode the file
# to a decoded logs' DataFrame.
#
# # Arguments
# - `log_file_path`: Path to the log file
# - `abi_df_path`: Path to the abi parquet file
#
# # Returns
# A `PyResult` containing a decoded logs' `PyDataFrame` or an error
log_file = os.path.join(logs_folder_path, os.listdir(logs_folder_path)[0])  # Get first log file
decoded_df = gl.decode_log_file(log_file, abi_file_path)
print(f"\nDecoded Logs in the log file {log_file}:\n{decoded_df.head()}\n\n")


######## Test decode_log_df ########
# Decode a logs' DataFrame
#
# This function takes a raw logs' DataFrame and a abi parquet file path and decode the df
# to a decoded logs' DataFrame.
#
# # Arguments
# - `logs_df`: A DataFrame containing raw blockchain logs
# - `abi_file_path`: Path to the abi file containing the topic0 and event signatures
#
# # Returns
# A `PyResult` containing a decoded logs' `PyDataFrame` or an error
#
# # Errors
# Returns a `PyValueError` if there are issues processing the logs
logs_df = pl.read_parquet(f"{logs_folder_path}/{LOGS_FILE_NAME}")
decoded_df = gl.decode_log_df(logs_df, abi_file_path)
print(f"\nDecoded Logs DataFrame:\n{decoded_df.head()}\n\n")


######## Test decode_log_df_with_abi_df ########
# Decode a logs' DataFrame with a abi DataFrame
#
# This function takes a raw logs' DataFrame and a abi DataFrame and decode the df
# to a decoded logs' DataFrame.
#
# # Arguments
# - `logs_df`: A DataFrame containing raw blockchain logs
# - `abi_df`: A DataFrame containing the ABI definitions
#
# # Returns
# A `PyResult` containing a decoded logs' `PyDataFrame` or an error

logs_df = pl.read_parquet(f"{logs_folder_path}/{LOGS_FILE_NAME}")
abi_df = pl.read_parquet(abi_file_path)
decoded_df = gl.decode_log_df_with_abi_df(logs_df, abi_df)
print(f"\nDecoded Logs using ABI DataFrame:\n{decoded_df.head()}\n\n")


######## Test polars_decode_logs ########
# Decode dataframe event logs using ABI definitions dataframe, without multi-threading
#
# Args:
#     logs_df: A DataFrame containing the raw logs.
#     abi_df: A DataFrame containing:
#         - topic0: The topic0 (event signature hash) as bytes
#         - full_signature: The full event signature as string (e.g. "Transfer(address indexed from, address indexed to, uint256 value)")
#
# Returns:
#     A DataFrame containing the decoded events with additional columns:
#     - event_values: The decoded parameter values
#     - event_keys: The parameter names
#     - event_json: JSON representation of the decoded event
# Transfer event
TRANSFER_EVENT = "event Transfer(address indexed from, address indexed to, uint256 value)"
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
# Create ABI DataFrame
abi_df = pl.DataFrame(
    {
        "topic0": TRANSFER_TOPIC,
        "full_signature": TRANSFER_EVENT,
        "name": "Transfer",
    }
)
# Decode the events
decoded_df = gl.polars_decode_logs(logs_df, abi_df)
print(f"\nDecoded Logs DataFrame:\n{decoded_df.head(5)}\n\n")


######## Test unnest_event ########
# Filter and unnest an event from a decoded logs' DataFrame. After filtering using the combination of the
# optional arguments, the event needs to be unique.
#
# Arguments
# - `decoded_log_df`: A DataFrame containing the decoded logs
# - `event_name`: Optional, default None. The name of the event to unnest
# - `full_signature`: Optional, default None. The full signature of the event to unnest
# - `event_address`: Optional, default None. The address of the event to unnest
# - `topic0`: Optional, default None. The topic0 of the event to unnest
#
# Returns
# A `PyResult` containing a unnested event's `PyDataFrame` or an error
#
# Errors
# Returns a `PyValueError` if there are issues processing the unnesting or the event is not unique.

unnested_df = gl.unnest_event(decoded_df, full_signature='event Transfer(address indexed from, address indexed to, uint256 value)')
print(f"\nUnnested Logs DataFrame:\n{unnested_df.head(5)}\n\n")