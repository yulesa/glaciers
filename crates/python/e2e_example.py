import os
from os.path import dirname
import polars as pl
import glaciers as gl
import toml
from io import StringIO

# Get the project directory
python_dir = os.path.dirname(os.path.abspath(__file__))
project_dir =  dirname(dirname(python_dir))+"/"
print(f"Project dir: {project_dir}")

# Set and get the configs
gl.set_config("decoder.decoded_chunk_size", 500_000)
gl.set_config_toml(project_dir+"glaciers_config_edit_example.toml")
config = gl.get_config()
print(f"Glaciers config:\n{config}")
config = toml.load(StringIO(config))

# Instantiate the paths to the files and folders
events_abi_file_path = project_dir+config['main']['events_abi_db_file_path']
functions_abi_file_path = project_dir+config['main']['functions_abi_db_file_path']
abi_folder_path = project_dir+config['main']['abi_folder_path']
logs_folder_path = project_dir+config['main']['raw_logs_folder_path']
traces_folder_path = project_dir+config['main']['raw_traces_folder_path']


######## Test update_abi_db ########
# Reads ABIs in a folder and append to the abi parquet file
#
# This function loads each ABI definitions from a ABI folder and append the new itens (events and functions, according to the configs)
# into an existing abi DF (parquet file) the new unique entries. Function also output the dataframe.
#
# # Arguments
# - `abi_db_path`: Optional, default in config. Path to the parquet file containing the existing DataFrame.
# - `abi_folder_path`: Optional, default in config. Path to the folder containing ABI JSON files
#
# # Returns
# A `PyResult` containing a `PyDataFrame` with all functions and events itens signatures
#
# # Errors
# Returns a `PyValueError` if there are issues reading or processing the ABIs
abis_df = gl.update_abi_db(abi_db_path=events_abi_file_path, abi_folder_path=abi_folder_path)
print(f"\nFirst 5 rows of updatedABIs DataFrame:\n{abis_df.head()}\n\n")


######## Test read_new_abi_folder ########
# Reads ABIs in a folder
#
# This function loads ABI files from a folder and creates a DataFrame containing functions 
# and events itens (according to the configs) found in the ABI files. It doesn't save them in a DB file.
#
# # Arguments
# - `abi_folder_path`: Optional, default in config. Path to the folder containing ABI JSON files
#
# # Returns
# A `PyResult` containing a `PyDataFrame` with all functions and events itens signatures
#
# # Errors
# Returns a `PyValueError` if there are issues reading or processing the ABIs
folder_df = gl.read_new_abi_folder(abi_folder_path=abi_folder_path)
print(f"\nABIs DataFrame from folder:\n{folder_df.head()}\n\n")


######## Test read_new_abi_file ########
# Reads a single ABI file
#
# This function loads ABI definitions from a file and creates a DataFrame containing functions 
# and events itens (according to the configs) found in the ABI file.
#
# # Arguments
# - `path`: Path to the ABI JSON file
#
# # Returns
# A `PyResult` containing a `PyDataFrame` with all functions and events itens signatures
#
# # Errors
# Returns a `PyValueError` if there are issues reading or processing the ABI
abi_file = os.path.join(abi_folder_path, os.listdir(abi_folder_path)[0])  # Get first ABI file
file_df = gl.read_new_abi_file(path=abi_file)
print(f"\nABIs DataFrame from single file:\n{file_df.head()}\n\n")


######## Test read_new_abi_json ########
# Reads a single ABI JSON string
#
# This function loads ABI definitions from a JSON string and creates a DataFrame containing
# functions and events itens (according to the configs) found in the ABI.
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



######## Test decode_folder ########
# Decode a folder of logs/traces.
#
# This function takes a logs folder path and a abi parquet file path. It iterate through 
# logs files, decode, and save them into decoded logs' files.
#
# # Arguments
# - `decoder_type`: Type of the decoder to use, allowed values = ["log", "trace"]
# - `abi_db_path`: Optional, default in config. Path to the abi parquet file
# - `folder_path`: Optional, default in config. Path to the folder containing the logs/traces files
#
# # Returns
# No Return
#
# # Errors
# Returns a `PyValueError` if there are issues processing the logs
gl.decode_folder(decoder_type="log", abi_db_path=events_abi_file_path, folder_path=logs_folder_path)
print(f"\n Decoded logs saved in the decoded folder.\n\n")


######## Test decode_file ########
# Decode a log/trace file
#
# This function takes a log file path and a abi db parquet file and decode the file
# to a decoded logs/traces DataFrame.
#
# # Arguments
# - `decoder_type`: Type of the decoder to use, allowed values = ["log", "trace"]
# - `file_path`: Optional, default in config. Path to the log/trace file
# - `abi_db_path`: Optional, default in config. Path to the abi parquet file
#
# # Returns
# A `PyResult` containing a decoded logs/traces DataFrame or an error
trace_file = os.path.join(traces_folder_path, os.listdir(traces_folder_path)[0])  # Get first trace file
decoded_df = gl.decode_file(decoder_type="trace", file_path=trace_file, abi_db_path=functions_abi_file_path)
print(f"\nDecoded Traces in the trace file {trace_file}:\n{decoded_df.head()}\n\n")


######## Test decode_df ########
# Decode a logs/traces DataFrame
#
# This function takes a raw logs/traces DataFrame and a abi DB parquet file path and decode the df
# to a decoded logs/traces DataFrame.
#
# # Arguments
# - `df`: A DataFrame containing raw blockchain logs/traces
# - `decoder_type`: Type of the decoder to use, allowed values = ["log", "trace"]
# - `abi_db_path`: Optional, default in config. Path to the abi parquet file
#
# # Returns
# A `PyResult` containing a decoded logs/traces DataFrame or an error
#
# # Errors
# Returns a `PyValueError` if there are issues processing the logs

logs_df_path = os.path.join(logs_folder_path, os.listdir(logs_folder_path)[0])  # Get first logs file
logs_df = pl.read_parquet(logs_df_path)
decoded_df = gl.decode_df(df=logs_df, decoder_type="log", abi_db_path=events_abi_file_path)
print(f"\nDecoded Logs DataFrame:\n{decoded_df.head()}\n\n")


######## Test decode_df_with_abi_df ########
# Decode a logs/traces DataFrame with a abi DataFrame
#
# This function takes a raw logs/traces DataFrame and a abi DataFrame and decode the df
# to a decoded logs/traces DataFrame.
#
# # Arguments
# - `df`: A DataFrame containing raw blockchain logs/traces
# - `abi_df`: A DataFrame containing the ABI definitions
# - `decoder_type`: Type of the decoder to use, allowed values = ["log", "trace"]
#
# # Returns
# A `PyResult` containing a decoded logs/traces DataFrame or an error
logs_df_path = os.path.join(logs_folder_path, os.listdir(logs_folder_path)[0])  # Get first logs file
logs_df = pl.read_parquet(logs_df_path)
abi_df = pl.read_parquet(events_abi_file_path)
decoded_df = gl.decode_df_with_abi_df(df=logs_df, abi_df=abi_df, decoder_type="log")
print(f"\nDecoded Logs using ABI DataFrame:\n{decoded_df.head()}\n\n")


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