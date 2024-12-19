import os
import asyncio
from os.path import dirname
import polars as pl
import glaciers as gl

TOPIC0_FILE_PATH = "ABIs/ethereum__abis_topic0.parquet"
ABIS_FOLDER_PATH = "ABIs/abi_database"
LOGS_FOLDER_PATH = "data/logs"
LOGS_FILE_NAME = "ethereum__logs__blocks__18426253_to_18426303_example.parquet"


python_dir = os.path.dirname(os.path.abspath(__file__))
project_dir =  dirname(dirname(python_dir))+"/"
print(f"Project dir: {project_dir}")

topic0_file_path = project_dir+TOPIC0_FILE_PATH
abis_folder_path = project_dir+ABIS_FOLDER_PATH
logs_folder_path = project_dir+LOGS_FOLDER_PATH

logs_df = pl.read_parquet(f"{logs_folder_path}/{LOGS_FILE_NAME}")
print(f"Raw logs file: \n{logs_df.head()}")




######## Test read_abis_topic0 ########
# Reads ABIs in a folder and append to the topic0 parquet file
#
# This function loads each ABI definitions from a ABI folder and append the new topic0 and event signatures
# into an existing topic0 (parquet file) the unique entries. Function also output the dataframe.
#
# # Arguments
# - `topic0_path`: Path to the parquet file containing the existing DataFrame.
# - `abi_folder_path`: Path to the folder containing ABI JSON files
#
# # Returns
# A `PyResult` containing a `PyDataFrame` with all unique topic0 and event signatures
#
# # Errors
# Returns a `PyValueError` if there are issues reading or processing the ABIs
abis_df = gl.read_abis_topic0(topic0_file_path, abis_folder_path)
print(f"\nFirst 5 rows of ABIs DataFrame:\n{abis_df.head()}")


######## Test decode_log_files ########
# Decode a folder of logs in parquet format.
#
# This function takes a logs folder path and a topic0 parquet file path. It iterate through 
# logs files, decode, and save them into decoded logs' parquet files.
#
# # Arguments
# - `log_folder_path`: Path to a folder containing the logs parquet files
# - `topic0_path`: Path to the topic0 file containing the topic0 and event signatures
#
# # Returns
# No Return
#
# # Errors
# Returns a `PyValueError` if there are issues processing the logs
gl.decode_log_files(logs_folder_path, topic0_file_path)
print(f"\n Decoded logs saved in the decoded folder.")


######## Test decode_log_files ########
# Decode a logs' DataFrame
#
# This function takes a raw logs' DataFrame and a topic0 parquet file path and decode the df
# to a decoded logs' DataFrame.
#
# # Arguments
# - `logs_df`: A DataFrame containing raw blockchain logs
# - `topic0_path`: Path to the topic0 file containing the topic0 and event signatures
#
# # Returns
# A `PyResult` containing a decoded logs' `PyDataFrame` or an error
#
# # Errors
# Returns a `PyValueError` if there are issues processing the logs
logs_df = pl.read_parquet(f"{logs_folder_path}/{LOGS_FILE_NAME}")
decoded_df = gl.decode_log_df(logs_df, topic0_file_path)
print(f"\nDecoded Logs DataFrame:\n{decoded_df.head()}")


######## Test polars_decode_logs ########
# Decode dataframe event logs using a minimal ABI definitions dataframe
#
# Args:
#     logs_df: A polars DataFrame containing the raw logs with topic0, topic1, topic2, topic3, and data columns
#     abi_df: A polars DataFrame containing:
#         - topic0: The topic0 (event signature hash) as bytes
#         - full_signature: The full event signature as string (e.g. "Transfer(address indexed from, address indexed to, uint256 value)")
#
# Returns:
#     A polars DataFrame containing the decoded events with additional columns:
#     - event_values: The decoded parameter values
#     - event_keys: The parameter names
#     - event_json: JSON representation of the decoded event
# Transfer event
TRANSFER_EVENT = "Transfer(address indexed from, address indexed to, uint256 value)"
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
# Create ABI DataFrame
abi_df = pl.DataFrame(
    {
        "topic0": TRANSFER_TOPIC,
        "full_signature": TRANSFER_EVENT,
    }
)
# Decode the events
decoded_df = gl.polars_decode_logs(logs_df, abi_df)
print(f"\nDecoded Logs DataFrame:\n{decoded_df.head(5)}")