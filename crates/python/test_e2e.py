import os
import asyncio
from os.path import dirname
import polars as pl
import glaciers as gl

print(f"Glacier functions: {dir(gl)}")

TOPIC0_FILE_PATH = "ABIs/ethereum__abis_topic0.parquet"
ABIS_FOLDER_PATH = "ABIs/abi_database"
LOGS_FOLDER_PATH = "data/logs"
LOGS_FILE_NAME = "ethereum__logs__blocks__18426253_to_18426303_example.parquet"


python_dir = os.path.dirname(os.path.abspath(__file__))
project_dir =  dirname(dirname(python_dir))+"/"
print(f"Project dir: {project_dir}")
logs_df = pl.read_parquet(f"{project_dir}{LOGS_FOLDER_PATH}/{LOGS_FILE_NAME}")
print(f"Raw logs file: \n{logs_df.head()}")




######## Test read_abis_topic0 ########
abis_df = gl.read_abis_topic0(project_dir+TOPIC0_FILE_PATH, project_dir+ABIS_FOLDER_PATH)
print(f"\nFirst 5 rows of ABIs DataFrame:\n{abis_df.head()}")


######## Test decode_log_files ########
gl.decode_log_files(project_dir+LOGS_FOLDER_PATH, project_dir+TOPIC0_FILE_PATH)


######## Test decode_log_files ########
logs_df = pl.read_parquet(f"{project_dir}{LOGS_FOLDER_PATH}/{LOGS_FILE_NAME}")
decoded_df = gl.decode_log_df(logs_df, project_dir+TOPIC0_FILE_PATH)
print(f"\nDecoded Logs DataFrame:\n{decoded_df.head()}")

######## Test polars_decode_logs ########
# Transfer event
TRANSFER_EVENT = "Transfer(address indexed from, address indexed to, uint256 value)"
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
# Create ABI DataFrame
abi_df = pl.DataFrame(
    {
        "topic0": [
            TRANSFER_TOPIC
        ],
        "full_signature": [TRANSFER_EVENT],
    }
)
# Decode the events
decoded_df = gl.polars_decode_logs(logs_df, abi_df)
print(f"\nDecoded Logs DataFrame:\n{decoded_df.head(5)}")