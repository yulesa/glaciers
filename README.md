# Glaciers

Glaciers is tool for batch decoding raw logs and traces files (traces support coming soon), producing respective decoded tables.It matches raw log and traces entries with ABI event and function signatures, adding context — what each field or value represents and type casting.

## Installation
Currently, Glaciers requires manual compilation and execution. To get started:

1. Clone the repository:
```
git clone https://github.com/yulesa/glaciers
cd glaciers
```
2. Compile and run the project:

```
cargo run
```

## Usage examples

Some initial ABIs, a topic0 database, and a raw log file  are provided as examples.

1. Add new ABIs to the abi_database folder. New ABIs need to be json files and have the file name as a valid contract address.

2. Create a data folder structure in the project folder. If you want to use different names or existing folders, change the const in the Rust files.
```
├── data

│   ├── logs

│   ├── decoded

```
3. Add logs parquet files to the logs folder.  Logs schemas need to have: topic0, topic1, topic2, topic3, data.
4. Manually set the const in the rust files: ABIS_FOLDER_PATH, DECODED_FOLDER_PATH, MAX_CONCURRENT_FILES_DECODING, MAX_CHUNK_THREADS_PER_FILE, DECODED_CHUCK_SIZE
5. Compile and Run


## How It Works
Glaciers currently only support decoding event logs and using parquet files. It includes the following functions:

**read_abis_topic0:** 

This function scans an ABI database and generates a list of event signatures in an abis_topic0.parquet file. The schema is optimized for event decoding but could be extended to auxiliary tables for other purposes.
*Note: In the future CLI implementation, this function may become an optional step.*

**process_log_files:** 

This function performs the following steps:

1. Input Handling: Reads through multiple raw log files in a folder. [Cryo](https://github.com/paradigmxyz/cryo) schema was used as a reference. Each file is processed by a separate thread.

2. Event Matching: Matches raw log entries with event signatures stored in abis_topic0.parquet using a Polars join. For function matching, an advanced algorithm will be implemented in the future.

3. Chunking and Parallel Decoding: Files are split into smaller chunks, spawning threads for each, enabling efficient decoding of large logs data frames. Decoding is performed using a Polars UDF (User Defined Function).

4. Output: Chunks' results are collected and combined into a single decoded output file for each raw log file and saved in a decoded folder.

*Notes:*
- Event Matching: For events, this is a simple Polars join, but for functions, this will encompass an algorithm to find the best match.
- A future CLI will be used to configure each step in the process. Some variables are already highlighted as constants.
- Different file types and schemas to come in future versions.

