# Glaciers

Glaciers is tool for batch decoding EVM (Ethereum Virtual Machine) raw logs and traces files (traces support coming soon), producing respective decoded tables. It matches raw log and traces entries with ABI event and function signatures, adding context — what each field or value represents and type casting.

## Features
- Decode Ethereum event logs using ABI definitions
- Process logs in parallel using Polars DataFrames
- Python bindings for easy integration

## Rust Installation
Currently, Glaciers requires manual compilation and execution. Methods requires having rust installed. See [rustup](https://rustup.rs/) for instructions. To get started:

1. Clone the repository:
```bash
git clone https://github.com/yulesa/glaciers
# Navigate to the project folder
cd glaciers
```
2. Compile and run the project:

```bash
cargo run
```



## Python Installation

Glaciers can also be installed as a python package:

### From pypi (Recommended)

1. Clone the repository for example files:
```bash
git clone https://github.com/yulesa/glaciers
# Navigate to the project python module folder
cd glaciers/crates/python
```
2. Install Glaciers using pip
```bash
pip install glaciers
```
3. Run the end-to-end example
```bash
python e2e_example.py
```

### Using uv, from source
This method requires having uv (Python package and project manager) installed. See [uv](https://docs.astral.sh/uv/getting-started/installation/) for instructions.

1. Clone the repository:
```bash
git clone https://github.com/yulesa/glaciers
# Navigate to the project python module folder
cd glaciers/crates/python
```
2. Install dependencies in .venv
```bash
uv sync
```
3.  Build the glaciers python module and install the package

```bash
uv run maturin develop --uv

```
4.  Run the end-to-end example

```bash
uv run python e2e_example.py
```

### Using pip, from source


1. Install [maturin](https://www.maturin.rs/), a tool to embed Rust libraries into a Python module. 
```bash
pip install maturin
```
2. Clone the repository:
```bash
git clone https://github.com/yulesa/glaciers
# Navigate to the project python module folder
cd glaciers/crates/python
```
3.  Build the Rust python module and install the package

```bash
maturin develop
```
4.  Run the end-to-end example

```bash
python e2e_example.py
```


## Usage examples

Some initial ABIs, a topic0 database, and a raw log file are provided as examples.

1. Add new ABIs to the abi_database folder. New ABIs need to be json files and have the file name as a valid contract address.
2. Add raw logs parquet files to the logs folder. File name needs to have 'logs'. Logs schemas need to have: topic0, topic1, topic2, topic3, data.
3. Compile and Run

## Schemas

Glaciers will repeat the same schema you have for your input files.
Input files must contain:

    - ('topic0', Binary)
    - ('topic1', Binary)
    - ('topic2', Binary)
    - ('topic3', Binary)
    - ('data', Binary)

The following columns will be added to your original table:

    Decoded Log Schema:
    - ('full_signature', String): event Transfer(address indexed from, address indexed to, uint256 value)
    - ('name', String): Transfer
    - ('anonymous', Boolean): False
    - ('event_values', String): '[Address(0xeed...), Address(0x7a2...), Uint(3151936770479715624, 256)]'
    - ('event_keys', String): '["from", "to", "value"]'
    - ('event_json', String): [{"name":"from","index":0,"value_type":"address","value":"0xeEDfF72A683058F8FF531e8c98575f920430FdC5"}...]

## How It Works
Glaciers currently only support decoding EVM event logs and parquet files. It includes the following functions:

**read_abis_topic0:** 

This function scans an ABI database and generates a list of event signatures in an abis_topic0.parquet file. The schema is optimized for event decoding but could be extended to auxiliary tables for other purposes.

*Note: In the future CLI implementation, this function may become an optional step.*

**decode_log_files:** 

This function performs the following steps:

1. Input Handling: Reads through multiple raw log files in a folder. [Cryo](https://github.com/paradigmxyz/cryo) schema was used as a reference. Each file is processed by a separate thread.

2. Event Matching: Matches raw log entries with event signatures stored in `abis_topic0.parquet` using a Polars. 

3. Chunking and Parallel Decoding: Files are split into smaller chunks, spawning threads for each, enabling efficient decoding of large logs data frames. Decoding is performed using a Polars UDF (User Defined Function).

4. Output: Chunks' results are collected and combined into a single decoded output file for each raw log file and saved in a decoded folder.

*Notes:*
- Event Matching: For EVM events, this is a simple Polars join, but for EVM functions, this will encompass an algorithm to find the best match.
- A future CLI will be used to configure each step in the process. Some variables are already highlighted as constants.
- Different file types and schemas to come in future versions.

