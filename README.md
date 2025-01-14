# Glaciers

We are looking for financial support to continue developing Glaciers. If you are interested or know of a potential grantor, please contact us. You can read a pitch post in ["Batch Decoding with Glaciers"](https://glaciers.substack.com/p/batch-decoding-with-glaciers).

Glaciers is a tool for batch decoding EVM (Ethereum Virtual Machine) raw logs and traces files (traces support coming soon), producing respective decoded tables. It matches raw log and traces entries with ABI event and function signatures, adding context — what each field or value represents and type casting.

We highly recommend users read the [Decoding Key Concepts](./docs/decoding_key_concepts.md) documentation to understand how Glaciers works.

If you haven't already indexed your raw logs and traces, we recommend using [Cryo](https://github.com/paradigmxyz/cryo). Although Glaciers is generic, we used Cryo as schema sources.

To discuss Glaciers, contact us at [t.me/yuleandrade](http://t.me/yuleandrade)


## Features
- Batch decodes EVM event logs files from multiple contracts using an ABI database.
- Batch decode Polars DataFrames in Python or Rust.
- Decode multiple logs from a single topic0 DataFrame in Python or Rust.

## Quick Start
### Rust Installation
Library Mode
```toml
# Cargo.toml
[dependencies]
glaciers = "[Choose your version]"
```
```rust
use glaciers::decoder;
use glaciers::abi_reader;

async fn main() -> {
    abi_reader::read_abis_topic0(TOPIC0_FILE_PATH, ABIS_FOLDER_PATH);
    decoder::process_log_files(RAW_LOGS_FOLDER_PATH, TOPIC0_FILE_PATH).await;
}
```
### Python Installation

Glaciers can also be installed as a Python package:

```bash
# Clone the repository for example files:
git clone https://github.com/yulesa/glaciers
# Navigate to the project python module folder
cd glaciers/crates/python
# Install Glaciers using pip
pip install glaciers
# Run the end-to-end example
python e2e_example.py
```
For other installations, you can visit the [Installation](./docs/installation.md) guide.
## Usage

Glaciers divide the decoding process into two key steps:

- In the first step, users generate a table containing ABI Items. Glaciers provide functions to aggregate multiple ABI files in a folder, a single ABI, or even a manually inputted ABI. The resulting table can be stored either as a Parquet file or as a DataFrame, which can then be used in the next step for matching.

    Available functions:
    - `update_abi_df(abi_df_path, abi_folder_path)`
    - `read_new_abi_folder(abi_folder_path)`
    - `read_new_abi_file(abi_file_path)`
    - `read_new_abi_json(abi, address)`

- In the second step, raw data from function calls or events matches the ABI items created in Step 1. Glaciers employ a simple LEFT JOIN using the hash as the key to associate the raw log data with the corresponding ABI information. After the join, each row is decoded using a User Defined Function (UDF), producing decoded columns that are added to the schema. Glaciers offers functions to decode multiple files in a folder, single files translated to dataframes.

    Available functions:
    - `decode_log_folder(log_folder_path, abi_df_path)`
    - `decode_log_file(log_file_path, abi_df_path)`
    - `decode_log_df(logs_df, abi_df_path)`
    - `decode_log_df_with_abi_df(logs_df, abi_df)`
    - `polars_decode_logs(logs_df, abi_df)`

- You can change the system configurations:

    Available functions:
    - `set_config_toml(config_file_path)`
    - `set_config(config_key, config_value)`
    - `get_config()`

### Examples

Some initial ABIs, an abi_df database (only events), and a raw log file are provided as examples.

1. Add new ABIs to the abi_database folder. New ABIs need to be json files and have the file name as a valid contract address.
2. Add raw logs parquet files to the logs folder. File name needs to have 'logs'. Logs schemas need to have: topic0, topic1, topic2, topic3, data.
3. Compile and Run using rust, or run the python e2e_example file.

## Schemas

Glaciers will repeat the same schema you have for your input files.
Input files must contain:

    - ('topic0', Binary)
    - ('topic1', Binary)
    - ('topic2', Binary)
    - ('topic3', Binary)
    - ('data', Binary)

The following columns will be added to your original table:

    Decoded Log Schema, and example:
    - ('full_signature', String): event Transfer(address indexed from, address indexed to, uint256 value)
    - ('name', String): Transfer
    - ('anonymous', Boolean): False
    - ('event_values', String): '[Address(0xeed...), Address(0x7a2...), Uint(3151936770479715624, 256)]'
    - ('event_keys', String): '["from", "to", "value"]'
    - ('event_json', String): [{"name":"from","index":0,"value_type":"address","value":"0xeEDfF72A683058F8FF531e8c98575f920430FdC5"}...]

## Roadmap

Visit [Roadmap](./docs/roadmap.md) guide.

