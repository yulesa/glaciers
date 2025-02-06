# Glaciers

We are looking for financial support to continue developing Glaciers. If you are interested or know of a potential grantor, please contact us. You can read a pitch post in ["Batch Decoding with Glaciers"](https://glaciers.substack.com/p/batch-decoding-with-glaciers).

Glaciers is a tool for batch decoding EVM (Ethereum Virtual Machine) raw logs and traces files, producing respective decoded tables. It matches raw log and traces entries with ABI event and function signatures, adding context — what each field or value represents and type casting.

You can read a 8 tweet thread with a quick example using Glaciers for analytics [here](https://x.com/yulesa/status/1879574555158831389) or go straight to the [file](./glacier_analytics_example/glacier_analytics_example.ipynb). We highly recommend users read the [Decoding Key Concepts](./docs/decoding_key_concepts.md) documentation to understand how Glaciers works.

To decode raw data, you’ll need an ABI database. This repository includes a small example, but you can also download a larger, specially curated [ABI database for Glaciers](https://github.com/yulesa/sourcify_abis), obtained from [Sourcify](https://sourcify.dev/). It contains 366,646 unique ABIs from 4,863,129 contracts across multiple chains. If you haven't already indexed your raw logs and traces, we recommend using [Cryo](https://github.com/paradigmxyz/cryo). Although Glaciers is generic, we used Cryo as schema sources.

To discuss Glaciers, contact us at [t.me/yuleandrade](http://t.me/yuleandrade)

## Quick Start
### Rust Installation
If you want to use glaciers as a CLI, you can install it using cargo:

```bash
# Clone the repository:
git clone https://github.com/yulesa/glaciers
# Navigate to the project CLI folder
cd glaciers
# Install the CLI
cargo install --path ./crates/cli
```
To uninstall, run:
```bash
cargo uninstall glaciers_cli
```

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
    abi_reader::update_abi_db(abi_db_path, abi_folder_path)?;
    decoder::decode_folder(log_folder_path, abi_db_path, DecoderType::Log).await?;
}
```
### Python Installation

Glaciers can also be installed as a Python package:

```bash
# Install Glaciers using pip
pip install glaciers
# Clone the repository if you want to use the example files and the provided ABI database:
git clone https://github.com/yulesa/glaciers
# Navigate to the project python module folder
cd glaciers/crates/python
# Run the end-to-end example
python e2e_example.py
```
For other installations, you can visit the [Installation](./docs/installation.md) guide.
## Usage

Glaciers divide the decoding process into two key steps:

- In the first step, users generate a table containing ABI Items. Glaciers provide functions to aggregate multiple ABI files in a folder, a single ABI, or even a manually inputted ABI. The resulting table can be stored either as a Parquet file or as a DataFrame, which can then be used in the next step for matching.

    Available functions:
    - `update_abi_db(abi_db_path, abi_folder_path)`
    - `read_new_abi_folder(abi_folder_path)`
    - `read_new_abi_file(abi_file_path)`
    - `read_new_abi_json(abi, address)`

- In the second step, raw data from function calls or events matches the ABI items created in Step 1. Glaciers employs two algorithms to match logs to ABI signatures:
    - `hash_address`: match logs/traces to ABI signatures using both the hash and address. Only contracts with ABI in the ABI DB will be matched.
    - `hash`: match logs/traces to ABI signatures by hash. For contracts without ABIs in the ABI DB, the most frequent signature in the ABI DB will be matched.

    After the join, each row is decoded using a User Defined Function (UDF), producing decoded columns that are added to the schema. Glaciers offers functions to decode multiple files in a folder, single files translated to dataframes.

    Available functions:
    - `decode_folder(log_folder_path, abi_db_path, decoder_type)`
    - `decode_file(log_file_path, abi_db_path, decoder_type)`
    - `decode_df(logs_df, abi_db_path, decoder_type)`
    - `decode_df_with_abi_df(logs_df, abi_df, decoder_type)`

- You can change the system configurations:

    Available functions:
    - `set_config_toml(config_file_path)`
    - `set_config(config_key, config_value)`
    - `get_config()`

- You also have a shortcut function to decode logs from a single contract (`decode_df_using_single_contract(log_df, contract_address, decoder_type)`). This function will download the ABI from Sourcify and decode the logs. Nevertheless, we recommend following the normal flow and creating the ABI DB first.

- There is also a helper function to unnest an unique event from a decoded logs' DataFrame: `unnest_event(decoded_logs_df, full_signature=None, event_name=None, event_address=None, topic0=None)`. It will only work if the full_signature is unique after filtering the logs_df using the optional arguments (full_signature, event_name, event_address, topic0). It's only available in Python.

### Examples

A small ABI database and a raw log file are provided as examples in the repo. If you want to use a larger ABI DB, you can download this [ABI DB from Sourcify](https://github.com/yulesa/sourcify_abis).

1. Add some new ABIs to the abi_database folder. New ABIs need to be json files and have the file name as a valid contract address.
2. Add raw logs files to the logs folder.
3. Run glaciers as a CLI:
```bash
glaciers abi -d ABIs/ethereum__events__abis.parquet -a ABIs/abi_database
glaciers decode-logs -l data/logs -a ABIs/ethereum__events__abis.parquet
glaciers decode-traces #use the paths in the configs
```
4. Instead, if you want install glaciers as a Python package, run the python e2e_example file.

## Schemas

Glaciers will repeat the same schema you have for your input files and add 2 sets of new columns:
The first set of columns belongs to the ABI DB, and are used to match the logs/traces to the ABI items:

    - ('full_signature', String):   event Transfer(address indexed from, address indexed to, uint256 value)
    - ('name', String):             Transfer
    - ('anonymous', Boolean):       False
    - ('num_indexed_args', int):    2
    - ('state_mutability', String): view
    - ('id', String):               '0xa9059cbb - function transfer(address to, uint256 amount) returns (bool) - 0xF19308F923582A6f7c465e5CE7a9Dc1BEC6665B1'

The second set of columns belongs to the decoded logs/traces:

    Decoded Logs Schema, and example:
    - ('event_values', String):     '[Address(0xeed...), Address(0x7a2...), Uint(3151936770479715624, 256)]'
    - ('event_keys', String):       '["from", "to", "value"]'
    - ('event_json', String):       '[{"name":"from","index":0,"value_type":"address","value":"0xeED..."}...]'

    Decoded Traces Schema, and example:
    - ('input_values', String):     '[Address(0x7a2...), Uint(3151936770479715624, 256)]'
    - ('input_keys', String):       '["to", "amount"]'
    - ('input_json', String):       '[{"name":"to","index":0,"value_type":"address","value":"0x7a2..."}...]'
    - ('output_values', String):    '[Bool(True)]'
    - ('output_keys', String):      '["success"]'
    - ('output_json', String):      '[{"name":"success","index":0,"value_type":"bool","value":"True"}]'

## Roadmap

Visit [Roadmap](./docs/roadmap.md) guide.

