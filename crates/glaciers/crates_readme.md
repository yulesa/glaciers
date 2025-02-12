# Glaciers

Glaciers is a Rust library for batch decoding Ethereum Virtual Machine (EVM) raw logs and traces. It matches raw log and traces entries with ABI event and function signatures, adding context — what each field or value represents and type casting.

## Installation

To use Glaciers as a Rust library, add it to your `Cargo.toml`:

```toml
[dependencies]
glaciers = "[Choose your version]"
```

## Usage

Glaciers divides the decoding process into two key steps:

1. **Generating an ABI Database**: Aggregates ABI files into a structured database that will be used for decoding.
2. **Decoding Raw Data**: Matches raw logs and traces against the ABI database to produce decoded tables.

### Example

```rust
use glaciers::decoder;
use glaciers::abi_reader;
use glaciers::DecoderType;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let abi_db_path = "path/to/abi_db.parquet";
    let abi_folder_path = "path/to/abi_folder";
    let log_folder_path = "path/to/logs";

    // Update ABI database
    abi_reader::update_abi_db(abi_db_path, abi_folder_path)?;
    
    // Decode logs
    decoder::decode_folder(log_folder_path, abi_db_path, DecoderType::Log).await?;
    
    Ok(())
}
```

## API Functions

### ABI Management
- `update_abi_db(abi_db_path, abi_folder_path)`: Aggregates ABI files into a database.
- `read_new_abi_folder(abi_folder_path)`: Reads ABI files from a folder.
- `read_new_abi_file(abi_file_path)`: Reads a single ABI file.
- `read_new_abi_json(abi, address)`: Parses a manually provided ABI JSON.

### Decoding Functions
- `decode_folder(log_folder_path, abi_db_path, decoder_type)`: Decodes all logs or traces in a folder.
- `decode_file(log_file_path, abi_db_path, decoder_type)`: Decodes a single log or trace file.
- `decode_df(logs_df, abi_db_path, decoder_type)`: Decodes a DataFrame containing raw logs or traces.
- `decode_df_with_abi_df(logs_df, abi_df, decoder_type)`: Decodes logs using a provided ABI DataFrame.

### Configuration
- `set_config_toml(config_file_path)`: Loads configuration from a TOML file.
- `set_config(config_key, config_value)`: Sets a specific configuration key.
- `get_config()`: Retrieves the current configuration.

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

## License

Glaciers is licensed under [MIT License](LICENSE).

## Project

Glaciers can also be used as a Python package. For more information on the project, visit the project repository [here](https://github.com/yulesa/glaciers).

