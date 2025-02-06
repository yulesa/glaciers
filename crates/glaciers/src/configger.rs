use std::sync::{LazyLock, RwLock};
use std::fs;
use serde::{Deserialize, Serialize};
use pyo3::FromPyObject;
use thiserror::Error;
#[derive(Error, Debug)]
pub enum ConfiggerError {
    #[error("Error while setting GLACIERS_CONFIG, could not read Toml file, IO error: {0}")]
    IOError(#[from] std::io::Error),
    #[error("Error while setting GLACIERS_CONFIG, could not parse Toml file, parse error: {0}")]
    ParseError(#[from] toml::de::Error),
    #[error("Error while setting GLACIERS_CONFIG, invalid Toml format")]
    InvalidTomlFormat,
    #[error("Error while setting GLACIERS_CONFIG, unsupported value type for config field {0}")]
    UnsupportedValueType(String),
    #[error("Error while setting GLACIERS_CONFIG, invalid config field or value type for field {0}")]
    InvalidFieldOrValue(String),
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct Config {
    pub glaciers: GlaciersConfig,
    pub main: MainConfig,
    pub abi_reader: AbiReaderConfig,
    pub decoder: DecoderConfig,
    pub log_decoder: LogDecoderConfig,
    pub trace_decoder: TraceDecoderConfig,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct GlaciersConfig {
    pub prefered_dataframe_type: PreferedDataframeType,
    pub unnesting_hex_string_encoding: bool,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub enum PreferedDataframeType {
    Polars,
    Pandas
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct MainConfig {
    pub events_abi_db_file_path: String,
    pub functions_abi_db_file_path: String,
    pub abi_folder_path: String,
    pub raw_logs_folder_path: String,
    pub raw_traces_folder_path: String,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct AbiReaderConfig {
    pub unique_key: Vec<String>,
    pub output_hex_string_encoding: bool,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct DecoderConfig {
    pub algorithm: DecoderAlgorithm,
    pub output_hex_string_encoding: bool,
    pub output_file_format: String,
    pub max_concurrent_files_decoding: usize,
    pub max_chunk_threads_per_file: usize,
    pub decoded_chunk_size: usize,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub enum DecoderAlgorithm {
    HashAddress,
    Hash
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct LogDecoderConfig {
    pub log_schema: LogSchemaConfig,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct LogSchemaConfig {
    pub log_alias: LogAliasConfig,
    pub log_datatype: LogDatatypeConfig,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct LogAliasConfig {
    pub topic0: String,
    pub topic1: String,
    pub topic2: String,
    pub topic3: String,
    pub data: String,
    pub address: String,
}

impl LogAliasConfig {
    pub fn as_array(&self) -> Vec<String> {
        // excluding the address column because it is not used in the log decoding
        vec![self.topic0.clone(), self.topic1.clone(), self.topic2.clone(), self.topic3.clone(), self.data.clone()]
    }
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct LogDatatypeConfig {
    pub topic0: DataType,
    pub topic1: DataType,
    pub topic2: DataType,
    pub topic3: DataType,
    pub data: DataType,
    pub address: DataType,
}

impl LogDatatypeConfig {
    pub fn as_array(&self) -> Vec<DataType> {
        vec![self.topic0.clone(), self.topic1.clone(), self.topic2.clone(), self.topic3.clone(), self.data.clone(), self.address.clone()]
    }
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct TraceDecoderConfig {
    pub trace_schema: TraceSchemaConfig,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct TraceSchemaConfig {
    pub trace_alias: TraceAliasConfig,
    pub trace_datatype: TraceDatatypeConfig,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct TraceAliasConfig {
    pub selector: String,
    pub action_input: String,
    pub result_output: String,
    pub action_to: String,
}

impl TraceAliasConfig {
    pub fn as_array(&self) -> Vec<String> {
        // excluding the selector and address column because it is not used in the trace decoding
        vec![self.action_input.clone(), self.result_output.clone()]
    }
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct TraceDatatypeConfig {
    pub selector: DataType,
    pub action_input: DataType,
    pub result_output: DataType,
    pub action_to: DataType,
}

impl TraceDatatypeConfig {
    pub fn as_array(&self) -> Vec<DataType> {
        vec![self.selector.clone(), self.action_input.clone(), self.result_output.clone(), self.action_to.clone()]
    }
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub enum DataType {
    Binary,
    HexString
}

pub static GLACIERS_CONFIG: LazyLock<RwLock<Config>> = LazyLock::new(|| {
    RwLock::new(Config {
        glaciers: GlaciersConfig {
            prefered_dataframe_type: PreferedDataframeType::Polars,
            unnesting_hex_string_encoding: false,
        },
        main: MainConfig {
            events_abi_db_file_path: String::from("ABIs/ethereum__events__abis.parquet"),
            functions_abi_db_file_path: String::from("ABIs/ethereum__functions__abis.parquet"),
            abi_folder_path: String::from("ABIs/abi_database"),
            raw_logs_folder_path: String::from("data/logs"),
            raw_traces_folder_path: String::from("data/traces"),
        },
        abi_reader: AbiReaderConfig {
            output_hex_string_encoding: false,
            unique_key: vec![String::from("hash"), String::from("full_signature"), String::from("address")],
        },
        decoder: DecoderConfig {
            algorithm: DecoderAlgorithm::Hash,
            output_hex_string_encoding: false,
            output_file_format: String::from("parquet"),
            max_concurrent_files_decoding: 16,
            max_chunk_threads_per_file: 16,
            decoded_chunk_size: 500_000,
        },
        log_decoder: LogDecoderConfig {
            log_schema: LogSchemaConfig {
                log_alias: LogAliasConfig {
                    topic0: String::from("topic0"),
                    topic1: String::from("topic1"),
                    topic2: String::from("topic2"),
                    topic3: String::from("topic3"),
                    data: String::from("data"),
                    address: String::from("address"),
                },
                log_datatype: LogDatatypeConfig {
                    topic0: DataType::Binary,
                    topic1: DataType::Binary,
                    topic2: DataType::Binary,
                    topic3: DataType::Binary,
                    data: DataType::Binary,
                    address: DataType::Binary,
                }
            },
        },
        trace_decoder: TraceDecoderConfig {
            trace_schema: TraceSchemaConfig {
                trace_alias: TraceAliasConfig {
                    selector: String::from("selector"),
                    action_input: String::from("action_input"),
                    result_output: String::from("result_output"),
                    action_to: String::from("action_to"),
                },
                trace_datatype: TraceDatatypeConfig {
                    selector: DataType::Binary,
                    action_input: DataType::Binary,
                    result_output: DataType::Binary,
                    action_to: DataType::Binary,
                }
            },
        },
    })
});

#[derive(Clone, FromPyObject, Debug)]
pub enum ConfigValue {
    String(String),
    Number(usize),
    List(Vec<String>),
    Boolean(bool)
}

impl From<&str> for ConfigValue {
    fn from(s: &str) -> Self {
        ConfigValue::String(s.to_string())
    }
}

impl From<usize> for ConfigValue {
    fn from(n: usize) -> Self {
        ConfigValue::Number(n)
    }
}

impl From<Vec<String>> for ConfigValue {
    fn from(v: Vec<String>) -> Self {
        ConfigValue::List(v)
    }
}

impl From<bool> for ConfigValue {
    fn from(b: bool) -> Self {
        ConfigValue::Boolean(b)
    }
}

pub fn get_config() -> Config {
    GLACIERS_CONFIG.read().unwrap().clone()
}

pub fn set_config(config_path: &str, value: impl Into<ConfigValue>) -> Result<(), ConfiggerError> {
    let mut config = GLACIERS_CONFIG.write().unwrap();
    let value = value.into();
    let section = config_path.split(".").nth(0).ok_or(ConfiggerError::InvalidFieldOrValue(format!("Section missing in field: {}", config_path.to_string())))?;
    let field = config_path.split(".").nth(1);
    let subfield = config_path.split(".").nth(2);
    let schema_field = config_path.split(".").nth(3);

    match section {
        "glaciers" => match (field, value) {
            (Some("prefered_dataframe_type"), ConfigValue::String(v)) => {
                match v.to_lowercase().as_str() {
                    "polars" => config.glaciers.prefered_dataframe_type = PreferedDataframeType::Polars,
                    "pandas" => config.glaciers.prefered_dataframe_type = PreferedDataframeType::Pandas,
                    _ => return Err(ConfiggerError::InvalidFieldOrValue(field.unwrap_or("").to_string()))
                }
            },
            (Some("unnesting_hex_string_encoding"), ConfigValue::Boolean(v)) => config.glaciers.unnesting_hex_string_encoding = v,
            _ => return Err(ConfiggerError::InvalidFieldOrValue(field.unwrap_or("").to_string()))
        },
        "main" => match (field, value) {
            (Some("events_abi_db_file_path"), ConfigValue::String(v)) => config.main.events_abi_db_file_path = v,
            (Some("functions_abi_db_file_path"), ConfigValue::String(v)) => config.main.functions_abi_db_file_path = v,
            (Some("abi_folder_path"), ConfigValue::String(v)) => config.main.abi_folder_path = v,
            (Some("raw_logs_folder_path"), ConfigValue::String(v)) => config.main.raw_logs_folder_path = v,
            (Some("raw_traces_folder_path"), ConfigValue::String(v)) => config.main.raw_traces_folder_path = v,
            _ => return Err(ConfiggerError::InvalidFieldOrValue(field.unwrap_or("").to_string()))
        },

        "abi_reader" => match (field, value) {
            (Some("output_hex_string_encoding"), ConfigValue::Boolean(v)) => config.abi_reader.output_hex_string_encoding = v,
            (Some("output_hex_string_encoding"), ConfigValue::Number(v)) => {
                match v {
                    1 => config.abi_reader.output_hex_string_encoding = true,
                    0 => config.abi_reader.output_hex_string_encoding = false,
                    _ => return Err(ConfiggerError::InvalidFieldOrValue(field.unwrap_or("").to_string()))
                }
            },
            (Some("unique_key"), ConfigValue::List(v)) => {
                let v = v.iter().map(|s| s.to_lowercase()).collect();
                validate_unique_key(&v)?;
                config.abi_reader.unique_key = v;
            },
            (Some("unique_key"), ConfigValue::String(v)) => {
                let v = vec![v.to_lowercase()];
                validate_unique_key(&v)?;
                config.abi_reader.unique_key = v;
            },
            _ => return Err(ConfiggerError::InvalidFieldOrValue(field.unwrap_or("").to_string()))
        },
        
        "decoder" => match (field, value) {
            (Some("algorithm"), ConfigValue::String(v)) => {
                match v.to_lowercase().as_str() {
                    "hash_address" => config.decoder.algorithm = DecoderAlgorithm::HashAddress,
                    "hash" => config.decoder.algorithm = DecoderAlgorithm::Hash,
                    _ => return Err(ConfiggerError::InvalidFieldOrValue(field.unwrap_or("").to_string()))
                }
            },
            (Some("output_hex_string_encoding"), ConfigValue::Boolean(v)) => config.decoder.output_hex_string_encoding = v,
            (Some("output_hex_string_encoding"), ConfigValue::Number(v)) => {
                match v {
                    1 => config.abi_reader.output_hex_string_encoding = true,
                    0 => config.abi_reader.output_hex_string_encoding = false,
                    _ => return Err(ConfiggerError::InvalidFieldOrValue(field.unwrap_or("").to_string()))
                }
            },
            (Some("output_file_format"), ConfigValue::String(v)) => {
                let v = v.to_lowercase();
                validate_output_file_format(&v)?;
                config.decoder.output_file_format = v;
            },
            (Some("max_concurrent_files_decoding"), ConfigValue::Number(v)) => config.decoder.max_concurrent_files_decoding = v,
            (Some("max_chunk_threads_per_file"), ConfigValue::Number(v)) => config.decoder.max_chunk_threads_per_file = v,
            (Some("decoded_chunk_size"), ConfigValue::Number(v)) => config.decoder.decoded_chunk_size = v,
            _ => return Err(ConfiggerError::InvalidFieldOrValue(field.unwrap_or("").to_string()))
        },
        
        "log_decoder" => match (field, value) {
            (Some("schema"), value) => match (subfield, value) {
                (Some("alias"), ConfigValue::String(v)) => {
                    match schema_field {
                        Some("topic0") => config.log_decoder.log_schema.log_alias.topic0 = v,
                        Some("topic1") => config.log_decoder.log_schema.log_alias.topic1 = v,
                        Some("topic2") => config.log_decoder.log_schema.log_alias.topic2 = v,
                        Some("topic3") => config.log_decoder.log_schema.log_alias.topic3 = v,
                        Some("data") => config.log_decoder.log_schema.log_alias.data = v,
                        Some("address") => config.log_decoder.log_schema.log_alias.address = v,
                        _ => return Err(ConfiggerError::InvalidFieldOrValue(schema_field.unwrap_or("").to_string()))
                    }
                },
                (Some("datatype"), ConfigValue::String(v)) => {
                    match schema_field {
                        Some("topic0") => config.log_decoder.log_schema.log_datatype.topic0 = match v.to_lowercase().as_str() {
                            "binary" => DataType::Binary,
                            "hexstring" => DataType::HexString,
                            _ => return Err(ConfiggerError::InvalidFieldOrValue("Invalid datatype".to_string()))
                        },
                        Some("topic1") => config.log_decoder.log_schema.log_datatype.topic1 = match v.to_lowercase().as_str() {
                            "binary" => DataType::Binary,
                            "hexstring" => DataType::HexString,
                            _ => return Err(ConfiggerError::InvalidFieldOrValue("Invalid datatype".to_string()))
                        },
                        Some("topic2") => config.log_decoder.log_schema.log_datatype.topic2 = match v.to_lowercase().as_str() {
                            "binary" => DataType::Binary,
                            "hexstring" => DataType::HexString,
                            _ => return Err(ConfiggerError::InvalidFieldOrValue("Invalid datatype".to_string()))
                        },
                        Some("topic3") => config.log_decoder.log_schema.log_datatype.topic3 = match v.to_lowercase().as_str() {
                            "binary" => DataType::Binary,
                            "hexstring" => DataType::HexString,
                            _ => return Err(ConfiggerError::InvalidFieldOrValue("Invalid datatype".to_string()))
                        },
                        Some("data") => config.log_decoder.log_schema.log_datatype.data = match v.to_lowercase().as_str() {
                            "binary" => DataType::Binary,
                            "hexstring" => DataType::HexString,
                            _ => return Err(ConfiggerError::InvalidFieldOrValue("Invalid datatype".to_string()))
                        },
                        Some("address") => config.log_decoder.log_schema.log_datatype.address = match v.to_lowercase().as_str() {
                            "binary" => DataType::Binary,
                            "hexstring" => DataType::HexString,
                            _ => return Err(ConfiggerError::InvalidFieldOrValue("Invalid datatype".to_string()))
                        },
                        _ => return Err(ConfiggerError::InvalidFieldOrValue(schema_field.unwrap_or("").to_string()))
                    }
                },
                _ => return Err(ConfiggerError::InvalidFieldOrValue(subfield.unwrap_or("").to_string()))
            },
            _ => return Err(ConfiggerError::InvalidFieldOrValue(field.unwrap_or("").to_string()))
        },
        
        "trace_decoder" => match (field, value) {
            (Some("schema"), value) => match (subfield, value) {
                (Some("alias"), ConfigValue::String(v)) => {
                    match schema_field {
                        Some("selector") => config.trace_decoder.trace_schema.trace_alias.selector = v,
                        Some("action_input") => config.trace_decoder.trace_schema.trace_alias.action_input = v,
                        Some("result_output") => config.trace_decoder.trace_schema.trace_alias.result_output = v,
                        Some("action_to") => config.trace_decoder.trace_schema.trace_alias.action_to = v,
                        _ => return Err(ConfiggerError::InvalidFieldOrValue(schema_field.unwrap_or("").to_string()))
                    }
                },
                (Some("datatype"), ConfigValue::String(v)) => {
                    match schema_field {
                        Some("selector") => config.trace_decoder.trace_schema.trace_datatype.selector = match v.to_lowercase().as_str() {
                            "binary" => DataType::Binary,
                            "hexstring" => DataType::HexString,
                            _ => return Err(ConfiggerError::InvalidFieldOrValue("Invalid datatype".to_string()))
                        },
                        Some("action_input") => config.trace_decoder.trace_schema.trace_datatype.action_input = match v.to_lowercase().as_str() {
                            "binary" => DataType::Binary,
                            "hexstring" => DataType::HexString,
                            _ => return Err(ConfiggerError::InvalidFieldOrValue("Invalid datatype".to_string()))
                        },
                        Some("result_output") => config.trace_decoder.trace_schema.trace_datatype.result_output = match v.to_lowercase().as_str() {
                            "binary" => DataType::Binary,
                            "hexstring" => DataType::HexString,
                            _ => return Err(ConfiggerError::InvalidFieldOrValue("Invalid datatype".to_string()))
                        },
                        Some("action_to") => config.trace_decoder.trace_schema.trace_datatype.action_to = match v.to_lowercase().as_str() {
                            "binary" => DataType::Binary,
                            "hexstring" => DataType::HexString,
                            _ => return Err(ConfiggerError::InvalidFieldOrValue("Invalid datatype".to_string()))
                        },
                        _ => return Err(ConfiggerError::InvalidFieldOrValue(schema_field.unwrap_or("").to_string()))
                    }
                },
                _ => return Err(ConfiggerError::InvalidFieldOrValue(subfield.unwrap_or("").to_string()))
            },
            _ => return Err(ConfiggerError::InvalidFieldOrValue(field.unwrap_or("").to_string()))
        },
        _ => return Err(ConfiggerError::InvalidFieldOrValue(section.to_string()))
    }

    Ok(())
}

/// Loads and processes a TOML configuration file, setting all valid configurations
pub fn set_config_toml(file_path: &str) -> Result<(), ConfiggerError> {
    // Read and parse TOML file into toml::Value
    let config: toml::Value = fs::read_to_string(file_path)
        .map_err(ConfiggerError::IOError)
        .and_then(|content| toml::from_str(&content)
        .map_err(ConfiggerError::ParseError))?;
    
    // Extract root table or return error if invalid format
    let table = config.as_table()
        .ok_or(ConfiggerError::InvalidTomlFormat)?;
    
    // Process table and set each config key-value pair
    let config_pairs = process_table("", table)?;
    for (key, value) in config_pairs {
        set_config(&key, value)?;
    }
    Ok(())
 }
 
 fn process_table(prefix: &str, table: &toml::Table) -> Result<Vec<(String, ConfigValue)>, ConfiggerError> {
    let mut config_pairs = Vec::new();
    
    for (key, value) in table {
        // Build full key path with prefix for nested tables
        let full_key = if prefix.is_empty() {
            key.to_string()
        } else {
            format!("{}.{}", prefix, key)
        };
        
         // if the value is a table, process it recursively without adding it to the config_pairs, otherwise add it to the config_pairs
        match value {
            toml::Value::Table(nested) => config_pairs.extend(process_table(&full_key, nested)?),
            
            // Handle string values, checking for hex prefix
            toml::Value::String(s) => config_pairs.push((full_key, ConfigValue::String(s.clone()))),
            // Convert integer to usize 
            toml::Value::Integer(n) => config_pairs.push((full_key, ConfigValue::Number(*n as usize))),
            // Convert array to Vec<String>, ensuring all elements are strings
            toml::Value::Array(arr) => {
                let string_vec: Result<Vec<String>, _> = arr.iter()
                    .map(|v| v.as_str()
                        .ok_or_else(|| ConfiggerError::UnsupportedValueType(full_key.clone()))
                        .map(String::from))
                    .collect();
                config_pairs.push((full_key.clone(), ConfigValue::List(string_vec?)));
            },
            toml::Value::Boolean(b) => config_pairs.push((full_key, ConfigValue::Boolean(*b))),

            // Return error for unsupported types
            _ => return Err(ConfiggerError::UnsupportedValueType(full_key)),
        }
    }
    
    Ok(config_pairs)
 }

 //Validations:

 fn validate_unique_key(unique_key: &Vec<String>) -> Result<(), ConfiggerError> {
    let allowed_keys = ["hash", "full_signature", "address"];
    for key in unique_key {
        if !allowed_keys.contains(&key.as_str()) {
            return Err(ConfiggerError::InvalidFieldOrValue(format!("unique_key = '{}'. Allowed values are: {:?}", key, allowed_keys)));
        }
    }
    Ok(())
 }

 fn validate_output_file_format(output_file_format: &String) -> Result<(), ConfiggerError> {
    let allowed_formats = ["csv", "parquet"];
    if !allowed_formats.contains(&output_file_format.as_str()) {
        return Err(ConfiggerError::InvalidFieldOrValue(format!("output_file_format = '{}'. Allowed values are: {:?}", output_file_format, allowed_formats)));
    }
    Ok(())
 }