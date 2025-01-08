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
    pub main: MainConfig,
    pub abi_reader: AbiReaderConfig,
    pub decoder: DecoderConfig,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct MainConfig {
    pub abi_df_file_path: String,
    pub abi_folder_path: String,
    pub raw_logs_folder_path: String,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct AbiReaderConfig {
    pub unique_key: Vec<String>,
    pub output_hex_string_encoding: bool,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct DecoderConfig {
    pub schema: SchemaConfig,
    pub output_hex_string_encoding: bool,
    pub max_concurrent_files_decoding: usize,
    pub max_chunk_threads_per_file: usize,
    pub decoded_chunk_size: usize,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct SchemaConfig {
    pub alias: AliasConfig,
    pub datatype: DatatypeConfig,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct AliasConfig {
    pub topic0: String,
    pub topic1: String,
    pub topic2: String,
    pub topic3: String,
    pub data: String,
}

impl AliasConfig {
    pub fn as_array(&self) -> Vec<String> {
        vec![self.topic0.clone(), self.topic1.clone(), self.topic2.clone(), self.topic3.clone(), self.data.clone()]
    }
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct DatatypeConfig {
    pub topic0: DataType,
    pub topic1: DataType,
    pub topic2: DataType,
    pub topic3: DataType,
    pub data: DataType,
}

impl DatatypeConfig {
    pub fn as_array(&self) -> Vec<DataType> {
        vec![self.topic0.clone(), self.topic1.clone(), self.topic2.clone(), self.topic3.clone(), self.data.clone()]
    }
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub enum DataType {
    Binary,
    HexString
}


pub static GLACIERS_CONFIG: LazyLock<RwLock<Config>> = LazyLock::new(|| {
    RwLock::new(Config {
        main: MainConfig {
            abi_df_file_path: String::from("ABIs/ethereum__abis.parquet"),
            abi_folder_path: String::from("ABIs/abi_database"),
            raw_logs_folder_path: String::from("data/logs"),
        },
        abi_reader: AbiReaderConfig {
            output_hex_string_encoding: false,
            unique_key: vec![String::from("hash"), String::from("full_signature")],
        },
        decoder: DecoderConfig {
            schema: SchemaConfig {
                alias: AliasConfig {
                    topic0: String::from("topic0"),
                    topic1: String::from("topic1"),
                    topic2: String::from("topic2"),
                    topic3: String::from("topic3"),
                    data: String::from("data"),
                },
                datatype: DatatypeConfig {
                    topic0: DataType::Binary,
                    topic1: DataType::Binary,
                    topic2: DataType::Binary,
                    topic3: DataType::Binary,
                    data: DataType::Binary,
                }
            },
            output_hex_string_encoding: false,
            max_concurrent_files_decoding: 16,
            max_chunk_threads_per_file: 16,
            decoded_chunk_size: 500_000,
        },
    })
});

#[derive(Clone, FromPyObject)]
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
        "main" => match (field, value) {
            (Some("abi_df_file_path"), ConfigValue::String(v)) => config.main.abi_df_file_path = v,
            (Some("abi_folder_path"), ConfigValue::String(v)) => config.main.abi_folder_path = v,
            (Some("raw_logs_folder_path"), ConfigValue::String(v)) => config.main.raw_logs_folder_path = v,
            _ => return Err(ConfiggerError::InvalidFieldOrValue(field.unwrap_or("").to_string()))
        },

        "abi_reader" => match (field, value) {
            (Some("output_hex_string_encoding"), ConfigValue::Boolean(v)) => config.abi_reader.output_hex_string_encoding = v,
            (Some("unique_key"), ConfigValue::List(v)) => config.abi_reader.unique_key = v,
            (Some("unique_key"), ConfigValue::String(v)) => config.abi_reader.unique_key = vec![v],
            _ => return Err(ConfiggerError::InvalidFieldOrValue(field.unwrap_or("").to_string()))
        },
        
        "decoder" => match (field, value) {
            (Some("schema"), value) => match (subfield, value) {
                (Some("alias"), ConfigValue::String(v)) => {
                    match schema_field {
                        Some("topic0") => config.decoder.schema.alias.topic0 = v,
                        Some("topic1") => config.decoder.schema.alias.topic1 = v,
                        Some("topic2") => config.decoder.schema.alias.topic2 = v,
                        Some("topic3") => config.decoder.schema.alias.topic3 = v,
                        Some("data") => config.decoder.schema.alias.data = v,
                        _ => return Err(ConfiggerError::InvalidFieldOrValue(schema_field.unwrap_or("").to_string()))
                    }
                },
                (Some("datatype"), ConfigValue::String(v)) => {
                    match schema_field {
                        Some("topic0") => config.decoder.schema.datatype.topic0 = match v.as_str() {
                            "Binary" => DataType::Binary,
                            "HexString" => DataType::HexString,
                            _ => return Err(ConfiggerError::InvalidFieldOrValue("Invalid datatype".to_string()))
                        },
                        Some("topic1") => config.decoder.schema.datatype.topic1 = match v.as_str() {
                            "Binary" => DataType::Binary,
                            "HexString" => DataType::HexString,
                            _ => return Err(ConfiggerError::InvalidFieldOrValue("Invalid datatype".to_string()))
                        },
                        Some("topic2") => config.decoder.schema.datatype.topic2 = match v.as_str() {
                            "Binary" => DataType::Binary,
                            "HexString" => DataType::HexString,
                            _ => return Err(ConfiggerError::InvalidFieldOrValue("Invalid datatype".to_string()))
                        },
                        Some("topic3") => config.decoder.schema.datatype.topic3 = match v.as_str() {
                            "Binary" => DataType::Binary,
                            "HexString" => DataType::HexString,
                            _ => return Err(ConfiggerError::InvalidFieldOrValue("Invalid datatype".to_string()))
                        },
                        Some("data") => config.decoder.schema.datatype.data = match v.as_str() {
                            "Binary" => DataType::Binary,
                            "HexString" => DataType::HexString,
                            _ => return Err(ConfiggerError::InvalidFieldOrValue("Invalid datatype".to_string()))
                        },
                        _ => return Err(ConfiggerError::InvalidFieldOrValue(schema_field.unwrap_or("").to_string()))
                    }
                },
                _ => return Err(ConfiggerError::InvalidFieldOrValue(subfield.unwrap_or("").to_string()))
            },
            (Some("output_hex_string_encoding"), ConfigValue::Boolean(v)) => config.decoder.output_hex_string_encoding = v,
            (Some("max_concurrent_files_decoding"), ConfigValue::Number(v)) => config.decoder.max_concurrent_files_decoding = v,
            (Some("max_chunk_threads_per_file"), ConfigValue::Number(v)) => config.decoder.max_chunk_threads_per_file = v,
            (Some("decoded_chunk_size"), ConfigValue::Number(v)) => config.decoder.decoded_chunk_size = v,
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
 
 fn process_table(
    prefix: &str, 
    table: &toml::Table,
 ) -> Result<Vec<(String, ConfigValue)>, ConfiggerError> {
    table.iter()
        .map(|(key, value)| {
            // Build full key path with prefix for nested tables
            let full_key = if prefix.is_empty() {
                key.to_string()
            } else {
                format!("{}.{}", prefix, key)
            };
            
            match value {
                // Recursively process nested tables
                toml::Value::Table(nested) => process_table(&full_key, nested),
                
                // Handle string values, checking for hex prefix
                toml::Value::String(s) => Ok(vec![(
                    full_key,
                    ConfigValue::String(s.clone())
                )]),
 
                // Convert integer to usize
                toml::Value::Integer(n) => Ok(vec![(
                    full_key,
                    ConfigValue::Number(*n as usize)
                )]),
 
                // Convert array to Vec<String>, ensuring all elements are strings
                toml::Value::Array(arr) => Ok(vec![(
                    full_key.clone(),
                    ConfigValue::List(arr.iter()
                        .map(|v| v.as_str()
                            .ok_or_else(|| ConfiggerError::UnsupportedValueType(full_key.clone()))
                            .map(String::from))
                        .collect::<Result<_, _>>()?
                    )
                )]),
 
                toml::Value::Boolean(b) => Ok(vec![(
                    full_key,
                    ConfigValue::Boolean(*b)
                )]),
 
                // Return error for unsupported types
                _ => Err(ConfiggerError::UnsupportedValueType(full_key)),
            }
        })
        // Combine all results into a single Vec
        .try_fold(Vec::new(), |mut acc, result| {
            acc.extend(result?);
            Ok(acc)
        })
 }