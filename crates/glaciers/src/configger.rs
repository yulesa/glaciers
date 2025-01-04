use std::sync::{LazyLock, RwLock};
use std::fs;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ConfiggerError {
    #[error("Error while setting GLACIERS_CONFIG, could not read Toml file, IO error: {0}")]
    IOError(#[from] std::io::Error),
    #[error("Error while setting GLACIERS_CONFIG, could not parse Toml file, parse error: {0}")]
    ParseError(#[from] toml::de::Error),
    #[error("Error while setting GLACIERS_CONFIG, invalid Toml format")]
    InvalidTomlFormat,
    #[error("Error while setting GLACIERS_CONFIG, unsupported value type for field {0}")]
    UnsupportedValueType(String),
    #[error("Error while setting GLACIERS_CONFIG, invalid field or value type for field {0}")]
    InvalidFieldOrValue(String),
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
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct DecoderConfig {
    pub max_concurrent_files_decoding: usize,
    pub max_chunk_threads_per_file: usize,
    pub decoded_chunk_size: usize,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct Config {
    pub main: MainConfig,
    pub abi_reader: AbiReaderConfig,
    pub decoder: DecoderConfig,
}

pub static GLACIERS_CONFIG: LazyLock<RwLock<Config>> = LazyLock::new(|| {
    RwLock::new(Config {
        main: MainConfig {
            abi_df_file_path: String::from("ABIs/ethereum__abis.parquet"),
            abi_folder_path: String::from("ABIs/abi_database"),
            raw_logs_folder_path: String::from("data/logs"),
        },
        abi_reader: AbiReaderConfig {
            unique_key: vec![String::from("hash"), String::from("full_signature")],
        },
        decoder: DecoderConfig {
            max_concurrent_files_decoding: 16,
            max_chunk_threads_per_file: 16,
            decoded_chunk_size: 500_000,
        },
    })
});

#[derive(Clone)]
pub enum ConfigValue {
    String(String),
    Number(usize),
    List(Vec<String>),
}

impl From<String> for ConfigValue {
    fn from(s: String) -> Self {
        ConfigValue::String(s)
    }
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

pub fn get_config() -> Config {
    GLACIERS_CONFIG.read().unwrap().clone()
}

pub fn set_config(field: &str, value: impl Into<ConfigValue>) -> Result<(), ConfiggerError> {
    let mut config = GLACIERS_CONFIG.write().unwrap();
    let value = value.into();
    let section = field.split(".").next().unwrap();
    let field = field.split(".").last().unwrap();

    match section {
        "main" => match (field, value) {
            ("abi_df_file_path", ConfigValue::String(v)) => {
                config.main.abi_df_file_path = v;
                Ok(())
            },
            ("abi_folder_path", ConfigValue::String(v)) => {
                config.main.abi_folder_path = v;
                Ok(())
            },
            ("raw_logs_folder_path", ConfigValue::String(v)) => {
                config.main.raw_logs_folder_path = v;
                Ok(())
            },
            _ => Err(ConfiggerError::InvalidFieldOrValue(field.to_string()))
        },
        "abi_reader" => match (field, value) {
            ("unique_key", ConfigValue::List(v)) => {
                config.abi_reader.unique_key = v; 
                Ok(())
            },
            ("unique_key", ConfigValue::String(v)) => {
                config.abi_reader.unique_key = vec![v];
                Ok(())
            },
            _ => Err(ConfiggerError::InvalidFieldOrValue(field.to_string()))
        },
        "decoder" => match (field, value) {
            ("max_concurrent_files_decoding", ConfigValue::Number(v)) => {
                config.decoder.max_concurrent_files_decoding = v;
                Ok(())
            },
            ("max_chunk_threads_per_file", ConfigValue::Number(v)) => {
                config.decoder.max_chunk_threads_per_file = v;
                Ok(())
            },
            ("decoded_chunk_size", ConfigValue::Number(v)) => {
                config.decoder.decoded_chunk_size = v;
                Ok(())
            },
            _ => Err(ConfiggerError::InvalidFieldOrValue(field.to_string()))
        },
        _ => Err(ConfiggerError::InvalidFieldOrValue(section.to_string()))
    }
}

pub fn set_config_toml(file_path: &str) -> Result<(), ConfiggerError>{
    let toml_content = fs::read_to_string(file_path).map_err(ConfiggerError::IOError)?;
    let update_value = toml::from_str::<toml::Value>(&toml_content).map_err(ConfiggerError::ParseError)?;
    let table = update_value.as_table().ok_or(ConfiggerError::InvalidTomlFormat)?;
    
    for (section, values) in table {
        let section_table = values.as_table().ok_or(ConfiggerError::InvalidTomlFormat)?;
        
        for (key, value) in section_table {
            let full_key = format!("{}.{}", section, key);
            
            let config_value = match value {
                toml::Value::String(s) => ConfigValue::String(s.clone()),
                toml::Value::Integer(n) => ConfigValue::Number(*n as usize),
                toml::Value::Array(v) => ConfigValue::List(v.iter().map(|s| s.as_str().unwrap().to_string()).collect()),
                _ => return Err(ConfiggerError::UnsupportedValueType(full_key))
            };

            set_config(&full_key, config_value)?;
        }
    }
    Ok(())
}