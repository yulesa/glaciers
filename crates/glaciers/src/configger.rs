use std::sync::{LazyLock, RwLock};
use std::fs;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct MainConfig {
    pub abi_df_file_path: String,
    pub abi_folder_path: String,
    pub raw_logs_folder_path: String,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct AbiReaderConfig {
    pub existing_df_joining_key: String,
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
            abi_df_file_path: String::new(),
            abi_folder_path: String::new(),
            raw_logs_folder_path: String::new(),
        },
        abi_reader: AbiReaderConfig {
            existing_df_joining_key: String::new(),
        },
        decoder: DecoderConfig {
            max_concurrent_files_decoding: 16,
            max_chunk_threads_per_file: 16,
            decoded_chunk_size: 500_000,
        },
    })
});

pub fn initialize_glaciers_config() {
    let config_str = fs::read_to_string("glaciers_config.toml").unwrap();
    let config: Config = toml::from_str(&config_str).unwrap();
    *GLACIERS_CONFIG.write().unwrap() = config;
}

#[derive(Clone)]
pub enum ConfigValue {
    String(String),
    Number(usize),
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

pub fn set_config(field: &str, value: impl Into<ConfigValue>) -> Result<(), &'static str> {
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
            _ => Err("Invalid field or value type for main section")
        },
        "abi_reader" => match (field, value) {
            ("existing_df_joining_key", ConfigValue::String(v)) => {
                config.abi_reader.existing_df_joining_key = v;
                Ok(())
            },
            _ => Err("Invalid field or value type for abi_reader section")
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
            _ => Err("Invalid field or value type for decoder section")
        },
        _ => Err("Invalid section name")
    }
}

pub fn get_config() -> Config {
    GLACIERS_CONFIG.read().unwrap().clone()
}