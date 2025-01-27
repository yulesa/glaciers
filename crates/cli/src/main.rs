use clap::{Parser, Subcommand};
use glaciers::{abi_reader, configger, decoder};
use std::path::PathBuf;
use thiserror::Error;
use toml;

#[derive(Error, Debug)]
enum AppError {
    #[error("Configger error: {0}")]
    ConfigError(#[from] configger::ConfiggerError),
    #[error("ABI Reader error: {0}")]
    AbiError(#[from] abi_reader::AbiReaderError),
    #[error("Decoder error: {0}")]
    DecodeError(#[from] decoder::DecoderError),
    #[error("Invalid input: {0}")]
    InvalidInput(String),
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Read ABI file or folder, or update an existing ABI database
    Abi {
        /// Path to ABI database file (or the path to create a new file)
        abi_df_path: String,
        /// Path to ABI file or folder
        abi_path: String
    },
    
    /// Decode Ethereum logs
    Decode {
        /// Path to log file or folder to decode
        log_path: PathBuf,
        /// Path to ABI database file
        abi_df_path: String,
    },
    
    /// Manage configuration
    SetConfig {
        /// config key
        key: String,
        /// Value to set (if a list don't use spaces)
        value: String,
    },

    /// Set configuration from a TOML file
    SetConfigToml {
        /// Path to TOML config file
        path: String,
    },

    /// Display current configuration
    DisplayConfig,
}

#[tokio::main]
async fn main() {
    if let Err(err) = async_main().await {
        eprintln!("Error: {}", err);
        std::process::exit(1);
    }
}

async fn async_main() -> Result<(), AppError> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Abi { abi_df_path, abi_path } => {
            abi_reader::update_abi_df(abi_df_path, abi_path)?;
        },
        
        Commands::Decode { log_path, abi_df_path } => {
            if !log_path.exists() {
                return Err(AppError::InvalidInput(format!("Path does not exist: {}", log_path.display())));
            }

            if log_path.is_dir() {
                decoder::decode_log_folder(log_path.to_string_lossy().into_owned(), abi_df_path).await?;
            } else {
                decoder::decode_log_file(log_path, abi_df_path).await?;
            }
        },
        
        Commands::SetConfig { key, value } => {
            let value = parse_config_value(&value);
            configger::set_config(&key, value)?;
        },

        Commands::SetConfigToml { path } => {
            configger::set_config_toml(&path)?;
        },

        Commands::DisplayConfig => {
            println!("Current configuration:\n{}", toml::to_string(&configger::get_config()).unwrap());
        },
    }

    Ok(())
}

fn parse_config_value(value: &str) ->configger::ConfigValue {

    let value = match value.to_lowercase().as_str() {
        // Boolean values
        "true" => configger::ConfigValue::Boolean(true),
        "false" => configger::ConfigValue::Boolean(false),
        // Numeric values
        _ if value.parse::<usize>().is_ok() => configger::ConfigValue::Number(value.parse().unwrap()),
        // List values
        _ if value.contains(',') => configger::ConfigValue::List(value.replace("[", "").replace("]", "").split(',').map(|s| s.trim().to_string()).collect()),
        // String values
        _ => configger::ConfigValue::String(value.to_string()),
    };

    value
}