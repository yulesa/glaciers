use clap::{Parser, Subcommand};
use glaciers::{abi_reader, configger};
use glaciers::decoder::{self, DecoderType};
use std::path::PathBuf;
use thiserror::Error;

#[derive(Error, Debug)]
enum AppError {
    #[error("Configger error: {0}")]
    ConfigError(#[from] configger::ConfiggerError),
    #[error("ABI Reader error: {0}")]
    AbiError(#[from] abi_reader::AbiReaderError),
    #[error("Decoder error: {0}")]
    DecoderError(#[from] decoder::DecoderError),
    #[error("Invalid input: {0}")]
    InvalidInput(String),
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Set configs using a TOML file
    #[arg(short, long, value_names = ["PATH"])]
    toml: Option<String>,

    /// Set config values (ie: -c glacier.prefered_dataframe_type polars). It accepts multiple configs and will always override toml configs.
    #[arg(short, long = "config", value_names = ["KEY", "VALUE"], num_args = 2, action = clap::ArgAction::Append)]
    config: Vec<String>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Read ABI file or folder, or update an existing ABI database
    Abi {
        /// Path to ABI database file (or the path to create a new file). Optional, default: events_abi_db_file_path in config file
        #[arg(short='d', long = "db")]
        abi_db_path: Option<String>,
        /// Path to ABI file or folder. Optional, default: config file
        #[arg(short, long="abi")]
        abi_path: Option<String>
    },
    
    /// Decode Ethereum logs
    DecodeLogs {
        /// Path to log file or folder to decode. Optional, default: raw_logs_folder_path in config file
        #[arg(short, long="log")]
        log_path: Option<String>,
        /// Path to ABI database file. Optional, default: events_abi_db_file_path in config file
        #[arg(short, long="db")]
        abi_db_path: Option<String>
    },

    /// Decode Ethereum traces
    DecodeTraces {
        /// Path to trace file or folder to decode. Optional, default: raw_traces_folder_path in config file
        #[arg(short, long="trace")]
        trace_path: Option<String>,
        /// Path to ABI database file. Optional, default: functions_abi_db_file_path in config file
        #[arg(short, long="db")]
        abi_db_path: Option<String>
    },
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

    // Handle set_config_toml if present
    if let Some(toml) = cli.toml {
        configger::set_config_toml(&toml)?;
    }

    // Handle multiple config args
    for chunk in cli.config.chunks(2) {
        if chunk.len() == 2 {
            let key = &chunk[0];
            let value = &chunk[1];
            let parsed_value = parse_config_value(value);
            configger::set_config(key, parsed_value)?;
        }
    }

    match cli.command {
        Commands::Abi { abi_db_path, abi_path } => {
            let abi_db_path = abi_db_path.unwrap_or_else(|| configger::get_config().main.events_abi_db_file_path);
            let abi_path = abi_path.unwrap_or_else(|| configger::get_config().main.abi_folder_path);

            abi_reader::update_abi_db(abi_db_path, abi_path)?;
        },
        
        Commands::DecodeLogs { log_path, abi_db_path } => {
            let log_path = log_path.unwrap_or_else(|| configger::get_config().main.raw_logs_folder_path);
            let abi_db_path = abi_db_path.unwrap_or_else(|| configger::get_config().main.events_abi_db_file_path);

            let log_path = PathBuf::from(log_path);

            if !log_path.exists() {
                return Err(AppError::InvalidInput(format!("Path does not exist: {}", log_path.display())));
            }

            if log_path.is_dir() {
                decoder::decode_folder(log_path.to_string_lossy().into_owned(), abi_db_path, DecoderType::Log).await?;
            } else {
                decoder::decode_file(log_path, abi_db_path, DecoderType::Log).await?;
            }
        }

        Commands::DecodeTraces { trace_path, abi_db_path } => {
            let trace_path = trace_path.unwrap_or_else(|| configger::get_config().main.raw_traces_folder_path);
            let abi_db_path = abi_db_path.unwrap_or_else(|| configger::get_config().main.functions_abi_db_file_path);
            
            let trace_path = PathBuf::from(trace_path);

            if !trace_path.exists() {
                return Err(AppError::InvalidInput(format!("Path does not exist: {}", trace_path.display())));
            }

            if trace_path.is_dir() {
                decoder::decode_folder(trace_path.to_string_lossy().into_owned(), abi_db_path, DecoderType::Trace).await?;
            } else {
                decoder::decode_file(trace_path, abi_db_path, DecoderType::Trace).await?;
            }
        }
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