use glaciers_decoder::abi_reader;
use glaciers_decoder::decoder::process_log_files;
use glaciers_decoder::decoder::DecodeError;
use thiserror::Error;

const TOPIC0_FILE_PATH: &str = "ABIs/ethereum__abis_topic0.parquet";
const ABIS_FOLDER_PATH: &str = "ABIs/abi_database";
const RAW_LOGS_FOLDER_PATH: &str = "data/logs";

#[derive(Error, Debug)]
enum AppError {
    #[error("Decode error: {0}")]
    DecodeError(#[from] DecodeError),
    #[error("Polars error: {0}")]
    PolarsError(#[from] polars::prelude::PolarsError),
    #[error("Join error: {0}")]    
    JoinError(#[from] tokio::task::JoinError),
    #[error("IO error: {0}")]
    IOError(#[from] std::io::Error),
}

#[tokio::main]
async fn main() -> Result<(), AppError> {
    // Read ABI list
    abi_reader::read_abis_topic0(TOPIC0_FILE_PATH.to_string(), ABIS_FOLDER_PATH.to_string())?;

    // process the log files concurrently
    process_log_files(RAW_LOGS_FOLDER_PATH.to_string(), TOPIC0_FILE_PATH.to_string()).await?;

    Ok(())
}
