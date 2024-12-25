use glaciers::abi_reader;
use glaciers::decoder::process_log_files;
use glaciers::decoder::DecodeError;
use thiserror::Error;

const ABI_DF_FILE_PATH: &str = "ABIs/ethereum__abis.parquet";
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
    #[error("ABI error: {0}")]
    AbiError(String),
}

impl From<glaciers::abi_reader::AbiReadError> for AppError {
    fn from(err: glaciers::abi_reader::AbiReadError) -> Self {
        match err {
            glaciers::abi_reader::AbiReadError::PolarsError(e) => AppError::PolarsError(e),
            _ => AppError::AbiError(err.to_string()),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), AppError> {
    // Read ABI list
    abi_reader::update_abi_df(ABI_DF_FILE_PATH.to_string(), ABIS_FOLDER_PATH.to_string())?;

    // process the log files concurrently
    process_log_files(RAW_LOGS_FOLDER_PATH.to_string(), ABI_DF_FILE_PATH.to_string()).await?;

    Ok(())
}
