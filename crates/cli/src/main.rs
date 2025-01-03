use glaciers::abi_reader;
use glaciers::decoder::decode_log_folder;
use glaciers::decoder::DecodeError;
use glaciers::configger::{initialize_glaciers_config, get_config};
use thiserror::Error;

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
    initialize_glaciers_config();
    // Read ABI list
    abi_reader::update_abi_df(get_config().main.abi_df_file_path, get_config().main.abi_folder_path)?;

    // process the log files concurrently
    decode_log_folder(get_config().main.raw_logs_folder_path, get_config().main.abi_df_file_path).await?;
    Ok(())
}
