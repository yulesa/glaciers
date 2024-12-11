use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use decoding::abi_reader;
use decoding::decoder::decode_logs;
use polars::prelude::*;
use decoding::decoder::DecodeError;
use thiserror::Error;
use chrono::Local;
use tokio::task;
use tokio::sync::Semaphore;

const TOPIC0_FILE_PATH: &str = "ABIs/ethereum__abis_topic0.parquet";
const RAW_LOGS_FOLDER_PATH: &str = "data/logs";
const MAX_CONCURRENT_FILES_DECODING: usize = 16;

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
    let abi_list_df = abi_reader::read_abis_topic0(TOPIC0_FILE_PATH)?;

    // Collect log files' paths from RAW_LOGS_FOLDER_PATH
    let log_files: Vec<PathBuf> = fs::read_dir(RAW_LOGS_FOLDER_PATH)?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .collect();

    // Create a semaphore with MAX_CONCURRENT_FILES_DECODING permits
    let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_FILES_DECODING));
    // Create a vector to hold our join handles
    let mut handles = Vec::new();

    // Spawn a task for each log file
    for log_file_path in log_files {
        // Clone the DataFrame and semafore for each task
        let abi_list_df = abi_list_df.clone();
        let semaphore = semaphore.clone();

        
        // Spawn a tokio task for each file
        let handle = task::spawn(async move {
            // Acquire a permit before processing
            let _permit = semaphore.acquire().await.unwrap();
            process_log_file(log_file_path, abi_list_df).await
        });

        handles.push(handle);
    }

    // Wait for all tasks to complete and collect errors
    for handle in handles {
        // Unwrap the outer Result from task::spawn
        handle.await??;
    }

    println!("[{}] All files processed", Local::now().format("%Y-%m-%d %H:%M:%S"));

    Ok(())
}

async fn process_log_file(
    log_file_path: PathBuf, 
    abi_list_df: DataFrame
) -> Result<(), AppError> {
    let file_path_str = log_file_path.to_string_lossy().into_owned();
    let file_name = log_file_path.file_name().unwrap().to_string_lossy().into_owned();
    
    println!("[{}] Starting file: {}", Local::now().format("%Y-%m-%d %H:%M:%S"), file_path_str);

    // Load Ethereum logs for this specific file
    let ethereum_logs_df = load_ethereum_logs(&file_path_str)?;

    // Perform left join with ABI list using more explicit join strategy
    let logs_left_join_abi_df = ethereum_logs_df
        .lazy()
        .join(
            abi_list_df.lazy(),
            [col("topic0")],
            [col("topic0")],
            JoinArgs::new(JoinType::Left)
        )
        .drop(&["id"])
        .collect()?;

    // Decode logs and handle potential error
    decode_logs(logs_left_join_abi_df, &file_name).await?;
    
    println!("[{}] Finished file: {}", Local::now().format("%Y-%m-%d %H:%M:%S"), file_path_str);

    Ok(())
}

fn load_ethereum_logs(path: &str) -> Result<DataFrame, AppError> {
    LazyFrame::scan_parquet(path, Default::default())?
        .collect()
        .map_err(AppError::from)
}