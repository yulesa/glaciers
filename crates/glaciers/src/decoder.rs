//! Module for the high level processing and decoding blockchain data.
//! 
//! This module provides functionality to:
//! - Decode a folder of logs/traces
//! - Decode a single log/trace file
//! - Decode a DataFrame of logs/traces using an ABI database file path
//! - Decode a DataFrame of logs/traces using a pre-loaded ABI DataFrame
//! - Split logs/traces DF in chunks, decode logs/traces, collect and union results and save in the decoded folder

use chrono::Local;
use polars::prelude::*;
use serde::Serialize;
use std::fs;
use std::path::{Path, PathBuf};
use thiserror::Error;
use tokio::sync::{mpsc, Mutex, Semaphore};
use tokio::task;

use crate::configger::{get_config, DecoderAlgorithm};
use crate::matcher;
use crate::utils;
use crate::log_decoder;
use crate::trace_decoder;

/// Error types that can occur during decoding operations
#[derive(Error, Debug)]
pub enum DecoderError {
    #[error("Decoding error: {0}")]
    DecodingError(String),
    #[error("Polars error: {0}")]
    PolarsError(#[from] PolarsError),
    #[error("Matcher error: {0}")]
    MatcherError(#[from] matcher::MatcherError),
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Join error: {0}")]
    JoinError(#[from] tokio::task::JoinError)
}

/// Represents a structured parameter from decoded data
/// 
/// Contains the name, position, type and value of a decoded parameter
/// This is each item of event_json (logs) or input_json/output_json (traces)
#[derive(Debug, Serialize)]
pub struct StructuredParam {
    pub name: String,
    pub index: u32,
    pub value_type: String,
    pub value: String,
}

/// Specifies the source type of blockchain data to decode
#[derive(Debug, Clone)]
pub enum DecoderType {
    /// Event logs source data
    Log,
    /// Transaction traces source data
    Trace,
}

/// Decodes all files in a folder. It spawns a task for each file to parallelize the decoding process.
///
/// # Arguments
/// * `folder_path` - Path to folder containing files to decode
/// * `abi_db_path` - Path to ABI database file
/// * `decoder_type` - Type of data to decode (Log or Trace)
///
/// # Returns
/// * `Ok(())` if all files were processed successfully. Succeful return is empty, since multiple files are decoded in parallel.
/// * `Err(DecoderError)` if any file fails to process
/// 
/// 
/// # Notes
/// This function gets the max_concurrent_files_decoding from the config and uses it
/// to limit the number of concurrent files that can be decoded at the same time.
///
/// # Example
/// ```no_run
/// use glaciers::decoder::{decode_folder, DecoderType};
/// 
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     decode_folder(
///         "path/to/folder".to_string(),
///         "path/to/abi_db.parquet".to_string(),
///         DecoderType::Log
///     ).await?;
///     Ok(())
/// }
/// ```
pub async fn decode_folder(
    folder_path: String,
    abi_db_path: String,
    decoder_type: DecoderType,
) -> Result<(), DecoderError> {

    // Collect files' paths from folder_path
    let files: Vec<PathBuf> = fs::read_dir(folder_path)?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .collect();

    // Create a semaphore with MAX_CONCURRENT_FILES_DECODING permits
    let semaphore = Arc::new(Semaphore::new(get_config().decoder.max_concurrent_files_decoding));
    // Create a vector to hold our join handles
    let mut handles = Vec::new();

    // Spawn a task for each file
    for file_path in files {
        // skip PathBuf belonging to folders
        if file_path.is_dir() {
            continue
        }
        // Clone the DataFrame and semafore for each task
        let abi_db_path = abi_db_path.clone();
        let semaphore = semaphore.clone();
        let decoder_type_clone = decoder_type.clone();
        // Spawn a tokio task for each file
        let handle = task::spawn(async move {
            // Acquire a permit before processing
            let _permit = semaphore.acquire().await.unwrap();
            decode_file(file_path, abi_db_path, decoder_type_clone).await
        });

        handles.push(handle);
    }

    // Wait for all tasks to complete and collect errors
    for handle in handles {
        // Unwrap the outer Result from task::spawn
        handle.await??;
    }

    println!(
        "[{}] All files processed",
        Local::now().format("%Y-%m-%d %H:%M:%S")
    );
    Ok(())
}

/// Decodes a single file using the specified ABI database
///
/// # Arguments
/// * `file_path` - Path to file to decode
/// * `abi_db_path` - Path to ABI database file
/// * `decoder_type` - Type of data to decode (Log or Trace)
///
/// # Returns
/// * `Ok(DataFrame)` containing decoded data
/// * `Err(DecoderError)` if decoding fails
///
/// It also saves decoded data to a new file in a 'decoded' subdirectory, in the same folder as the source file.
/// 
/// # Notes
/// The output format (binary/hex) of some columns is determined by configuration.
pub async fn decode_file(
    file_path: PathBuf,
    abi_db_path: String,
    decoder_type: DecoderType,
) -> Result<DataFrame, DecoderError> {
    let file_path_str = file_path.to_string_lossy().into_owned();
    let file_name = file_path
        .file_name()
        .unwrap()
        .to_string_lossy()
        .into_owned();
    let mut file_folder_path = file_path
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_string_lossy()
        .into_owned();
    
    if !file_folder_path.is_empty() {
        file_folder_path = file_folder_path + "/";
    }
    let save_path = match decoder_type {
        DecoderType::Log => format!(
            "{}decoded/{}",
            file_folder_path,
            if file_name.contains("logs") {
                file_name.replace("logs", "decoded_logs")
            } else {
                format!("decoded_logs_{}", file_name)
            }
        ),
        DecoderType::Trace => format!(
            "{}decoded/{}",
            file_folder_path,
            if file_name.contains("traces") {
                file_name.replace("traces", "decoded_traces")
            } else {
                format!("decoded_traces_{}", file_name)
            }
        )
    };

    println!(
        "[{}] Starting decoding file: {}",
        Local::now().format("%Y-%m-%d %H:%M:%S"),
        file_path_str
    );

    let file_df = utils::read_df_file(&file_path)?;
    let file_df = utils::hex_string_columns_to_binary(file_df, &decoder_type)?;
    let mut decoded_df = decode_df(file_df, abi_db_path, decoder_type).await?;

    println!(
        "[{}] Finished decoding file: {}",
        Local::now().format("%Y-%m-%d %H:%M:%S"),
        file_name
    );

    let save_path: &Path = Path::new(&save_path);

    if let Some(parent) = save_path.parent() {
        // create folder if it doesn't exist
        fs::create_dir_all(parent.to_string_lossy().into_owned())?;
    }

    let save_path= save_path.with_extension(get_config().decoder.output_file_format);
    utils::write_df_file(&mut decoded_df, &save_path)?;
    
    println!(
        "[{}] Saving decoded to: {:?}",
        Local::now().format("%Y-%m-%d %H:%M:%S"),
        save_path
    );

    Ok(decoded_df)
}

/// Decodes a logs/traces DataFrame using an ABI database file path
///
/// # Arguments
/// * `df` - DataFrame containing raw blockchain data
/// * `abi_db_path` - Path to ABI database file
/// * `decoder_type` - Type of data to decode
///
/// # Returns
/// * `Ok(DataFrame)` containing decoded data
/// * `Err(DecoderError)` if decoding fails
pub async fn decode_df(
    df: DataFrame,
    abi_db_path: String,
    decoder_type: DecoderType,
) -> Result<DataFrame, DecoderError> {
    let abi_db_path = Path::new(&abi_db_path);
    let abi_df = utils::read_df_file(&abi_db_path)?;

    decode_df_with_abi_df(df, abi_df, decoder_type).await
}

/// Decodes a logs/traces DataFrame using a pre-loaded ABI DataFrame
///
/// # Arguments
/// * `df` - DataFrame containing raw blockchain data
/// * `abi_df` - DataFrame containing ABI definitions
/// * `decoder_type` - Type of data to decode
///
/// # Returns
/// * `Ok(DataFrame)` containing decoded data
/// * `Err(DecoderError)` if decoding fails
/// 
/// # Notes
/// The function gets the matching algorithm from the config and uses it to join the logs/traces with ABI itens.
pub async fn decode_df_with_abi_df(
    df: DataFrame,
    abi_df: DataFrame,
    decoder_type: DecoderType,
) -> Result<DataFrame, DecoderError> {
    // Convert hash and address columns to binary if they aren't already
    let abi_df = utils::abi_df_hex_string_columns_to_binary(abi_df)?;

    // perform matching
    let matched_df = match decoder_type {
        DecoderType::Log => match get_config().decoder.algorithm {
            DecoderAlgorithm::HashAddress => matcher::match_logs_by_topic0_address(df, abi_df)?,
            DecoderAlgorithm::Hash => matcher::match_logs_by_topic0(df, abi_df)?
        },
        DecoderType::Trace => match get_config().decoder.algorithm {
            DecoderAlgorithm::HashAddress => matcher::match_traces_by_4bytes_address(df, abi_df)?,
            DecoderAlgorithm::Hash => matcher::match_traces_by_4bytes(df, abi_df)?
        }
    };

    // Split logs files in chunk, decode logs, collected and union results and save in the decoded folder
    decode(matched_df, decoder_type).await
}

/// Handles the decoding of matched logs/traces with ABI itens. It spawns a thread for each chunk to parallelize the decoding process.
///
/// # Arguments
/// * `df` - DataFrame containing matched logs/traces and ABI itens
/// * `decoder_type` - Type of data to decode
///
/// # Returns
/// * `Ok(DataFrame)` containing all decoded chunks combined
/// * `Err(DecoderError)` if decoding fails
/// 
/// # Notes
/// The function gets the decoded_chunk_size from the config and uses it to split the DataFrame in chunks.
/// It also gets the max_chunk_threads_per_file from the config and uses it to limit the number 
/// of parallel threads that can be used to decode each chunk.
/// Total number of threads can be a max of max_chunk_threads_per_file * max_concurrent_files_decoding.
async fn decode(df: DataFrame, decoder_type: DecoderType) -> Result<DataFrame, DecoderError> {
    // Create a semaphore with MAX_THREAD_NUMBER permits
    let semaphore = Arc::new(Semaphore::new(get_config().decoder.max_chunk_threads_per_file));
    // Create a channel to communicate tasks results
    let (tx, mut rx) = mpsc::channel(10);
    // Shared vector to collect DataFrame chunks
    let collected_dfs = Arc::new(Mutex::new(Vec::new()));
    // Vector to hold our tasks handles
    let mut handles = Vec::new();
    
    // Split the DataFrame in chunks and spawn a task for each chunk
    let total_height = df.height();
    let mut i = 0;
    while i < total_height {
        let end = (i + get_config().decoder.decoded_chunk_size).min(total_height);
        let chunk_df = df.slice(i as i64, end - i);

        let sem_clone = semaphore.clone();
        let tx_clone = tx.clone();
        let collected_dfs_clone = collected_dfs.clone();
        let decoder_type_clone = decoder_type.clone();
        let handle = task::spawn(async move {

            let _permit = sem_clone.acquire().await;
            //Use polars to iterate through each row and decode, communicate through channel the result.
            let decoded_chunk = match decoder_type_clone {
                DecoderType::Log => log_decoder::polars_decode_logs(chunk_df),
                DecoderType::Trace => trace_decoder::polars_decode_traces(chunk_df)
            };
            match decoded_chunk {
                Ok(decoded_chunk) => {
                    // Acquire lock before modifying shared state
                    let mut dfs = collected_dfs_clone.lock().await;
                        dfs.push(decoded_chunk);

                        tx_clone
                            .send(Ok(()))
                            .await
                            .expect("Failed to send result. Main thread may have been dropped");
                }
                Err(e) => {
                    tx_clone.send(Err(e)).await.expect("Failed. polars_decode_logs returned an error");
                }
            }
            // Permit is automatically released when _permit goes out of scope
        });
        
        handles.push(handle);
        i = end;
    }
    
    // Drop the original sender to allow rx to complete
    drop(tx);
    
    // Collect all results
    while let Some(result) = rx.recv().await {
        match result {
            Ok(_) => {}
            Err(e) => return Err(e),
        }
    }
        
    // Wait for all spawned tasks to complete
    for handle in handles {
        handle.await?;
    }
    
    let collected_dfs = collected_dfs.lock().await.clone();
    
    // Concatenate and save the final DataFrame
    union_dataframes(collected_dfs).await
}

/// Auxiliary function to combine multiple DataFrames into a single DataFrame
///
/// # Arguments
/// * `dfs` - Vector of DataFrames to combine
///
/// # Returns
/// * `Ok(DataFrame)` containing all input DataFrames combined
/// * `Err(DecoderError)` if union operation fails
async fn union_dataframes(dfs: Vec<DataFrame>) -> Result<DataFrame, DecoderError> {
    // If only one DataFrame, return it directly
    if dfs.len() == 1 {
        return Ok(dfs[0].clone());
    }

    // Use Polars' vertical concatenation with union semantics
    let lazy_dfs: Vec<LazyFrame> = dfs.into_iter().map(|df| df.lazy()).collect();
    let unioned_df = concat(&lazy_dfs, UnionArgs::default())?.collect()?;

    Ok(unioned_df)
}