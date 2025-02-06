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

#[derive(Debug, Serialize)]
pub struct StructuredParam {
    pub name: String,
    pub index: u32,
    pub value_type: String,
    pub value: String,
}

#[derive(Debug, Clone)]
pub enum DecoderType {
    Log,
    Trace,
}

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

pub async fn decode_df (
    df: DataFrame,
    abi_db_path: String,
    decoder_type: DecoderType,
) -> Result<DataFrame, DecoderError> {
    let abi_db_path = Path::new(&abi_db_path);
    let abi_df = utils::read_df_file(&abi_db_path)?;

    decode_df_with_abi_df(df, abi_df, decoder_type).await
}

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

pub async fn decode(df: DataFrame, decoder_type: DecoderType) -> Result<DataFrame, DecoderError> {
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