use alloy::dyn_abi::{DecodedEvent, DynSolValue, EventExt};
use alloy::json_abi::{Event, EventParam};
use alloy::primitives::FixedBytes;
use chrono::Local;
use polars::prelude::*;
use serde::Serialize;
use std::fs;
use std::path::{Path, PathBuf};
use thiserror::Error;
use tokio::sync::{mpsc, Mutex, Semaphore};
use tokio::task;

use crate::configger::{self, get_config};
use crate::matcher;
use crate::utils;

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
struct StructuredEventParam {
    name: String,
    index: u32,
    value_type: String,
    value: String,
}

struct ExtDecodedEvent {
    event_values: Vec<String>,
    event_keys: Vec<String>,
    event_json: String,
}

pub async fn decode_log_folder(
    log_folder_path: String,
    abi_df_path: String,
) -> Result<(), DecoderError> {

    // Collect log files' paths from log_folder_path
    let log_files: Vec<PathBuf> = fs::read_dir(log_folder_path)?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .collect();

    // Create a semaphore with MAX_CONCURRENT_FILES_DECODING permits
    let semaphore = Arc::new(Semaphore::new(get_config().decoder.max_concurrent_files_decoding));
    // Create a vector to hold our join handles
    let mut handles = Vec::new();

    // Spawn a task for each log file
    for log_file_path in log_files {
        // Clone the DataFrame and semafore for each task
        let abi_df_path = abi_df_path.clone();
        let semaphore = semaphore.clone();

        // Spawn a tokio task for each file
        let handle = task::spawn(async move {
            // Acquire a permit before processing
            let _permit = semaphore.acquire().await.unwrap();
            decode_log_file(log_file_path, abi_df_path).await
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

pub async fn decode_log_file(
    log_file_path: PathBuf,
    abi_df_path: String,
) -> Result<DataFrame, DecoderError> {
    let file_path_str = log_file_path.to_string_lossy().into_owned();
    let file_name = log_file_path
        .file_name()
        .unwrap()
        .to_string_lossy()
        .into_owned();
    let mut file_folder_path = log_file_path
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_string_lossy()
        .into_owned();

    println!(
        "[{}] Starting decoding file: {}",
        Local::now().format("%Y-%m-%d %H:%M:%S"),
        file_path_str
    );

    let ethereum_logs_df = utils::read_df_file(&log_file_path)?;
    let ethereum_logs_df = utils::hex_string_columns_to_binary(ethereum_logs_df)?;
    let mut decoded_df = decode_log_df(ethereum_logs_df, abi_df_path).await?;

    println!(
        "[{}] Finished decoding file: {}",
        Local::now().format("%Y-%m-%d %H:%M:%S"),
        file_name
    );
    
    if !file_folder_path.is_empty() {
        file_folder_path = file_folder_path + "/";
    }
        
    let save_path = format!(
        "{}decoded/{}",
        file_folder_path,
        if file_name.contains("logs") {
            file_name.replace("logs", "decoded_logs")
        } else {
            format!("decoded_logs_{}", file_name)
        }
    );

    let save_path: &Path = Path::new(&save_path);

    if let Some(parent) = save_path.parent() {
        // create folder if it doesn't exist
        fs::create_dir_all(parent.to_string_lossy().into_owned())?;
    }
    let save_path= save_path.with_extension(get_config().decoder.output_file_format);
    println!(
        "[{}] Saving decoded logs to: {:?}",
        Local::now().format("%Y-%m-%d %H:%M:%S"),
        save_path
    );
    utils::write_df_file(&mut decoded_df, &save_path)?;

    Ok(decoded_df)
}

pub async fn decode_log_df (
    log_df: DataFrame,
    abi_df_path: String,
) -> Result<DataFrame, DecoderError> {
    let abi_df_path = Path::new(&abi_df_path);
    let abi_df = utils::read_df_file(&abi_df_path)?;

    decode_log_df_with_abi_df(log_df, abi_df).await
}

pub async fn decode_log_df_with_abi_df(
    log_df: DataFrame,
    abi_df: DataFrame,
) -> Result<DataFrame, DecoderError> {
    // Convert hash and address columns to binary if they aren't already
    let abi_df = utils::abi_df_hex_string_columns_to_binary(abi_df)?;

    // perform matching
    let matched_df = match get_config().decoder.logs_algorithm {
        configger::Algorithm::Topic0Address => matcher::match_logs_by_topic0_address(log_df, abi_df)?,
        configger::Algorithm::Topic0 => matcher::match_logs_by_topic0(log_df, abi_df)?
    };

    // Split logs files in chunk, decode logs, collected and union results and save in the decoded folder
    decode_logs(matched_df).await
}


pub async fn decode_logs(df: DataFrame) -> Result<DataFrame, DecoderError> {
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
        let handle = task::spawn(async move {

            let _permit = sem_clone.acquire().await;
            //Use polars to iterate through each row and decode, communicate through channel the result.
            match polars_decode_logs(chunk_df) {
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

pub fn polars_decode_logs(df: DataFrame) -> Result<DataFrame, DecoderError> {
    let input_schema_alias = get_config().decoder.schema.alias;

    let mut alias_exprs: Vec<Expr> = input_schema_alias.as_array()
        .iter()
        .map(|alias| col(alias.as_str()).alias(alias.as_str()))
        .collect();
    alias_exprs.push(col("full_signature").alias("full_signature"));
    
    let decoded_chuck_df = df
        .lazy()
        //apply decode_log_udf, creating a decoded_log column
        .with_columns([as_struct(alias_exprs)
        .map(decode_log_udf, GetOutput::from_type(DataType::String))
        .alias("decoded_log")])
        //split the udf output column (decoded_log) into 3 columns
        .with_columns([col("decoded_log")
            .str()
            .split(lit(";"))
            .list()
            .get(lit(0))
            .alias("event_values")])
        .with_columns([col("decoded_log")
            .str()
            .split(lit(";"))
            .list()
            .get(lit(1))
            .alias("event_keys")])
        .with_columns([col("decoded_log")
            .str()
            .split(lit(";"))
            .list()
            .get(lit(2))
            .alias("event_json")])
        // Remove the original decoded_log column
        .select([col("*").exclude(["decoded_log"])])
        .collect()?;

    Ok(if get_config().decoder.output_hex_string_encoding {
        utils::binary_columns_to_hex_string(decoded_chuck_df)?
    } else {
        decoded_chuck_df
    })    
}

// Helper function to union DataFrames
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

//Construct a vec with topics, data and signature, and iterate through each, calling the decode function and mapping it to a 3 parts result string
fn decode_log_udf(s: Series) -> PolarsResult<Option<Series>> {
    let series_struct_array: &StructChunked = s.struct_()?;
    let fields = series_struct_array.fields();
    //extract topics, data and signature from the df struct arrays
    let topics_data_sig = extract_log_fields(&fields)?;

    let udf_output: StringChunked = topics_data_sig
        .into_iter()
        .map(|(topics, data, sig)| {
            decode(sig, topics, data)
                .map(|event| {
                    format!(
                        "{:?}; {:?}; {}",
                        event.event_values, event.event_keys, event.event_json
                    )
                })
                // Ignore decoding errors. In the future, we can have a param to log errors or store them in the table.
                .ok()
        })
        .collect();

    Ok(Some(udf_output.into_series()))
}

fn extract_log_fields(fields: &[Series]) -> PolarsResult<Vec<(Vec<FixedBytes<32>>, &[u8], &str)>> {
    let zero_filled_topic = vec![0u8; 32];

    let fields_topic0 = fields[0].binary()?;
    let fields_topic1 = fields[1].binary()?;
    let fields_topic2 = fields[2].binary()?;
    let fields_topic3 = fields[3].binary()?;
    let fields_data = fields[4].binary()?;
    let fields_sig = fields[5].str()?;

    fields_topic0
        .into_iter()
        .zip(fields_topic1.into_iter())
        .zip(fields_topic2.into_iter())
        .zip(fields_topic3.into_iter())
        .zip(fields_data.into_iter())
        .zip(fields_sig.into_iter())
        .map(
            |(((((opt_topic0, opt_topic1), opt_topic2), opt_topic3), opt_data), opt_sig)| {
                let topics = vec![
                    FixedBytes::from_slice(opt_topic0.unwrap_or(&zero_filled_topic)),
                    FixedBytes::from_slice(opt_topic1.unwrap_or(&zero_filled_topic)),
                    FixedBytes::from_slice(opt_topic2.unwrap_or(&zero_filled_topic)),
                    FixedBytes::from_slice(opt_topic3.unwrap_or(&zero_filled_topic)),
                ];
                let data = opt_data.unwrap_or(&[]);
                let sig = opt_sig.unwrap_or("");

                Ok((topics, data, sig))
            },
        )
        .collect()
}

//use alloy functions to create the necessary structs, and call the decode_log_parts function
fn decode(
    full_signature: &str,
    topics: Vec<FixedBytes<32>>,
    data: &[u8],
) -> Result<ExtDecodedEvent, DecoderError> {
    let event_sig = parse_event_signature(full_signature)?;
    let decoded_event = decode_event_log(&event_sig, topics, data)?;
    // Store the indexed values in a vector
    let mut event_values: Vec<DynSolValue> = decoded_event.indexed.clone();
    // Extend the vector with the body(data) values
    event_values.extend(decoded_event.body.clone());

    let structured_event = map_event_sig_and_values(&event_sig, &event_values)?;
    let event_keys: Vec<String> = structured_event.iter().map(|p| p.name.clone()).collect();
    let event_json = serde_json::to_string(&structured_event).unwrap_or_else(|_| "[]".to_string()).trim().to_string();
    // Convert the event_values to a vector of strings
    let event_values: Vec<String> = event_values.iter().map(|d| utils::StrDynSolValue::from(d.clone()).to_string().unwrap_or("None".to_string())).collect();

    let extended_decoded_event = ExtDecodedEvent {
        event_values,
        event_keys,
        event_json,
    };

    Ok(extended_decoded_event)
}

fn parse_event_signature(full_signature: &str) -> Result<Event, DecoderError> {
    Event::parse(full_signature).map_err(|e| DecoderError::DecodingError(e.to_string()))
}

fn decode_event_log(
    event: &Event,
    topics: Vec<FixedBytes<32>>,
    data: &[u8],
) -> Result<DecodedEvent, DecoderError> {
    event
        .decode_log_parts(topics, data, false)
        .map_err(|e| DecoderError::DecodingError(e.to_string()))
}

fn map_event_sig_and_values(
    event_sig: &Event,
    event_values: &Vec<DynSolValue>,
) -> Result<Vec<StructuredEventParam>, DecoderError> {
    // This error might be impossible, because it would make decode_log_parts fail before.
    if event_values.len() != event_sig.inputs.len() {
        return Err(DecoderError::DecodingError(
            "Mismatch between signature length and returned params length".to_string(),
        ));
    }

    // Partition event inputs into indexed and non-indexed so it has the same order as the event_values
    let (event_indexed_inputs, event_data_inputs): (Vec<EventParam>, Vec<EventParam>) = 
        event_sig.inputs.iter()
            .cloned() // Clone to convert &EventParam to EventParam
            .partition(|e| e.indexed);

    // Combine indexed inputs followed by detail inputs
    let mut event_inputs = Vec::with_capacity(event_indexed_inputs.len() + event_data_inputs.len());
    event_inputs.extend(event_indexed_inputs);
    event_inputs.extend(event_data_inputs);

    let mut structured_event: Vec<StructuredEventParam> = Vec::new();
    for (i, input) in event_inputs.iter().enumerate() {
        let str_value = utils::StrDynSolValue::from(event_values[i].clone());
        let event_param = StructuredEventParam {
            name: input.name.clone(),
            index: i as u32,
            value_type: input.ty.to_string(),
            value: str_value.to_string().unwrap_or_else(|| "None".to_string()),
        };
        structured_event.push(event_param);
    }

    Ok(structured_event)
}