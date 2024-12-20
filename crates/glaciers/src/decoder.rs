use alloy::dyn_abi::{DecodedEvent, DynSolValue, EventExt};
use alloy::json_abi::Event;
use alloy::primitives::FixedBytes;
use chrono::Local;
use polars::prelude::*;
use serde::Serialize;
use std::fs;
use std::fs::File;
use std::path::{Path, PathBuf};
use thiserror::Error;
use tokio::sync::{mpsc, Mutex, Semaphore};
use tokio::task;

const MAX_CONCURRENT_FILES_DECODING: usize = 16;
const MAX_CHUNK_THREADS_PER_FILE: usize = 16;
const DECODED_CHUCK_SIZE: usize = 500_000;

#[derive(Error, Debug)]
pub enum DecodeError {
    #[error("Decoding error: {0}")]
    DecodingError(String),
    #[error("Polars error: {0}")]
    PolarsError(#[from] PolarsError),
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Join error: {0}")]
    JoinError(#[from] tokio::task::JoinError),
}

#[derive(Debug, Serialize)]
struct StructuredEventParam {
    name: String,
    index: u32,
    value_type: String,
    value: String,
}

struct ExtDecodedEvent {
    event_values: Vec<DynSolValue>,
    event_keys: Vec<String>,
    event_json: String,
}

//Wrapper type around DynSolValue, to implement to_string function.
struct StringifiedValue(DynSolValue);

impl StringifiedValue {
    pub fn to_string(&self) -> Option<String> {
        match &self.0 {
            DynSolValue::Bool(b) => Some(b.to_string()),
            DynSolValue::Int(i, _) => Some(i.to_string()),
            DynSolValue::Uint(u, _) => Some(u.to_string()),
            DynSolValue::FixedBytes(w, _) => Some(format!("0x{}", w.to_string())),
            DynSolValue::Address(a) => Some(a.to_string()),
            DynSolValue::Function(f) => Some(f.to_string()),
            DynSolValue::Bytes(b) => Some(format!("0x{}", String::from_utf8_lossy(b))),
            DynSolValue::String(s) => Some(s.clone()),
            DynSolValue::Array(arr) => Some(format!(
                "[{}]",
                arr.iter()
                    .filter_map(|v| Self::from(v.clone()).to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )),
            DynSolValue::FixedArray(arr) => Some(format!(
                "[{}]",
                arr.iter()
                    .filter_map(|v| Self::from(v.clone()).to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )),
            DynSolValue::Tuple(tuple) => Some(format!(
                "({})",
                tuple
                    .iter()
                    .filter_map(|v| Self::from(v.clone()).to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )),
        }
    }
}

impl From<DynSolValue> for StringifiedValue {
    fn from(value: DynSolValue) -> Self {
        StringifiedValue(value)
    }
}

pub async fn process_log_files(
    log_folder_path: String,
    topic0_path: String,
) -> Result<(), DecodeError> {

    // Collect log files' paths from log_folder_path
    let log_files: Vec<PathBuf> = fs::read_dir(log_folder_path)?
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
        let topic0_path = topic0_path.clone();
        let semaphore = semaphore.clone();

        // Spawn a tokio task for each file
        let handle = task::spawn(async move {
            // Acquire a permit before processing
            let _permit = semaphore.acquire().await.unwrap();
            process_log_file(log_file_path, topic0_path).await
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

async fn process_log_file(
    log_file_path: PathBuf,
    topic0_path: String,
) -> Result<(), DecodeError> {
    let file_path_str = log_file_path.to_string_lossy().into_owned();
    let file_name = log_file_path
        .file_name()
        .unwrap()
        .to_string_lossy()
        .into_owned();
    let file_folder_path = log_file_path
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

    let ethereum_logs_df = read_parquet_file(&file_path_str)?.collect()?;
    let decoded_df = process_log_df(ethereum_logs_df, topic0_path).await?;

    println!(
        "[{}] Finished decoding file: {}",
        Local::now().format("%Y-%m-%d %H:%M:%S"),
        file_name
    );
    
    let save_path = format!(
        "{}/decoded/{}",
        file_folder_path,
        file_name.replace("logs", "decoded_logs")
    );
    // create folder if it doesn't exist
    fs::create_dir_all(file_folder_path.to_string() + "/decoded")?;

    println!(
        "[{}] Saving decoded logs to: {}",
        Local::now().format("%Y-%m-%d %H:%M:%S"),
        save_path
    );
    save_decoded_logs(decoded_df, &save_path)?;

    Ok(())
}

pub async fn process_log_df (
    log_df: DataFrame,
    topic0_path: String,
) -> Result<DataFrame, DecodeError> {
    let topic0_df = read_parquet_file(&topic0_path)?;
    
    // Perform left join with ABI topic0 list
    let logs_left_join_abi_df = log_df
        .lazy()
        .join(
            topic0_df,
            [col("topic0")],
            [col("topic0")],
            JoinArgs::new(JoinType::Left),
        )
        .select([col("*").exclude(["id"])])
        .collect()?;

    // Split logs files in chunk, decode logs, collected and union results and save in the decoded folder
    decode_logs(logs_left_join_abi_df).await
}

// Helper function to create lazydf from a parquet file
fn read_parquet_file(path: &str) -> Result<LazyFrame, DecodeError> {
    LazyFrame::scan_parquet(path, Default::default()).map_err(|err| DecodeError::PolarsError(err))
}

pub async fn decode_logs(df: DataFrame) -> Result<DataFrame, DecodeError> {
    // Create a semaphore with MAX_THREAD_NUMBER permits
    let semaphore = Arc::new(Semaphore::new(MAX_CHUNK_THREADS_PER_FILE));
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
        let end = (i + DECODED_CHUCK_SIZE).min(total_height);
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

pub fn polars_decode_logs(df: DataFrame) -> Result<DataFrame, DecodeError> {
    let decoded_chuck_df = df
        .lazy()
        //apply decode_log_udf, creating a decoded_log column
        .with_columns([as_struct(vec![
            col("topic0"),
            col("topic1"),
            col("topic2"),
            col("topic3"),
            col("data"),
            col("full_signature"),
        ])
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

    Ok(decoded_chuck_df)
}

// Helper function to union DataFrames
async fn union_dataframes(dfs: Vec<DataFrame>) -> Result<DataFrame, DecodeError> {
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
    let topics_data_sig = extract_log_fields(fields)?;

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
) -> Result<ExtDecodedEvent, DecodeError> {
    let event_sig = parse_event_signature(full_signature)?;
    let decoded_event = decode_event_log(&event_sig, topics, data)?;
    let mut event_values: Vec<DynSolValue> = decoded_event.indexed.clone();
    event_values.extend(decoded_event.body.clone());

    let structured_event = map_event_sig_and_values(&event_sig, &event_values)?;
    let event_keys: Vec<String> = structured_event.iter().map(|p| p.name.clone()).collect();
    let event_json = serde_json::to_string(&structured_event).unwrap_or_else(|_| "[]".to_string());

    let extended_decoded_event = ExtDecodedEvent {
        event_values,
        event_keys,
        event_json,
    };

    Ok(extended_decoded_event)
}

fn parse_event_signature(full_signature: &str) -> Result<Event, DecodeError> {
    Event::parse(full_signature).map_err(|e| DecodeError::DecodingError(e.to_string()))
}

fn decode_event_log(
    event: &Event,
    topics: Vec<FixedBytes<32>>,
    data: &[u8],
) -> Result<DecodedEvent, DecodeError> {
    event
        .decode_log_parts(topics, data, false)
        .map_err(|e| DecodeError::DecodingError(e.to_string()))
}

fn map_event_sig_and_values(
    event_sig: &Event,
    event_values: &Vec<DynSolValue>,
) -> Result<Vec<StructuredEventParam>, DecodeError> {
    if event_values.len() != event_sig.inputs.len() {
        return Err(DecodeError::DecodingError(
            "Mismatch between signature length and returned params length".to_string(),
        ));
    }

    let mut structured_event: Vec<StructuredEventParam> = Vec::new();
    for (i, input) in event_sig.inputs.iter().enumerate() {
        let str_value = StringifiedValue::from(event_values[i].clone());
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

fn save_decoded_logs(mut df: DataFrame, path: &str) -> Result<(), DecodeError> {
    let mut file = File::create(Path::new(path))?;
    ParquetWriter::new(&mut file).finish(&mut df)?;
    Ok(())
}
