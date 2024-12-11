use std::fs::File;
use std::path::Path;
use alloy::dyn_abi::{DecodedEvent, DynSolValue, EventExt};
use alloy::json_abi::Event;
use alloy::primitives::FixedBytes;
use polars::prelude::*;
use thiserror::Error;
use serde::Serialize;
use sysinfo::System;
use tokio::sync::{mpsc, Semaphore};
use tokio::task;

const DECODED_CHUCK_SIZE: usize = 500_000;
const MAX_THREAD_NUMBER: usize = 16;
const DECODED_PATH: &str = "data/decoded/ethereum__decoded_logs__";

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
struct StructuredEventParam  {
    name: String,
    index: u32,
    value_type: String,
    value: String,
}

struct ExtDecodedEvent {
    event_values: Vec<DynSolValue>,
    event_keys: Vec<String>,
    event_json: String
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
                tuple.iter()
                    .filter_map(|v| Self::from(v.clone()).to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ))
        }
    }
}

impl From<DynSolValue> for StringifiedValue {
    fn from(value: DynSolValue) -> Self {
        StringifiedValue(value)
    }
}

pub async  fn decode_logs(df: DataFrame) -> Result<(), DecodeError> {
    // Create a semaphore with 4 permits
    let semaphore = Arc::new(Semaphore::new(MAX_THREAD_NUMBER));
    let (tx, mut rx) = mpsc::channel(10); // Adjust buffer size as needed
    let total_height = df.height();
    let mut handles = Vec::new();

    //break the logs into chunks of DECODEDCHUCKSIZE
    let mut i = 0;
    while i < total_height {
        let end = (i + DECODED_CHUCK_SIZE).min(total_height);
        let chunk = df.slice(i as i64, end - i);
        
        // Clone the semaphore and transmitter for the task
        let sem_clone = semaphore.clone();
        let tx_clone = tx.clone();

        let handle = task::spawn(async move {
            // Acquire a permit from the semaphore
            let _permit = sem_clone.acquire().await.unwrap();

            // Run the blocking operation
            match polars_decode_logs(chunk, i, end) {
                Ok(_) => {
                    tx_clone.send(Ok((i, end))).await.expect("Failed to send result");
                },
                Err(e) => {
                    tx_clone.send(Err(e)).await.expect("Failed to send error");
                }
            }

            // Permit is automatically released when _permit goes out of scope
        });

        handles.push(handle);
        i = end;
    }

    // Drop the original sender to allow rx to complete
    drop(tx);

    // Wait for all tasks to complete and check for errors
    while let Some(result) = rx.recv().await {
        match result {
            Ok((start, end)) => {
                println!("[{}] Finished decoding chunk: {} to {}", 
                    chrono::Local::now().format("%Y-%m-%d %H:%M:%S"), 
                    start, 
                    end
                );
                print_system_memory();
            }
            Err(e) => return Err(e),
        }
    }

    // Wait for all spawned tasks to complete
    for handle in handles {
        handle.await?;
    }

    Ok(())
}

fn polars_decode_logs(df: DataFrame, i: usize, end: usize) -> Result<(), DecodeError> {
    println!("[{}] Start decoding chunk: {} to {}", 
        chrono::Local::now().format("%Y-%m-%d %H:%M:%S"), 
        i, 
        end
    );
    let decoded_chuck_df = df.lazy()
    //apply decode_log_udf
        .with_columns([
            as_struct(vec![col("topic0"), col("topic1"), col("topic2"), col("topic3"), col("data"), col("full_signature")])
                .map(decode_log_udf, GetOutput::from_type(DataType::String))
                .alias("decoded_log"),
        ])
    //split the udf output column (decoded_log) into 3 columns
        .with_columns([
            col("decoded_log")
            .str()
            .split(lit(";"))
            .list()
            .get(lit(0), false)
            .alias("event_values")
        ])
        .with_columns([
            col("decoded_log")
            .str()
            .split(lit(";"))
            .list()
            .get(lit(1), false)
            .alias("event_keys")
        ])
        .with_columns([
            col("decoded_log")
            .str()
            .split(lit(";"))
            .list()
            .get(lit(2), false)
            .alias("event_json")
        ])
    // Remove the original decoded_log column
        .drop(vec!["decoded_log"])
        .collect()?;

    //save the decoded logs to parquet 
    save_decoded_logs(decoded_chuck_df, &format!("{}{}_to_{}.parquet", DECODED_PATH, i, end))?;
    Ok(())

}

fn decode_log_udf(s: Series) -> PolarsResult<Option<Series>> {
    let series_struct_array: &StructChunked = s.struct_()?;
    let fields = series_struct_array.fields();
    let topics_data_sig = extract_log_fields(fields)?;

    let udf_output: StringChunked = topics_data_sig
        .into_iter()
        .map(|(topics, data, sig)| {
            decode(sig, topics, data)
                .map(|event| format!("{:?}; {:?}; {}", event.event_values, event.event_keys, event.event_json))
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
        .map(|(((((opt_topic0, opt_topic1), opt_topic2), opt_topic3), opt_data), opt_sig)| {
            let topics = vec![
                FixedBytes::from_slice(opt_topic0.unwrap_or(&zero_filled_topic)),
                FixedBytes::from_slice(opt_topic1.unwrap_or(&zero_filled_topic)),
                FixedBytes::from_slice(opt_topic2.unwrap_or(&zero_filled_topic)),
                FixedBytes::from_slice(opt_topic3.unwrap_or(&zero_filled_topic)),
            ];
            let data = opt_data.unwrap_or(&[]);
            let sig = opt_sig.unwrap_or("");

            Ok((topics, data, sig))
        })
        .collect()
}

fn decode(full_signature: &str, topics: Vec<FixedBytes<32>>, data: &[u8]) -> Result<ExtDecodedEvent, DecodeError> {
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
        event_json
    };
    
    Ok(extended_decoded_event)
}

fn parse_event_signature(full_signature: &str) -> Result<Event, DecodeError> {
    Event::parse(full_signature)
        .map_err(|e| DecodeError::DecodingError(e.to_string()))
}

fn decode_event_log(event: &Event, topics: Vec<FixedBytes<32>>, data: &[u8]) -> Result<DecodedEvent, DecodeError> {
    event.decode_log_parts(topics, data, false)
        .map_err(|e| DecodeError::DecodingError(e.to_string()))
}

fn map_event_sig_and_values(event_sig: &Event, event_values: &Vec<DynSolValue>) -> Result<Vec<StructuredEventParam>, DecodeError> {
    if event_values.len() != event_sig.inputs.len() {
        return Err(DecodeError::DecodingError(
            "Mismatch between signature length and returned params length".to_string()
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

fn print_system_memory() {
    let sys = System::new_all(); 
    println!("total memory: {} bytes", sys.total_memory());
    println!("used memory : {} bytes", sys.used_memory());
}

fn save_decoded_logs(mut df: DataFrame, path: &str) -> Result<(), DecodeError> {
    let mut file = File::create(Path::new(path))?;
    ParquetWriter::new(&mut file).finish(&mut df)?;
    Ok(())
}