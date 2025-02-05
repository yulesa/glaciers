use alloy::dyn_abi::{DynSolValue, FunctionExt, JsonAbiExt};
use alloy::json_abi::Function;
use chrono::Local;
use polars::prelude::*;
use serde::Serialize;
use std::fs;
use std::path::{Path, PathBuf};
use thiserror::Error;
use tokio::sync::{mpsc, Mutex, Semaphore};
use tokio::task;
use std::sync::Arc;

use crate::configger::{self, get_config};
use crate::matcher;
use crate::utils;

#[derive(Error, Debug)]
pub enum TraceDecoderError {
    #[error("Trace decoder decoding error: {0}")]
    DecodingError(String),
    #[error("Polars error: {0}")]
    PolarsError(#[from] PolarsError),
    #[error("Matcher error: {0}")]
    MatcherError(#[from] matcher::MatcherError),
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Join error: {0}")]
    JoinError(#[from] tokio::task::JoinError),
    #[error("Serde json error: {0}")]
    SerdeError(#[from] serde_json::Error)
}

#[derive(Debug, Serialize)]
struct StructuredFunctionParam {
    name: String,
    index: u32,
    value_type: String,
    value: String,
}

struct ExtDecodedFunction {
    input_values: Vec<String>,
    input_keys: Vec<String>,
    input_json: String,
    output_values: Vec<String>,
    output_keys: Vec<String>, 
    output_json: String,
}

pub async fn decode_trace_folder(
    trace_folder_path: String,
    abi_df_path: String,
) -> Result<(), TraceDecoderError> {

    // Collect trace files' paths from trace_folder_path
    let trace_files: Vec<PathBuf> = fs::read_dir(trace_folder_path)?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .collect();

    // Create a semaphore with MAX_CONCURRENT_FILES_DECODING permits
    let semaphore = Arc::new(Semaphore::new(get_config().trace_decoder.max_concurrent_files_decoding));
    // Create a vector to hold our join handles
    let mut handles = Vec::new();

    // Spawn a task for each trace file
    for trace_file_path in trace_files {
        // skip PathBuf belonging to folders
        if trace_file_path.is_dir() {
            continue
        }
        // Clone the DataFrame and semafore for each task
        let abi_df_path = abi_df_path.clone();
        let semaphore = semaphore.clone();

        // Spawn a tokio task for each file
        let handle = task::spawn(async move {
            // Acquire a permit before processing
            let _permit = semaphore.acquire().await.unwrap();
            decode_trace_file(trace_file_path, abi_df_path).await
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

pub async fn decode_trace_file(
    trace_file_path: PathBuf,
    abi_df_path: String,
) -> Result<DataFrame, TraceDecoderError> {
    let file_path_str = trace_file_path.to_string_lossy().into_owned();
    let file_name = trace_file_path
        .file_name()
        .unwrap()
        .to_string_lossy()
        .into_owned();
    let mut file_folder_path = trace_file_path
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

    let ethereum_traces_df = utils::read_df_file(&trace_file_path)?;
    let ethereum_traces_df = utils::hex_string_columns_to_binary(ethereum_traces_df)?;
    let mut decoded_df = decode_trace_df(ethereum_traces_df, abi_df_path).await?;

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
        if file_name.contains("traces") {
            file_name.replace("traces", "decoded_traces")
        } else {
            format!("decoded_traces_{}", file_name)
        }
    );

    let save_path: &Path = Path::new(&save_path);

    if let Some(parent) = save_path.parent() {
        // create folder if it doesn't exist
        fs::create_dir_all(parent.to_string_lossy().into_owned())?;
    }
    let save_path= save_path.with_extension(get_config().trace_decoder.output_file_format);
    println!(
        "[{}] Saving decoded traces to: {:?}",
        Local::now().format("%Y-%m-%d %H:%M:%S"),
        save_path
    );
    utils::write_df_file(&mut decoded_df, &save_path)?;

    Ok(decoded_df)
}

pub async fn decode_trace_df (
    trace_df: DataFrame,
    abi_df_path: String,
) -> Result<DataFrame, TraceDecoderError> {
    let abi_df_path = Path::new(&abi_df_path);
    let abi_df = utils::read_df_file(&abi_df_path)?;

    decode_trace_df_with_abi_df(trace_df, abi_df).await
}

pub async fn decode_trace_df_with_abi_df(
    trace_df: DataFrame,
    abi_df: DataFrame,
) -> Result<DataFrame, TraceDecoderError> {
    // Convert hash and address columns to binary if they aren't already
    let abi_df = utils::abi_df_hex_string_columns_to_binary(abi_df)?;

    // perform matching
    let matched_df = match get_config().trace_decoder.trace_algorithm {
        configger::TraceAlgorithm::FourBytesAddress => matcher::match_traces_by_4bytes_address(trace_df, abi_df)?,
        // TODO: add match_traces_by_4bytes
        configger::TraceAlgorithm::FourBytes => matcher::match_traces_by_4bytes_address(trace_df, abi_df)?
    };

    // Split traces files in chunk, decode traces, collected and union results and save in the decoded folder
    decode_traces(matched_df).await
}

pub async fn decode_traces(df: DataFrame) -> Result<DataFrame, TraceDecoderError> {
      // Create a semaphore with MAX_THREAD_NUMBER permits
      let semaphore = Arc::new(Semaphore::new(get_config().trace_decoder.max_chunk_threads_per_file));
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
          let end = (i + get_config().trace_decoder.decoded_chunk_size).min(total_height);
          let chunk_df = df.slice(i as i64, end - i);
  
          let sem_clone = semaphore.clone();
          let tx_clone = tx.clone();
          let collected_dfs_clone = collected_dfs.clone();
          let handle = task::spawn(async move {
  
              let _permit = sem_clone.acquire().await;
              //Use polars to iterate through each row and decode, communicate through channel the result.
              match polars_decode_trace(chunk_df) {
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
                      tx_clone.send(Err(e)).await.expect("Failed. polars_decode_trace_df returned an error");
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

pub fn polars_decode_trace(df: DataFrame) -> Result<DataFrame, TraceDecoderError> {
    let input_schema_alias = get_config().trace_decoder.trace_schema.trace_alias;

    // using the alias to select columns that will be used in the decode_trace_udf
    // as_array() is excluding the selector and address column because it is not used in the trace decoding
    let mut alias_exprs: Vec<Expr> = input_schema_alias.as_array()
        .iter()
        .map(|alias| col(alias.as_str()).alias(alias.as_str()))
        .collect();
    alias_exprs.push(col("full_signature").alias("full_signature"));
    
    // as_struct() passes the selected columns to the decode_trace_udf and returns a column decoded_trace of type String
    // decoded_trace column is then split into 6 columns separated by the ; character
    let decoded_df = df
        .lazy()
        .with_columns([as_struct(alias_exprs)
            .map(decode_trace_udf, GetOutput::from_type(DataType::String))
            .alias("decoded_trace")
        ])
        .with_columns([
            col("decoded_trace")
                .str()
                .split(lit(";"))
                .list()
                .get(lit(0))
                .alias("input_values"),
            col("decoded_trace")
                .str()
                .split(lit(";"))
                .list()
                .get(lit(1))
                .alias("input_keys"),
            col("decoded_trace")
                .str()
                .split(lit(";"))
                .list()
                .get(lit(2))
                .alias("input_json"),
            col("decoded_trace")
                .str()
                .split(lit(";"))
                .list()
                .get(lit(3))
                .alias("output_values"),
            col("decoded_trace")
                .str()
                .split(lit(";"))
                .list()
                .get(lit(4))
                .alias("output_keys"),
            col("decoded_trace")
                .str()
                .split(lit(";"))
                .list()
                .get(lit(5))
                .alias("output_json")
        ])
        .select([col("*").exclude(["decoded_trace"])])
        .collect()?;

    Ok(if get_config().trace_decoder.output_hex_string_encoding {
        utils::binary_columns_to_hex_string(decoded_df)?
    } else {
        decoded_df
    })
}

// Helper function to union DataFrames
async fn union_dataframes(dfs: Vec<DataFrame>) -> Result<DataFrame, TraceDecoderError> {
    // If only one DataFrame, return it directly
    if dfs.len() == 1 {
        return Ok(dfs[0].clone());
    }

    // Use Polars' vertical concatenation with union semantics
    let lazy_dfs: Vec<LazyFrame> = dfs.into_iter().map(|df| df.lazy()).collect();
    let unioned_df = concat(&lazy_dfs, UnionArgs::default())?.collect()?;

    Ok(unioned_df)
}

//Construct a vec with input, output and signature, and iterate through each, calling the decode function and mapping it to a 6 parts result string separated by ;
fn decode_trace_udf(s: Series) -> PolarsResult<Option<Series>> {
    let series_struct_array: &StructChunked = s.struct_()?;
    let fields = series_struct_array.fields();

    //extract input, output and signature from the df struct arrays
    let traces_data = extract_trace_fields(&fields)?;

    //iterate through each row value, calling the decode function and mapping it to a 6 parts result string separated by ;
    let udf_output: StringChunked = traces_data
        .into_iter()
        .map(|(input, output, func_sig)| {
            decode(input, output, func_sig)
                .map(|func| {
                    format!(
                        "{:?}; {:?}; {}; {:?}; {:?}; {}", 
                        func.input_values,
                        func.input_keys,
                        func.input_json,
                        func.output_values,
                        func.output_keys,
                        func.output_json
                    )
                })
                .ok()
        })
        .collect();

    Ok(Some(udf_output.into_series()))
}

// translate [Series of input, Series of output, Series of signature] to Series of (input, output, signature)
fn extract_trace_fields(fields: &[Series]) -> PolarsResult<Vec<(&[u8], &[u8], &str)>> {
    //extract input, output and signature from the df struct arrays
    let fields_input = fields[0].binary()?;
    let fields_output = fields[1].binary()?;
    let fields_sig = fields[2].str()?;

    //iterate through each row value, and map it to a tuple of input, output and signature
    fields_input
        .into_iter()
        .zip(fields_output.into_iter())
        .zip(fields_sig.into_iter())
        .map(|((opt_input, opt_output), opt_sig)| {
            let inputs = opt_input.unwrap_or(&[]);
            let outputs = opt_output.unwrap_or(&[]);
            let sigs = opt_sig.unwrap_or("");

            Ok((inputs, outputs, sigs))
        }
        )
        .collect()
}

//use alloy functions to create the necessary structs, and call the decode function
fn decode(
    input: &[u8],
    output: &[u8],
    full_signature: &str,
) -> Result<ExtDecodedFunction, TraceDecoderError> {
    //parse the full signature to create the function object
    let function_obj = Function::parse(full_signature)
        .map_err(|e| TraceDecoderError::DecodingError(e.to_string()))?;

    // Decode input data calling the alloy abi_decode_input function
    let decoded_input = function_obj
        .abi_decode_input(input, true)
        .map_err(|e| TraceDecoderError::DecodingError(e.to_string()))?;

    // Decode output data calling the alloy abi_decode_output function  
    let decoded_output = function_obj
        .abi_decode_output(output, true)
        .map_err(|e| TraceDecoderError::DecodingError(e.to_string()))?;

    // Map function inputs and values to structured format
    let structured_inputs = map_function_params(&function_obj.inputs, &decoded_input)?;
    let structured_outputs = map_function_params(&function_obj.outputs, &decoded_output)?;

    // Extract keys (param names)
    let input_keys: Vec<String> = structured_inputs.iter().map(|p| p.name.clone()).collect();
    let output_keys: Vec<String> = structured_outputs.iter().map(|p| p.name.clone()).collect();

    // Convert to JSON
    let input_json = serde_json::to_string(&structured_inputs)
        .unwrap_or_else(|_| "[]".to_string())
        .trim()
        .to_string();
    let output_json = serde_json::to_string(&structured_outputs)
        .unwrap_or_else(|_| "[]".to_string())
        .trim()
        .to_string();

    // Convert values to strings
    let input_values: Vec<String> = decoded_input
        .iter()
        .map(|d| utils::StrDynSolValue::from(d.clone()).to_string().unwrap_or("None".to_string()))
        .collect();
    let output_values: Vec<String> = decoded_output
        .iter()
        .map(|d| utils::StrDynSolValue::from(d.clone()).to_string().unwrap_or("None".to_string()))
        .collect();

    Ok(ExtDecodedFunction {
        input_values,
        input_keys,
        input_json,
        output_values,
        output_keys,
        output_json,
    })
}

fn map_function_params(
    params: &[alloy::json_abi::Param],
    values: &[DynSolValue],
) -> Result<Vec<StructuredFunctionParam>, TraceDecoderError> {
    // This error might be impossible, because it would make abi_decode_input/output fail before.
    if values.len() != params.len() {
        return Err(TraceDecoderError::DecodingError(
            "Mismatch between params length and returned values length".to_string(),
        ));
    }

    //iterate through each param, and map it to a StructuredFunctionParam
    let mut structured_params = Vec::new();
    for (i, param) in params.iter().enumerate() {
        let str_value = utils::StrDynSolValue::from(values[i].clone());
        let function_param = StructuredFunctionParam {
            name: param.name.clone(),
            index: i as u32,
            value_type: param.ty.to_string(),
            value: str_value.to_string().unwrap_or_else(|| "None".to_string()),
        };
        structured_params.push(function_param);
    }

    Ok(structured_params)
}