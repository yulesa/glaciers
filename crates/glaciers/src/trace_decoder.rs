use alloy::dyn_abi::{DynSolValue, FunctionExt};
use alloy::json_abi::Function;
use alloy::primitives::Bytes;
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

pub async fn decode_trace_file(
    trace_file_path: String,
    abi_df_path: String,
) -> Result<DataFrame, TraceDecoderError> {
    let trace_file_path = PathBuf::from(trace_file_path);
    let abi_df_path = PathBuf::from(abi_df_path);

    let trace_df = utils::read_df_file(&trace_file_path)?;
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

    let mut alias_exprs: Vec<Expr> = input_schema_alias.as_array()
        .iter()
        .map(|alias| col(alias.as_str()).alias(alias.as_str()))
        .collect();
    alias_exprs.push(col("full_signature").alias("full_signature"));
    
    println!("alias_exprs: {:?}", alias_exprs);
    // Use polars to decode each row
    let decoded_df = df
        .lazy()
        .with_columns([as_struct(alias_exprs)
            .map(decode_trace_udf, GetOutput::from_type(DataType::String))
            .alias("decoded_trace")
        ])
        // .with_columns([
        //     col("decoded_trace")
        //         .str()
        //         .split(lit(";"))
        //         .list()
        //         .get(lit(0))
        //         .alias("input_values"),
        //     col("decoded_trace")
        //         .str()
        //         .split(lit(";"))
        //         .list()
        //         .get(lit(1))
        //         .alias("input_keys"),
        //     col("decoded_trace")
        //         .str()
        //         .split(lit(";"))
        //         .list()
        //         .get(lit(2))
        //         .alias("input_json"),
        //     col("decoded_trace")
        //         .str()
        //         .split(lit(";"))
        //         .list()
        //         .get(lit(3))
        //         .alias("output_values"),
        //     col("decoded_trace")
        //         .str()
        //         .split(lit(";"))
        //         .list()
        //         .get(lit(4))
        //         .alias("output_keys"),
        //     col("decoded_trace")
        //         .str()
        //         .split(lit(";"))
        //         .list()
        //         .get(lit(5))
        //         .alias("output_json")
        // ])
        // .select([col("*").exclude(["decoded_trace"])])
        .collect()?;

    // Ok(if get_config().trace_decoder.output_hex_string_encoding {
    //     utils::binary_columns_to_hex_string(decoded_df)?
    // } else {
    //     decoded_df
    // })
    utils::write_df_file(&mut decoded_df.clone(), Path::new("decoded_trace.parquet"))?;
    println!("decoded_df: {:?}", decoded_df);
    Ok(decoded_df)

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

fn decode_trace_udf(s: Series) -> PolarsResult<Option<Series>> {
    println!("s: {:?}", s);
    Ok(None)
    // let series_struct_array: &StructChunked = s.struct_()?;
    // let fields = series_struct_array.fields();
    
    // let traces = extract_trace_fields(&fields)?;
}

// fn decode_trace_udf(s: Series) -> PolarsResult<Option<Series>> {
//     let series_struct_array: &StructChunked = s.struct_()?;
//     let fields = series_struct_array.fields();
    
//     let traces = extract_trace_fields(&fields)?;

//     let udf_output: StringChunked = traces
//         .into_iter()
//         .map(|(input, output, func_sig)| {
//             decode(func_sig, input, output)
//                 .map(|func| {
//                     format!(
//                         "{:?}; {:?}; {}; {:?}; {:?}; {}", 
//                         func.input_values,
//                         func.input_keys,
//                         func.input_json,
//                         func.output_values,
//                         func.output_keys,
//                         func.output_json
//                     )
//                 })
//                 .ok()
//         })
//         .collect();

//     Ok(Some(udf_output.into_series()))
// }

// fn extract_trace_fields(fields: &[Series]) -> PolarsResult<Vec<(&[u8], &[u8], &str)>> {
//     let fields_input = fields[0].binary()?;
//     let fields_output = fields[1].binary()?;
//     let fields_sig = fields[2].str()?;

//     fields_input
//         .into_iter()
//         .zip(fields_output.into_iter())
//         .zip(fields_sig.into_iter())
//         .map(|((opt_input, opt_output), opt_sig)| {
//             let input = opt_input.unwrap_or(&[]);
//             let output = opt_output.unwrap_or(&[]);
//             let sig = opt_sig.unwrap_or("");

//             Ok((input, output, sig))
//         })
//         .collect()
// }

// fn decode(
//     full_signature: &str,
//     input: &[u8],
//     output: &[u8],
// ) -> Result<ExtDecodedFunction, TraceDecoderError> {
//     let function = Function::parse(full_signature)
//         .map_err(|e| TraceDecoderError::DecodingError(e.to_string()))?;

//     // Decode input data
//     let decoded_input = function
//         .abi_decode_input(input, true)
//         .map_err(|e| TraceDecoderError::DecodingError(e.to_string()))?;

//     // Decode output data 
//     let decoded_output = function
//         .abi_decode_output(output, true)
//         .map_err(|e| TraceDecoderError::DecodingError(e.to_string()))?;

//     // Map function inputs and values to structured format
//     let structured_inputs = map_function_params(&function.inputs, &decoded_input)?;
//     let structured_outputs = map_function_params(&function.outputs, &decoded_output)?;

//     // Extract keys (param names)
//     let input_keys: Vec<String> = structured_inputs.iter().map(|p| p.name.clone()).collect();
//     let output_keys: Vec<String> = structured_outputs.iter().map(|p| p.name.clone()).collect();

//     // Convert to JSON
//     let input_json = serde_json::to_string(&structured_inputs)
//         .unwrap_or_else(|_| "[]".to_string())
//         .trim()
//         .to_string();
//     let output_json = serde_json::to_string(&structured_outputs)
//         .unwrap_or_else(|_| "[]".to_string())
//         .trim()
//         .to_string();

//     // Convert values to strings
//     let input_values: Vec<String> = decoded_input
//         .iter()
//         .map(|d| utils::StrDynSolValue::from(d.clone()).to_string().unwrap_or("None".to_string()))
//         .collect();
//     let output_values: Vec<String> = decoded_output
//         .iter()
//         .map(|d| utils::StrDynSolValue::from(d.clone()).to_string().unwrap_or("None".to_string()))
//         .collect();

//     Ok(ExtDecodedFunction {
//         input_values,
//         input_keys,
//         input_json,
//         output_values,
//         output_keys,
//         output_json,
//     })
// }

// fn map_function_params(
//     params: &[alloy::json_abi::Param],
//     values: &[DynSolValue],
// ) -> Result<Vec<StructuredFunctionParam>, TraceDecoderError> {
//     if values.len() != params.len() {
//         return Err(TraceDecoderError::DecodingError(
//             "Mismatch between params length and returned values length".to_string(),
//         ));
//     }

//     let mut structured_params = Vec::new();
//     for (i, param) in params.iter().enumerate() {
//         let str_value = utils::StrDynSolValue::from(values[i].clone());
//         let function_param = StructuredFunctionParam {
//             name: param.name.clone(),
//             index: i as u32,
//             value_type: param.ty.to_string(),
//             value: str_value.to_string().unwrap_or_else(|| "None".to_string()),
//         };
//         structured_params.push(function_param);
//     }

//     Ok(structured_params)
// }