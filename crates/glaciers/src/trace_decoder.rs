//! Trace decoder module have the functions that are specific to decode traces.
//! 
//! This module provides functions to:
//! - Run through a DataFrame of traces calling the UDF (User Defined Function) each line
//! - A UDF to decode a single trace line into a 6 parts string separated by ;
//! - A function to extract from an array of series the input, output and signature
//! - A function to decode the trace line using the alloy library decode_inputs/decode_outputs function
//! - A function to map the decoded input/output parts into a StructuredParam for serialization
use alloy::dyn_abi::{DynSolValue, FunctionExt, JsonAbiExt};
use alloy::json_abi::Function;
use polars::prelude::*;
use thiserror::Error;

use crate::configger::get_config;
use crate::decoder::{DecoderError, StructuredParam};
use crate::utils;

/// Error types specific to trace decoding operations.
#[derive(Error, Debug)]
pub enum TraceDecoderError {
    #[error("Trace decoder error: {0}")]
    DecodingError(String),
    #[error("Polars error: {0}")]
    PolarsError(#[from] PolarsError),   
}

/// Internal structure to hold each part of the decoded function
struct ExtDecodedFunction {
    input_values: Vec<String>,
    input_keys: Vec<String>,
    input_json: String,
    output_values: Vec<String>,
    output_keys: Vec<String>, 
    output_json: String,
}

/// Decodes EVM transaction traces in a DataFrame and decodes both the input
/// and output data using the provided function signatures.
///
/// # Arguments
/// * `df` - Input DataFrame containing raw trace data and matching function signatures
///
/// # Returns
/// If successful, a DataFrame with decoded trace data including:
///   - input_values: Array of decoded input parameter values
///   - input_keys: Array of input parameter names
///   - input_json: JSON string representation of decoded inputs
///   - output_values: Array of decoded output parameter values
///   - output_keys: Array of output parameter names  
///   - output_json: JSON string representation of decoded outputs
///
/// # Notes
/// The output format (binary/hex) of some columns is determined by configuration
pub fn polars_decode_traces(df: DataFrame) -> Result<DataFrame, DecoderError> {
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

    Ok(if get_config().decoder.output_hex_string_encoding {
        utils::binary_columns_to_hex_string(decoded_df)?
    } else {
        decoded_df
    })
}

/// UDF (User Defined Function) for decoding individual traces entries.
///
/// # Arguments
/// * `s` - Series containing struct arrays of input, output and signature
///
/// # Returns
/// If successful, a Series containing decoded trace in a string format, separated by ;
///   "input_values";"input_keys";"input_json";"output_values";"output_keys";"output_json"
///
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

/// Extracts each trace field necessary for decoding from an array of Series.
/// Translate [Series of input, Series of output, Series of signature] to Series of (input, output, signature)

/// # Arguments
/// * `fields` - Slice of Series containing input, output and signature
///
/// # Returns
/// If successful, a vector (with items for each row) of tuples containing 3 values:
///   - Vector of input as byte slice
///   - Vector of output as byte slice
///   - Event signature as string
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

/// Decodes a single transaction trace using Alloy's ABI decoding functions.
///
/// # Arguments
/// * `input` - Raw input data as bytes
/// * `output` - Raw output data as bytes  
/// * `full_signature` - Function signature string
///
/// # Returns
/// If successful, a struct containing 6 values:
///   - input_values: JSON string of decoded input parameter values
///   - input_keys: JSON string of input parameter names
///   - input_json: JSON string representation of the decoded inputs
///   - output_values: JSON string of decoded output parameter values
///   - output_keys: JSON string of output parameter names
///   - output_json: JSON string representation of the decoded outputs
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

/// Maps function signature parameters names to their corresponding decoded values.
/// This function is necessary because the source of param values (output of decode_input/decode_output) 
/// is different from the source of param names (Signature - Function Object), and we want to keep them in the same order.
///
/// # Arguments
/// * `params` - Slice of function parameters from the ABI
/// * `values` - Vector of decoded parameter values
///
/// # Returns
/// If successful, a vector of StructuredParam (each item of the log_json)
fn map_function_params(
    params: &[alloy::json_abi::Param],
    values: &[DynSolValue],
) -> Result<Vec<StructuredParam>, TraceDecoderError> {
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
        let function_param = StructuredParam {
            name: param.name.clone(),
            index: i as u32,
            value_type: param.ty.to_string(),
            value: str_value.to_string().unwrap_or_else(|| "None".to_string()),
        };
        structured_params.push(function_param);
    }

    Ok(structured_params)
}