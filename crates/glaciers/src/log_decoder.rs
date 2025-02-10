//! Log decoder module have the functions that are specific to decode logs.
//! 
//! This module provides functions to:
//! - Run through a DataFrame of logs calling the UDF (User Defined Function) each line
//! - A UDF to decode a single log line into a 3 parts string separated by ;
//! - A function to extract from an array of series the topics, data and signature
//! - A function to decode the log line using the alloy library decode_log_parts function
//! - A function to map the decoded log parts into a StructuredParam for serialization
use alloy::dyn_abi::{DynSolValue, EventExt};
use alloy::json_abi::{Event, EventParam};
use alloy::primitives::FixedBytes;
use polars::prelude::*;
use thiserror::Error;

use crate::configger::get_config;
use crate::decoder::{DecoderError, StructuredParam};
use crate::utils;

/// Error types specific to log decoding operations.
#[derive(Error, Debug)]
pub enum LogDecoderError {
    #[error("Log decoder error: {0}")]
    DecodingError(String),
    #[error("Polars error: {0}")]
    PolarsError(#[from] PolarsError),
}

/// Internal structure to hold each part of the decoded event
struct ExtDecodedEvent {
    event_values: Vec<String>,
    event_keys: Vec<String>,
    event_json: String,
}

/// Decodes EVM logs in a DataFrame into human-readable format.
///
/// # Arguments
/// * `df` - Input DataFrame containing raw log data and a matching event item.
///
/// # Returns
/// If successful, a DataFrame with decoded log data including:
///   - event_values: Array of decoded parameter values
///   - event_keys: Array of parameter names
///   - event_json: JSON string representation of the decoded event
/// 
/// # Notes
/// The output format (binary/hex) of some columns is determined by configuration
pub fn polars_decode_logs(df: DataFrame) -> Result<DataFrame, DecoderError> {
    let input_schema_alias = get_config().log_decoder.log_schema.log_alias;

    // using the alias to select columns that will be used in the decode_log_udf
    // as_array() is excluding the address column because it is not used in the log decoding
    let mut alias_exprs: Vec<Expr> = input_schema_alias.as_array()
        .iter()
        .map(|alias| col(alias.as_str()).alias(alias.as_str()))
        .collect();
    alias_exprs.push(col("full_signature").alias("full_signature"));
    
    // as_struct() passes the selected columns to the decode_log_udf and returns a column decoded_log of type String
    // decoded_log column is then split into 3 columns separated by the ; character
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

/// UDF (User Defined Function) for decoding individual log entries.
///
/// # Arguments
/// * `s` - Series containing struct arrays of log topics, data and signature
///
/// # Returns
/// If successful, a Series containing decoded log in a string format, separated by ;
///   "event_values";"event_keys";"event_json"
fn decode_log_udf(s: Series) -> PolarsResult<Option<Series>> {
    let series_struct_array: &StructChunked = s.struct_()?;
    let fields = series_struct_array.fields();
    //extract topics, data and signature from the df struct arrays
    let topics_data_sig = extract_log_fields(&fields)?;

    //iterate through each row value, calling the decode function and mapping it to a 3 parts result string separated by ;
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

/// Extracts each log field necessary for decoding from an array of Series.
/// Translate [Series of topic0, Series of topic1, ..., Series of data, Series of sig] to Series of ([topic0, topic1, topic2, topic3], data, signature)

/// # Arguments
/// * `fields` - Slice of Series containing log topics, data and signature
///
/// # Returns
/// If successful, a vector (with items for each row) of tuples containing 3 values:
///   - Vector of topics as 32-byte fixed arrays
///   - Raw data as byte slice
///   - Event signature as string
fn extract_log_fields(fields: &[Series]) -> PolarsResult<Vec<(Vec<FixedBytes<32>>, &[u8], &str)>> {
    let zero_filled_topic = vec![0u8; 32];

    let fields_topic0 = fields[0].binary()?;
    let fields_topic1 = fields[1].binary()?;
    let fields_topic2 = fields[2].binary()?;
    let fields_topic3 = fields[3].binary()?;
    let fields_data = fields[4].binary()?;
    let fields_sig = fields[5].str()?;

    //iterate through each row value, and map it to a tuple of topics, data and signature
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

/// Decodes a single log entry using Alloy's decode_log_parts function, and maps the decoded log parts into a StructuredParam for serialization
///
/// # Arguments
/// * `full_signature` - Event signature string
/// * `topics` - Vector of event topics as 32-byte fixed arrays
/// * `data` - Raw event data as byte slice
///
/// # Returns
/// If successful, a struct containing 3 values:
///   - event_values: JSON string of decoded parameter values
///   - event_keys: JSON string of parameter names
///   - event_json: JSON string representation of the complete decoded event
fn decode(
    full_signature: &str,
    topics: Vec<FixedBytes<32>>,
    data: &[u8],
) -> Result<ExtDecodedEvent, LogDecoderError> {
    //parse the full signature to create the event object
    let event_obj = Event::parse(full_signature)
        .map_err(|e| LogDecoderError::DecodingError(e.to_string()))?;

    //decode the event calling the alloy decode_log_parts function
    let decoded_event = event_obj.decode_log_parts(topics, data, false)
        .map_err(|e| LogDecoderError::DecodingError(e.to_string()))?;

    // Store the indexed values in a vector
    let mut event_values: Vec<DynSolValue> = decoded_event.indexed.clone();
    // Extend the vector with the body(data) values
    event_values.extend(decoded_event.body.clone());

    let structured_event = map_event_sig_and_values(&event_obj, &event_values)?;
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

/// Maps event signature parameters names to their corresponding decoded values.
/// This function is necessary because the source of param values (output of decode_log_parts) 
/// is different from the source of param names (Signature - Event Object), and we want to keep them in the same order.
///
/// # Arguments
/// * `event_sig` - Reference to the parsed Event object
/// * `event_values` - Vector of decoded parameter values
///
/// # Returns
/// If successful, a vector of StructuredParam (each item of the log_json)
fn map_event_sig_and_values(
    event_sig: &Event,
    event_values: &Vec<DynSolValue>,
) -> Result<Vec<StructuredParam>, LogDecoderError> {
    // This error might be impossible, because it would make decode_log_parts fail before.
    if event_values.len() != event_sig.inputs.len() {
        return Err(LogDecoderError::DecodingError(
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

    let mut structured_event: Vec<StructuredParam> = Vec::new();
    for (i, input) in event_inputs.iter().enumerate() {
        let str_value = utils::StrDynSolValue::from(event_values[i].clone());
        // This is each item of event_json
        let event_param = StructuredParam {
            name: input.name.clone(),
            index: i as u32,
            value_type: input.ty.to_string(),
            value: str_value.to_string().unwrap_or_else(|| "None".to_string()),
        };
        structured_event.push(event_param);
    }

    Ok(structured_event)
}