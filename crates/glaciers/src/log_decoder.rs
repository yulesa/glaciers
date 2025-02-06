use alloy::dyn_abi::{DynSolValue, EventExt};
use alloy::json_abi::{Event, EventParam};
use alloy::primitives::FixedBytes;
use polars::prelude::*;
use thiserror::Error;

use crate::configger::get_config;
use crate::decoder::{DecoderError, StructuredParam};
use crate::utils;

#[derive(Error, Debug)]
pub enum LogDecoderError {
    #[error("Log decoder error: {0}")]
    DecodingError(String),
    #[error("Polars error: {0}")]
    PolarsError(#[from] PolarsError),
}

struct ExtDecodedEvent {
    event_values: Vec<String>,
    event_keys: Vec<String>,
    event_json: String,
}

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

//Construct a vec with topics, data and signature, and iterate through each, calling the decode function and mapping it to a 3 parts result string separated by ;
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

// translate [Series of topic0, Series of topic1, ..., Series of data, Series of sig] to Series of ([topic0, topic1, topic2, topic3], data, signature)
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

//use alloy functions to create the necessary structs, and call the decode_log_parts function
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