use alloy::dyn_abi::{DecodedEvent, DynSolValue, EventExt};
use alloy::json_abi::Event;
use alloy::primitives::FixedBytes;
use polars::prelude::*;
use thiserror::Error;
use serde::Serialize;

#[derive(Error, Debug)]
pub enum DecodeError {
    #[error("Decoding error: {0}")]
    DecodingError(String),
    #[error("Polars error: {0}")]
    PolarsError(#[from] PolarsError),
}

#[derive(Debug, Serialize)]
struct EventParam  {
    name: String,
    index: u32,
    value_type: String,
    value: String,
}

struct ExtendedDecodedEvent {
     /// The indexed values, in order.
     pub indexed: Vec<DynSolValue>,
     /// The un-indexed values, in order.
     pub body: Vec<DynSolValue>,
     pub params_keys_mapping: Vec<String>,
     pub params_mapping: String
}

//Wrapper type around DynSolValue, to implement a conversion to String function.
struct StringifiedValue(DynSolValue);

impl StringifiedValue {
    pub fn convert(&self) -> Option<String> {
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
                    .filter_map(|v| Self::from(v.clone()).convert())
                    .collect::<Vec<_>>()
                    .join(", ")
            )),
            DynSolValue::FixedArray(arr) => Some(format!(
                "[{}]", 
                arr.iter()
                    .filter_map(|v| Self::from(v.clone()).convert())
                    .collect::<Vec<_>>()
                    .join(", ")
            )),
            DynSolValue::Tuple(tuple) => Some(format!(
                "({})", 
                tuple.iter()
                    .filter_map(|v| Self::from(v.clone()).convert())
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

pub fn decode_logs(df: DataFrame) -> Result<DataFrame, DecodeError> {
    df.lazy()
        .with_columns([
            as_struct(vec![col("topic0"), col("topic1"), col("topic2"), col("topic3"), col("data"), col("full_signature")])
                .map(decode_log_batch, GetOutput::from_type(DataType::String))
                .alias("decoded_log"),
        ])
        .collect()
        .map_err(DecodeError::from)
}

fn decode_log_batch(s: Series) -> PolarsResult<Option<Series>> {
    let series_struct_array: &StructChunked = s.struct_()?;
    let fields = series_struct_array.fields();
    
    let topics_data = extract_log_fields(fields)?;

    let out: StringChunked = topics_data
        .into_iter()
        .map(|(topics, data, sig)| {
            decode(sig, topics, data)
                .map(|decoded_log| format!("{:?}, {:?}, {:?}, {}", decoded_log.indexed, decoded_log.body, decoded_log.params_keys_mapping, decoded_log.params_mapping))
                .ok()
        })
        .collect();

    println!("Decoded log: {:?}", out.clone().into_series());

    Ok(Some(out.into_series()))
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

fn decode(full_signature: &str, topics: Vec<FixedBytes<32>>, data: &[u8]) -> Result<ExtendedDecodedEvent, DecodeError> {
    let event = parse_event_signature(full_signature)?;
    let decoded_event = decode_event_log(&event, topics, data)?;
    let params = extract_event_params(&event, &decoded_event)?;
    let params_keys_mapping: Vec<String> = params.iter().map(|p| p.name.clone()).collect();
    let params_mapping = serde_json::to_string(&params).unwrap_or_else(|_| "[]".to_string());
    
    let extended_decoded_event = ExtendedDecodedEvent {
        indexed: decoded_event.indexed,
        body: decoded_event.body,
        params_keys_mapping,
        params_mapping
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

fn extract_event_params(event: &Event, decoded_event: &DecodedEvent) -> Result<Vec<EventParam>, DecodeError> {
    let mut params_values: Vec<DynSolValue> = decoded_event.indexed.clone();
    params_values.extend(decoded_event.body.clone());

    if params_values.len() != event.inputs.len() {
        return Err(DecodeError::DecodingError(
            "Mismatch between signature length and returned params length".to_string()
        ));
    }

    let mut params = Vec::new();
    for (i, input) in event.inputs.iter().enumerate() {
        let str_value = StringifiedValue::from(params_values[i].clone());
        let param = EventParam {
            name: input.name.clone(),
            index: i as u32,
            value_type: input.ty.to_string(),
            value: str_value.convert().unwrap_or_else(|| "None".to_string()),
        };
        params.push(param);
    }

    Ok(params)
}