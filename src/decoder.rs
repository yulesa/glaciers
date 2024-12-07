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
struct Param {
    name: String,
    index: u32,
    value_type: String,
    value: String,
}

struct StringDynSolValue(DynSolValue);

impl StringDynSolValue {
    pub fn to_string_representation(&self) -> Option<String> {
        match &self.0 {
            DynSolValue::Bool(b) => Some(b.to_string()),
            DynSolValue::Int(i, _) => Some(i.to_string()),
            DynSolValue::Uint(u, _) => Some(u.to_string()),
            DynSolValue::FixedBytes(w, _) => Some(format!("0x{}", w.to_string())),
            DynSolValue::Address(a) => Some(a.to_string()),
            DynSolValue::Function(f) => Some(f.to_string()),
            DynSolValue::Bytes(b) => Some(format!("0x{}", String::from_utf8(b.to_vec()).expect("Our bytes should be valid utf8"))),
            DynSolValue::String(s) => Some(s.clone()),
            DynSolValue::Array(arr) => Some(format!(
                "[{}]", 
                arr.iter()
                    .filter_map(|v| StringDynSolValue::from(v.clone()).to_string_representation())
                    .collect::<Vec<_>>()
                    .join(", ")
            )),
            DynSolValue::FixedArray(arr) => Some(format!(
                "[{}]", 
                arr.iter()
                    .filter_map(|v|  StringDynSolValue::from(v.clone()).to_string_representation())
                    .collect::<Vec<_>>()
                    .join(", ")
            )),
            DynSolValue::Tuple(tuple) => Some(format!(
                "({})", 
                tuple.iter()
                    .filter_map(|v|  StringDynSolValue::from(v.clone()).to_string_representation())
                    .collect::<Vec<_>>()
                    .join(", ")
            ))
        }
    }
}

// Conversion methods
impl From<DynSolValue> for StringDynSolValue {
    fn from(value: DynSolValue) -> Self {
        StringDynSolValue(value)
    }
}

pub fn decode_logs(df: DataFrame) -> Result<DataFrame, DecodeError> {
    df.lazy()
        .with_columns([
            as_struct(vec![col("topic0"), col("topic1"), col("topic2"), col("topic3"), col("data"), col("full_signature")])
                .apply(
                    |s| decode_log_batch(s),
                    GetOutput::from_type(DataType::String),
                )
                .alias("decoded_log"),
        ])
        .collect()
        .map_err(DecodeError::from)
}

fn decode_log_batch(s: Series) -> PolarsResult<Option<Series>> {
    let ca = s.struct_()?;
    let fields = ca.fields();
    
    let ca_topic0 = fields[0].binary()?;
    let ca_topic1 = fields[1].binary()?;
    let ca_topic2 = fields[2].binary()?;
    let ca_topic3 = fields[3].binary()?;
    let ca_data = fields[4].binary()?;
    let ca_sig = fields[5].str()?;

    let out: StringChunked = ca_topic0
        .into_iter()
        .zip(ca_topic1.into_iter())
        .zip(ca_topic2.into_iter())
        .zip(ca_topic3.into_iter())
        .zip(ca_data.into_iter())
        .zip(ca_sig.into_iter())
        .map(|(((((opt_topic0, opt_topic1), opt_topic2), opt_topic3), opt_data), opt_sig)| {
            let zero_filled_topic = vec![0u8; 32];
            let topics = vec![
                FixedBytes::from_slice(opt_topic0.unwrap_or(&zero_filled_topic)),
                FixedBytes::from_slice(opt_topic1.unwrap_or(&zero_filled_topic)),
                FixedBytes::from_slice(opt_topic2.unwrap_or(&zero_filled_topic)),
                FixedBytes::from_slice(opt_topic3.unwrap_or(&zero_filled_topic)),
            ];
            let data = opt_data.unwrap_or(&[]);
            let sig = opt_sig.unwrap_or("");

            decode(sig, topics, data)
                .map(|decoded_log| format!("{:?}, {:?}", decoded_log.indexed, decoded_log.body))
                .ok()
        })
        .collect();

    Ok(Some(out.into_series()))
}

fn decode(full_signature: &str, topics: Vec<FixedBytes<32>>, data: &[u8]) -> Result<DecodedEvent, DecodeError> {
    let event = Event::parse(full_signature)
        .map_err(|e| DecodeError::DecodingError(e.to_string()))?;
    let decoded_event = event.decode_log_parts(topics, data, false)
        .map_err(|e| DecodeError::DecodingError(e.to_string()))?;

    let mut params:Vec<Param> = vec![];
    let mut params_values: Vec<DynSolValue> = decoded_event.indexed.clone();
    params_values.extend(decoded_event.body.clone());
    if params_values.len() != event.inputs.len() {
        return Err(DecodeError::DecodingError("Mismatch between signature lenght and retuned params lenght".to_string()));
    }
    if params_values.len() > 0 {
        for (i, input) in event.inputs.iter().enumerate() {
            let str_value = StringDynSolValue::from(params_values[i].clone());
            let value = str_value.to_string_representation().unwrap_or("None".to_string());
            let value_type = input.ty.to_string();
            let name = input.name.clone();
            let index = i as u32;
            let p = Param {name, index, value_type, value};
            params.push(p);
        }
    }
    let params_keys_mapping: Vec<String> = params.iter().map(|p| p.name.clone()).collect();
    println!("params_keys_mapping: {:?}", params_keys_mapping);
    let params_mapping = parse_params_to_json(&params);
    println!("Params JSON: {}", params_mapping);
    

    Ok(decoded_event)
}

fn parse_params_to_json(params: &Vec<Param>) -> String {
    // Directly serialize the entire Vec of Param structs
    serde_json::to_string(params)
        .unwrap_or_else(|_| "[]".to_string())
}