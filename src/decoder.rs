use alloy::dyn_abi::{DecodedEvent, EventExt};
use alloy::json_abi::Event;
use alloy::primitives::FixedBytes;
use polars::prelude::*;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum DecodeError {
    #[error("Decoding error: {0}")]
    DecodingError(String),
    #[error("Polars error: {0}")]
    PolarsError(#[from] PolarsError),
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
    event.decode_log_parts(topics, data, false)
        .map_err(|e| DecodeError::DecodingError(e.to_string()))
}
