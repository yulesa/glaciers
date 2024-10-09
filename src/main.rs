use alloy::{dyn_abi::EventExt, json_abi::Event, primitives::FixedBytes};
use decoding::abi_reader::{self, create_dataframe_from_event_rows};
use polars::prelude::*;
use thiserror::Error;

#[derive(Error, Debug)]
enum AppError {
    #[error("DataFrame error: {0}")]
    DataFrameError(String),
    #[error("Decoding error: {0}")]
    DecodingError(String),
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Polars error: {0}")]
    PolarsError(#[from] PolarsError),
}

fn main() -> Result<(), AppError> {
    let abi_list_df = abi_reader::read_abis()?;
    let ethereum_logs_df = load_ethereum_logs()?;
    

    Ok(())

}

// fn old_main() -> Result<(), AppError> {
//     let abi_list = abi_reader::read_abis();
//     let approval_event = abi_list.first()
//         .ok_or_else(|| AppError::DataFrameError("ABI list is empty".to_string()))?;
//     let approval_event = Event::parse(approval_event.full_signature.as_str())
//         .map_err(|e| AppError::DecodingError(e.to_string()))?;

//     let ethereum_logs_df = load_ethereum_logs()?;
//     let (topics, data) = extract_log_data(&ethereum_logs_df, 7)?;

    // let decoded_log = approval_event.decode_log_parts(topics, &data, false)
//         .map_err(|e| AppError::DecodingError(e.to_string()))?;
    
//     println!("Decoded log: {:#?}", decoded_log);
//     Ok(())
// }

fn load_ethereum_logs() -> Result<DataFrame, AppError> {
    LazyFrame::scan_parquet("data/ethereum__logs__*.parquet", Default::default())?
        .collect()
        .map_err(AppError::from)
}

fn extract_log_data(df: &DataFrame, row_index: usize) -> Result<(Vec<FixedBytes<32>>, Vec<u8>), AppError> {
    let binding = df.select(&["topic0", "topic1", "topic2", "topic3", "data"])?;
    let row = binding
        .get(row_index)
        .ok_or_else(|| AppError::DataFrameError("Row index out of bounds".to_string()))?;

    let topics = row.iter()
    .take(4)
    .filter_map(|s| match s {
        AnyValue::Binary(b) => Some(FixedBytes::from_slice(b)),
        _ => None,
    })
    .collect();

    let data: Vec<u8> = match row[0] {
        AnyValue::Binary(b) => b.to_vec(),
        _ => return Err(AppError::DataFrameError("Expected binary data".to_string())),
    };

    Ok((topics, data))
}