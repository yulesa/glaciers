use std::{fs::File, path::Path};
use decoding::abi_reader::{self};
use decoding::decoder::decode_logs;
use polars::prelude::*;
use decoding::decoder::DecodeError;
use thiserror::Error;

#[derive(Error, Debug)]
enum AppError {
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Decode error: {0}")]
    DecodeError(#[from] DecodeError),
    #[error("Polars error: {0}")]
    PolarsError(#[from] polars::prelude::PolarsError),
}

fn main() -> Result<(), AppError> {
    let abi_list_df = abi_reader::read_abis()?;
    let ethereum_logs_df = load_ethereum_logs()?;
    
    let logs_left_join_abi_df = ethereum_logs_df
        .lazy()
        .join(
            abi_list_df.lazy(),
            [col("topic0"), col("address")],
            [col("topic0"), col("Address")],
            JoinArgs::new(JoinType::Left),
        )
        .collect()?;

    println!("Joined DataFrame Schema: {:?}", &logs_left_join_abi_df.schema());

    let decoded_df = decode_logs(logs_left_join_abi_df)?;

    println!("Decoded DataFrame: {}", decoded_df);

    save_decoded_logs(decoded_df, "data/decoded__ethereum__logs.parquet")?;

    Ok(())
}


fn load_ethereum_logs() -> Result<DataFrame, AppError> {
    LazyFrame::scan_parquet("data/ethereum__logs__*.parquet", ScanArgsParquet::default())?
        .collect()
        .map_err(AppError::from)
}

fn save_decoded_logs(mut df: DataFrame, path: &str) -> Result<(), AppError> {
    let mut file = File::create(Path::new(path))?;
    ParquetWriter::new(&mut file).finish(&mut df)?;
    Ok(())
}