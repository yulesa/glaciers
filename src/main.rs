use decoding::abi_reader::{self};
use decoding::decoder::decode_logs;
use polars::prelude::*;
use decoding::decoder::DecodeError;
use thiserror::Error;
use chrono::Local;

const TOPIC0_PATH: &str = "ABIs/ethereum__abis_topic0.parquet";
const SOURCE_PATH: &str = "data/ethereum__logs__*.parquet";

#[derive(Error, Debug)]
enum AppError {
    #[error("Decode error: {0}")]
    DecodeError(#[from] DecodeError),
    #[error("Polars error: {0}")]
    PolarsError(#[from] polars::prelude::PolarsError),
}

#[tokio::main]
async fn main() -> Result<(), AppError> {
    let abi_list_df = abi_reader::read_abis_topic0(TOPIC0_PATH)?;
    let ethereum_logs_df = load_ethereum_logs(SOURCE_PATH)?;
    println!("[{}] Log ABI join has started", Local::now().format("%Y-%m-%d %H:%M:%S"));
    let logs_left_join_abi_df = ethereum_logs_df
        .lazy()
        .join(
            abi_list_df.lazy(),
            [col("topic0")],
            [col("topic0")],
            JoinArgs::new(JoinType::Left),
        ).drop(&["id"])
        .collect()?;

    println!("[{}] Decoding has started", Local::now().format("%Y-%m-%d %H:%M:%S"));
    decode_logs(logs_left_join_abi_df).await?;
    println!("[{}] Decoding has finished", Local::now().format("%Y-%m-%d %H:%M:%S"));

    Ok(())
}

fn load_ethereum_logs(path: &str) -> Result<DataFrame, AppError> {
    LazyFrame::scan_parquet(path,  ScanArgsParquet::default())?
        .collect()
        .map_err(AppError::from)
}