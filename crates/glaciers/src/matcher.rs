use polars::prelude::*;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum MatcherError {
    #[error("Polars error: {0}")]
    PolarsError(#[from] PolarsError),
}

pub fn match_logs_by_topic0(log_df: DataFrame, abi_df: DataFrame) -> Result<DataFrame, MatcherError> {
    //select only the relevant columns
    let abi_df = abi_df.lazy().select([
        col("hash"),
        col("full_signature"),
        col("name"),
        col("anonymous")
    ]);
    // Perform left join with ABI topic0 list
    let logs_left_join_abi_df = log_df
        .lazy()
        .join(
            abi_df,
            [col("topic0")],
            [col("hash")],
            JoinArgs::new(JoinType::Left),
        )
        .collect()?;

    Ok(logs_left_join_abi_df)
}