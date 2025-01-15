use polars::prelude::*;
use thiserror::Error;

use crate::configger::get_config;

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

    let topic0_alias = get_config().decoder.schema.alias.topic0;
    // Perform left join with ABI topic0 list
    let logs_left_join_abi_df = log_df
        .lazy()
        .join(
            abi_df,
            [col(topic0_alias.as_str())],
            [col("hash")],
            JoinArgs::new(JoinType::Left),
        )
        .collect()?;

    Ok(logs_left_join_abi_df)
}