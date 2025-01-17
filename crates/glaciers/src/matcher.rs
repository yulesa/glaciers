use polars::prelude::*;
use thiserror::Error;
use crate::configger::get_config;

#[derive(Error, Debug)]
pub enum MatcherError {
    #[error("Polars error: {0}")]
    PolarsError(#[from] PolarsError),
}

pub fn match_logs_by_topic0_address(log_df: DataFrame, abi_df: DataFrame) -> Result<DataFrame, MatcherError> {
    let topic0_alias = get_config().decoder.schema.alias.topic0;
    let address_alias = get_config().decoder.schema.alias.address;

    let abi_df = abi_df.lazy().select([
        col("hash"),
        col("address"),
        col("full_signature"),
        col("name"),
        col("anonymous")
    ]);

    let logs_left_join_abi_df = log_df
        .lazy()
        .join(
            abi_df,
            [col(topic0_alias.as_str()), col(address_alias.as_str())],
            [col("hash"), col("address")],
            JoinArgs::new(JoinType::Left),
        )
        .collect()?;

    Ok(logs_left_join_abi_df)
}

pub fn match_logs_by_topic0(log_df: DataFrame, abi_df: DataFrame) -> Result<DataFrame, MatcherError> {
    let logs_1 = match_logs_by_topic0_address(log_df, abi_df.clone())?;

    // Split the logs into matched and not matched in the first step
    let logs_address_matched = logs_1.clone().lazy().filter(col("full_signature").is_not_null()).collect()?;
    let logs_address_not_matched = logs_1.lazy().filter(col("full_signature").is_null()).drop(["hash", "full_signature", "name", "anonymous"]);

    // create an abi_df with the most frequent signature for each hash
    let abi_df = abi_df
        .lazy()
        //select only the relevant columns
        .select([
        col("hash"),
        col("full_signature"),
        col("name"),
        col("anonymous")])
        //count the number of rows for each full_signature
        .group_by(["hash", "full_signature", "name", "anonymous"]).agg([
            col("full_signature").count().alias("signature_count")])
        //sort the rows by signature_count in descending order
        .sort("signature_count", SortOptions {
            descending: true,
            nulls_last: true,
            ..Default::default()}
        )
        //group by hash and keep the first row for each hash
        .group_by(["hash"]).agg([
            col("full_signature").first().alias("full_signature"),
            col("name").first().alias("name"),
            col("anonymous").first().alias("anonymous"),
        ]);

    let topic0_alias = get_config().decoder.schema.alias.topic0;
    // Perform left join with the most frequent signature for each hash
    let logs_2 = logs_address_not_matched
        .join(
            abi_df,
            [col(topic0_alias.as_str())],
            [col("hash")],
            JoinArgs::new(JoinType::Left),
        )
        .collect()?;

    // Combine first and second matching steps
    let logs_df = logs_address_matched.vstack(&logs_2)?;

    Ok(logs_df)
}