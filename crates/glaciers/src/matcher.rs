use polars::prelude::*;
use thiserror::Error;
use crate::configger::get_config;

#[derive(Error, Debug)]
pub enum MatcherError {
    #[error("Polars error: {0}")]
    PolarsError(#[from] PolarsError),
}

pub fn match_logs_by_topic0_address(log_df: DataFrame, abi_df: DataFrame) -> Result<DataFrame, MatcherError> {
    let topic0_alias = get_config().log_decoder.log_schema.log_alias.topic0;
    let address_alias = get_config().log_decoder.log_schema.log_alias.address;


    let logs_left_join_abi_df = log_df
        .lazy()
        .with_column((lit(1 as u32) +
            col("topic1").is_not_null() +
            col("topic2").is_not_null() +
            col("topic3").is_not_null()).alias("num_indexed_args"))
        .join(
            abi_df.lazy(),
            [col(topic0_alias.as_str()), col(address_alias.as_str()), col("num_indexed_args")],
            [col("hash"), col("address"), col("num_indexed_args")],
            JoinArgs::new(JoinType::Left),
        )
        .collect()?;

    Ok(logs_left_join_abi_df)
}

pub fn match_logs_by_topic0(log_df: DataFrame, abi_df: DataFrame) -> Result<DataFrame, MatcherError> {
    let logs_1 = match_logs_by_topic0_address(log_df.clone(), abi_df.clone())?;
    let log_df_cols: Vec<Expr> = log_df.get_columns().iter().map(|s| col(s.name())).collect();
    // Split the logs into matched and not matched in the first step
    let logs_address_matched = logs_1.clone().lazy().filter(col("full_signature").is_not_null()).collect()?;
    let logs_address_not_matched = logs_1.lazy().filter(col("full_signature").is_null()).select(log_df_cols);

    // create an abi_df with the most frequent signature for each hash
    let abi_df = abi_df
        .lazy()
        //count the number of rows for each full_signature
        .group_by(["hash", "full_signature", "name", "anonymous", "num_indexed_args"])
        .agg([all().first(), len().alias("signature_count")])
        //sort the rows by signature_count in descending order
        .sort("signature_count", SortOptions {
            descending: true,
            nulls_last: true,
            ..Default::default()}
        )
        // group by hash and num_indexed_args and keep the first row (most frequent hash and num_indexed_args)
        .group_by(["hash", "num_indexed_args"]).agg([
            all().first()
        ]).drop(["address", "signature_count"]);

    let topic0_alias = get_config().log_decoder.log_schema.log_alias.topic0;
    // add a column with the number of indexed args
    let logs_2 = logs_address_not_matched
        .with_column((lit(1 as u32) +
            col("topic1").is_not_null() +
            col("topic2").is_not_null() +
            col("topic3").is_not_null()).alias("num_indexed_args"))
        // Perform left join with the most frequent signature for each hash that has the same number of indexed args
        .join(
            abi_df,
            [col(topic0_alias.as_str()), col("num_indexed_args")],
            [col("hash"), col("num_indexed_args")],
            JoinArgs::new(JoinType::Left),
        )
        .collect()?;

    // Combine first and second matching steps
    let logs_df = logs_address_matched.vstack(&logs_2)?;

    Ok(logs_df)
}

pub fn match_traces_by_4bytes_address(trace_df: DataFrame, abi_df: DataFrame) -> Result<DataFrame, MatcherError> {
    let selector_alias = get_config().trace_decoder.trace_schema.trace_alias.selector;
    let action_to = get_config().trace_decoder.trace_schema.trace_alias.action_to;

    let traces_left_join_abi_df = trace_df
        .lazy()
        .join(
            abi_df.lazy(),
            [col(selector_alias.as_str()), col(action_to.as_str())],
            [col("hash"), col("address")],
            JoinArgs::new(JoinType::Left),
        )
        .collect()?;

    Ok(traces_left_join_abi_df)
}

pub fn match_traces_by_4bytes(trace_df: DataFrame, abi_df: DataFrame) -> Result<DataFrame, MatcherError> {
    let traces_1 = match_traces_by_4bytes_address(trace_df.clone(), abi_df.clone())?;
    let trace_df_cols: Vec<Expr> = trace_df.get_columns().iter().map(|s| col(s.name())).collect();
    let traces_address_matched = traces_1.clone().lazy().filter(col("full_signature").is_not_null()).collect()?;
    let traces_address_not_matched = traces_1.lazy().filter(col("full_signature").is_null()).select(trace_df_cols);

    // create an abi_df with the most frequent signature for each hash
    let abi_df = abi_df
        .lazy()
        //count the number of rows for each full_signature
        .group_by(["hash", "full_signature", "name"])
        .agg([all().first(), len().alias("signature_count")])
        //sort the rows by signature_count in descending order
        .sort("signature_count", SortOptions {
            descending: true,
            nulls_last: true,
            ..Default::default()}
        )
        // group by hash and num_indexed_args and keep the first row (most frequent hash and num_indexed_args)
        .group_by(["hash"]).agg([
            all().first()
        ]).drop(["address", "signature_count"]);

    let selector_alias = get_config().trace_decoder.trace_schema.trace_alias.selector;
    let trace_2 = traces_address_not_matched
            .join(
                abi_df,
                [col(selector_alias.as_str())],
                [col("hash")],
                JoinArgs::new(JoinType::Left),
            )
            .collect()?;

    let traces_df = traces_address_matched.vstack(&trace_2)?;

    Ok(traces_df)
}
