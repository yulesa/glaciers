use std::{fs::File, path::Path};

use alloy::{dyn_abi::{DecodedEvent, EventExt}, json_abi::Event, primitives::FixedBytes};
use decoding::abi_reader::{self};
use polars::prelude::{*, as_struct};
use thiserror::Error;

#[derive(Error, Debug)]
enum AppError {
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
    let logs_left_join_abi_df  = ethereum_logs_df.lazy()
        .join(
            abi_list_df.clone().lazy(),
            [col("topic0"), col("address")],
            [col("topic0"), col("Address")],
            JoinArgs::new(JoinType::Left),
        )
        .collect()?;
    println!("{:?}", &logs_left_join_abi_df.schema());



//https://docs.pola.rs/user-guide/expressions/structs/#multi-column-ranking
    let mut decoded_df = logs_left_join_abi_df
        .lazy()
        .with_columns([
            // pack to struct to get access to multiple fields in a custom `apply/map`
            as_struct(vec![col("topic0"), col("topic1"), col("topic2"), col("topic3"), col("data"), col("full_signature")])
                // we will compute the len(a) + b
                .apply(
                    |s| {
                        // downcast to struct
                        let ca = s.struct_()?;

                        // get the fields as Series
                        let s_topic0 = &ca.fields()[0];
                        let s_topic1 = &ca.fields()[1];
                        let s_topic2 = &ca.fields()[2];
                        let s_topic3 = &ca.fields()[3];
                        let s_data = &ca.fields()[4];
                        let s_sig = &ca.fields()[5];

                        // downcast the `Series` to their known type
                        let ca_topic0 = s_topic0.binary()?;
                        let ca_topic1 = s_topic1.binary()?;
                        let ca_topic2 = s_topic2.binary()?;
                        let ca_topic3 = s_topic3.binary()?;
                        let ca_data = s_data.binary()?;
                        let ca_sig = s_sig.str()?;
                        
                        // iterate both `ChunkedArrays`
                        let out: StringChunked = ca_topic0
                            .into_iter()
                            .zip(ca_topic1.into_iter())
                            .zip(ca_topic2.into_iter())
                            .zip(ca_topic3.into_iter())
                            .zip(ca_data.into_iter())
                            .zip(ca_sig.into_iter())
                            .map(|(((((opt_topic0, opt_topic1), opt_topic2), opt_topic3), opt_data), opt_sig)| {
                                let zero_filled_topic = vec![0u8; 32];
                                let topic0 = opt_topic0.unwrap_or(&zero_filled_topic);
                                let topic1 = opt_topic1.unwrap_or(&zero_filled_topic);
                                let topic2 = opt_topic2.unwrap_or(&zero_filled_topic);
                                let topic3 = opt_topic3.unwrap_or(&zero_filled_topic);
                                let data = opt_data.unwrap_or(&[]);
                                let sig = opt_sig.unwrap_or("");
                    
                                let topics = vec![
                                    FixedBytes::from_slice(topic0),
                                    FixedBytes::from_slice(topic1),
                                    FixedBytes::from_slice(topic2),
                                    FixedBytes::from_slice(topic3),
                                ];
                    
                                match decode(sig, topics, data) {
                                    Ok(decoded_log) => {
                                        println!("{:?}", decoded_log);
                                        let decoded = format!("{:?}, {:?}", decoded_log.indexed, decoded_log.body);
                                        Some(decoded)
                                    },
                                    Err(e) => {
                                        eprintln!("Error decoding log: {:?}", e);
                                        None
                                    }
                                }
                            }).collect();


                        Ok(Some(out.into_series()))
                    },
                    GetOutput::from_type(DataType::String),
                )
                // note: the `'solution_map_elements'` alias is just there to show how you
                // get the same output as in the Python API example.
                .alias("decoded_log"),
        ])
        .collect()?;

    println!("{}", decoded_df);

    //save the decoded df as in parquet file.
    let path = Path::new("data/decoded__ethereum__logs.parquet");
    let mut file = File::create(path)?;
    ParquetWriter::new(&mut file).finish(&mut decoded_df)?;



    Ok(())

}

fn decode(full_signature: &str, topics: Vec<FixedBytes<32>>, data: &[u8]) -> Result<DecodedEvent, AppError> {
    let event = Event::parse(full_signature)
        .map_err(|e| AppError::DecodingError(e.to_string()))?;
    let decoded_log = event.decode_log_parts(topics, data, false)
        .map_err(|e| AppError::DecodingError(e.to_string()))?;
    Ok(decoded_log)
}

fn load_ethereum_logs() -> Result<DataFrame, AppError> {
    LazyFrame::scan_parquet("data/ethereum__logs__*.parquet", Default::default())?
        .collect()
        .map_err(AppError::from)
}