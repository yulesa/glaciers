use alloy::{dyn_abi::EventExt, json_abi::Event, primitives::FixedBytes};
use decoding::abi_reader::{self, create_dataframe_from_event_rows};
use polars::prelude::*;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    
    let abi_list = abi_reader::read_json_abi();
    // let abi_df = create_dataframe_from_event_rows(abi_list)?;
    // println!("{:#?}", abi_df.get(0));

    // println!("{:#?}", abi_list[0]);
    let approval_event:Event = abi_list[0].event_struct.clone();

    // alloy::json_abi::Event.decode_log_parts

    let ethereum_logs_df = LazyFrame::scan_parquet(r"data/ethereum__logs__*.parquet", Default::default())?.collect()?;
    let first_log = ethereum_logs_df.select(&["topic0", "topic1", "topic2", "topic3"])?;
    let first_row = first_log.get(7).ok_or("DataFrame is empty")?;
    

    let topics: Vec<FixedBytes<32>> = first_row.iter()
        .filter_map(|s| match s {
            AnyValue::Binary(b) => Some(FixedBytes::from_slice(b)),
            _ => None,
        })
        .collect();
    println!("{:#?}", topics);

    let first_log = ethereum_logs_df.select(&["data"])?;
    let first_row = first_log.get(7).ok_or("DataFrame is empty")?;
    let data: &[u8] = match first_row[0] {
        AnyValue::Binary(b) => b,
        _ => return Err("Expected binary data".into()),
    };
    
    let decodedlog = approval_event.decode_log_parts(topics, data, false)?;
    println!("{:#?}", decodedlog);
    return Ok(());
}

