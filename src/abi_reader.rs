use std::{fs, str::FromStr};
use alloy::{json_abi::{Event, JsonAbi}, primitives::{Address, B256}};
use polars::prelude::*;

#[derive(Debug)]
pub struct EventRow {
    address: Address,
    topic0: B256,
    signature: String,
    full_signature: String,
    abi_item: String,
    name: String,
    anonymous: bool,
    id: String,
    pub event_struct: Event
}

pub fn read_json_abi() -> Vec<EventRow>{
    // Define the path to the directory containing the ABI JSON files.
    let abis_path = "./ABIs";
    let mut event_list: Vec<EventRow> = vec![];

    // Iterate over each file in the directory.
    for entry in fs::read_dir(abis_path).unwrap_or_else(|_| panic!("Unable to read directory {}", abis_path)) {
        let entry = entry.expect("Unable to read file");
        let path = entry.path();
        // Ensure the file has a .json extension and the file name can be parsed as an Address.
        let address = path.extension().and_then(|s| s.to_str()).filter(|&ext| ext == "json")
            .and_then(|_| path.file_stem())
            .and_then(|s| s.to_str())
            .and_then(|str| Address::from_str(str).ok());

        if let Some(address) = address {
        // All conditions are met, and 'address' is now available for use
            println!("Reading file: {:?}", path);
            let json = std::fs::read_to_string(path).unwrap();
            let abi: JsonAbi = serde_json::from_str(&json).unwrap();
            for event in abi.events() {
                let row = EventRow {
                    address: address,
                    topic0: event.selector(),
                    signature: event.signature(),
                    full_signature: event.full_signature(),
                    abi_item: format!("{:?}", event),
                    name: event.name.to_string(),
                    anonymous: event.anonymous,
                    id: event.selector().to_string() + &event.signature()[..],
                    event_struct: event.clone()
                };
                event_list.push(row);
            } 
        } else {
            //skip file if it's not a .json or couldn't be parsed into an address
            println!("Skipping file: {:?}", path);
        }
    }
    return event_list
}

pub fn create_dataframe_from_event_rows(rows: Vec<EventRow>) -> PolarsResult<DataFrame> {
    let address = Series::new("Address", rows.iter().map(|r| r.address.to_string()).collect::<Vec<String>>());
    let topic0 = Series::new("topic0", rows.iter().map(|r| r.topic0.to_string()).collect::<Vec<String>>());
    let signature = Series::new("signature", rows.iter().map(|r| r.signature.clone()).collect::<Vec<String>>());
    let full_signature = Series::new("full_signature", rows.iter().map(|r| r.full_signature.clone()).collect::<Vec<String>>());
    let abi_item = Series::new("abi_item", rows.iter().map(|r| format!("{:?}", r.abi_item)).collect::<Vec<String>>());
    let name = Series::new("name", rows.iter().map(|r| r.name.clone()).collect::<Vec<String>>());
    let anonymous = Series::new("anonymous", rows.iter().map(|r| r.anonymous).collect::<Vec<bool>>());
    let id = Series::new("id", rows.iter().map(|r| r.id.clone()).collect::<Vec<String>>());

    DataFrame::new(vec![address, topic0, signature, full_signature, abi_item, name, anonymous, id])
}