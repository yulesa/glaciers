use std::{fs::{self, File}, str::FromStr, path::Path};
use alloy::{json_abi::JsonAbi, primitives::{Address, B256}};
use polars::prelude::*;

#[derive(Debug)]
pub struct EventRow {
    topic0: B256,
    full_signature: String,
    name: String,
    anonymous: bool,
    id: String,
}

pub fn read_abis_topic0(path: &str) -> PolarsResult<DataFrame> {
    let path = Path::new(path);
    let existing_df = if path.exists() {
        read_parquet_file(path)?
    } else {
        DataFrame::default()
    };

    let new_rows = read_new_abi_files();
    let new_df = create_dataframe_from_event_rows(new_rows)?;
    let mut combined_df = concat_dataframes(vec![existing_df.lazy(), new_df.lazy()])?;
    let mut file = File::create(path)?;
    ParquetWriter::new(&mut file).finish(&mut combined_df)?;

    Ok(combined_df)
}

fn read_parquet_file(path: &Path) -> PolarsResult<DataFrame> {
    ParquetReader::new(File::open(path)?).finish()
}

pub fn read_new_abi_files() -> Vec<EventRow> {
    let abis_path = "./ABIs";
    fs::read_dir(abis_path)
        .unwrap_or_else(|_| panic!("Unable to read directory {}", abis_path))
        .filter_map(|entry| process_abi_file(entry.expect("Unable to read file").path()))
        .flatten()
        .collect()
}

fn process_abi_file(path: std::path::PathBuf) -> Option<Vec<EventRow>> {
    let address = extract_address_from_path(&path);
    if address.is_some() {
        println!("Reading file: {:?}", path);
        let json = fs::read_to_string(&path).ok()?;
        let abi: JsonAbi = serde_json::from_str(&json).ok()?;
        Some(abi.events().map(|event| create_event_row(event)).collect())
    } else {
        //skip file if it's not a .json or couldn't be parsed into an address by the extract_address_from_path function
        println!("Skipping file: {:?}. It's not a .json or filename couldn't be parsed into an address", path);
        None
    }
}

fn extract_address_from_path(path: &std::path::Path) -> Option<Address> {
    path.extension().and_then(|s| s.to_str()).filter(|&ext| ext == "json")
        .and_then(|_| path.file_stem())
        .and_then(|s| s.to_str())
        .and_then(|str| Address::from_str(str).ok())
}

fn create_event_row(event: &alloy::json_abi::Event) -> EventRow {
    let event_row = EventRow {
        topic0: event.selector(),
        full_signature: event.full_signature(),
        name: event.name.to_string(),
        anonymous: event.anonymous,
        id: event.selector().to_string() +" - "+ &event.full_signature()[..],
    };
    println!("\tEventRow: {:?}, {:?}", event_row.full_signature, event_row.topic0);
    event_row
}

pub fn create_dataframe_from_event_rows(rows: Vec<EventRow>) -> PolarsResult<DataFrame> {
    let columns = vec![
        Series::new("topic0", rows.iter().map(|r| r.topic0.as_slice()).collect::<Vec<&[u8]>>()),
        Series::new("full_signature", rows.iter().map(|r| r.full_signature.clone()).collect::<Vec<String>>()),
        Series::new("name", rows.iter().map(|r| r.name.clone()).collect::<Vec<String>>()),
        Series::new("anonymous", rows.iter().map(|r| r.anonymous).collect::<Vec<bool>>()),
        Series::new("id", rows.iter().map(|r| r.id.clone()).collect::<Vec<String>>()),
    ];

    DataFrame::new(columns)
}

fn concat_dataframes(dfs: Vec<LazyFrame>) -> PolarsResult<DataFrame> {
    let df = concat(dfs, UnionArgs::default())?;
    let df = df.unique(Some(vec!["id".to_string()]), UniqueKeepStrategy::First).collect();
    df
}
