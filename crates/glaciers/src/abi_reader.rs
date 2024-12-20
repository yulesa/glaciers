use std::{fs::{self, File}, str::FromStr, path::Path};
use alloy::{json_abi::JsonAbi, primitives::{Address, B256}};
use polars::prelude::*;
use chrono::Local;


#[derive(Debug)]
pub struct EventRow {
    topic0: B256,
    full_signature: String,
    name: String,
    anonymous: bool,
    id: String,
}

pub fn read_abis_topic0(topic0_path: String, abi_folder_path: String) -> PolarsResult<DataFrame> {
    let path = Path::new(&topic0_path);
    let existing_df = if path.exists() {
        read_parquet_file(path)?
    } else {
        DataFrame::default()
    };

    let new_rows = read_new_abi_files(&abi_folder_path);
    let new_df = create_dataframe_from_event_rows(new_rows)?;
    let diff_df = new_df.join(
        &existing_df,
        ["id"],
        ["id"],
        JoinArgs::new(JoinType::Anti))?;
    //if diff_df is not empty, print all diff_df rows id's column
    if diff_df.height() == 0 {
        println!(
            "[{}] No new event signatures found in the scanned files.",
            Local::now().format("%Y-%m-%d %H:%M:%S"),
        );
    } else {
        println!(
            "[{}] New event signatures found found:",
            Local::now().format("%Y-%m-%d %H:%M:%S"),
        );
        diff_df.column("id").unwrap().iter().for_each(|s| println!("{}", s.to_string()));
    }

    let mut combined_df = concat_dataframes(vec![existing_df.lazy(), diff_df.lazy()])?;
    let mut file = File::create(path)?;
    ParquetWriter::new(&mut file).finish(&mut combined_df)?;

    Ok(combined_df)
}

fn read_parquet_file(path: &Path) -> PolarsResult<DataFrame> {
    ParquetReader::new(File::open(path)?).finish()
}

pub fn read_new_abi_files(abi_folder_path: &str) -> Vec<EventRow> {
    fs::read_dir(abi_folder_path)
        .unwrap_or_else(|_| panic!("Unable to read directory {}", abi_folder_path))
        .filter_map(|entry| process_abi_file(entry.expect("Unable to read file").path()))
        .flatten()
        .collect()
}

fn process_abi_file(path: std::path::PathBuf) -> Option<Vec<EventRow>> {
    let address = extract_address_from_path(&path);
    if address.is_some() {
        println!(
            "[{}] Reading ABI file: {:?}",
            Local::now().format("%Y-%m-%d %H:%M:%S"),
            path
        );

        let json = fs::read_to_string(&path).ok()?;
        let abi: JsonAbi = serde_json::from_str(&json).ok()?;
        Some(abi.events().map(|event| create_event_row(event)).collect())
    } else {
        //skip file if it's not a .json or couldn't be parsed into an address by the extract_address_from_path function
        println!(
            "[{}] Skipping ABI file: {:?}. It's not a .json or filename couldn't be parsed into an address",
            Local::now().format("%Y-%m-%d %H:%M:%S"),
            path
        );
        None
    }
}

fn extract_address_from_path(path: &Path) -> Option<Address> {
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
