use std::{fs::{self, File}, str::FromStr, path::Path};
use alloy::{json_abi::JsonAbi, primitives::{Address, FixedBytes}};
use polars::prelude::*;
use chrono::Local;


// #[derive(Debug)]
// pub struct EventRow {
//     address: Address,
//     topic0: FixedBytes<32>,
//     full_signature: String,
//     name: String,
//     anonymous: bool,
//     id: String,
// }
// #[derive(Debug)]
// pub struct FunctionRow {
//     address: Address,
//     byte4: FixedBytes<4>,
//     full_signature: String,
//     name: String,
//     statemutability : String,
//     id: String,
// }
#[derive(Debug, Clone)]
pub struct AbiItemRow {
    address: String,
    hash: FixedBytes<32>,
    full_signature: String,
    name: String,
    anonymous: Option<bool>,
    statemutability : Option<String>,
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
    let new_df = create_dataframe_from_rows(new_rows)?;
    let diff_df = if existing_df.height() > 0 {
        new_df.clone().join(
            &existing_df,
            ["id"],
            ["id"],
            JoinArgs::new(JoinType::Anti))?
    } else {
        new_df.clone()
    };
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
    let mut combined_df = if existing_df.height() > 0 {
        concat_dataframes(vec![existing_df.lazy(), diff_df.lazy()])?
    } else {
        new_df
    };
    let mut file = File::create(path)?;
    ParquetWriter::new(&mut file).finish(&mut combined_df)?;

    Ok(combined_df)
}

fn read_parquet_file(path: &Path) -> PolarsResult<DataFrame> {
    ParquetReader::new(File::open(path)?).finish()
}

pub fn read_new_abi_files(abi_folder_path: &str) -> Vec<AbiItemRow> {
    fs::read_dir(abi_folder_path)
        .unwrap_or_else(|_| panic!("Unable to read directory {}", abi_folder_path))
        .filter_map(|entry| process_abi_file(entry.expect("Unable to read file").path()))
        .flatten()
        .collect()
}

fn process_abi_file(path: std::path::PathBuf) -> Option<Vec<AbiItemRow>> {
    let address = extract_address_from_path(&path);
    if let Some(address) = address {
        println!(
            "[{}] Reading ABI file: {:?}",
            Local::now().format("%Y-%m-%d %H:%M:%S"),
            path
        );

        let json = fs::read_to_string(&path).ok()?;
        let abi: JsonAbi = serde_json::from_str(&json).ok()?;
        // let a = Some(abi.events().map(|event| create_event_row(event)).collect());
        Some(process_abi_itens(abi, address))
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

fn process_abi_itens(abi: JsonAbi, address: Address) -> Vec<AbiItemRow>{
    let function_rows: Vec<AbiItemRow> = abi.functions().map(|function| create_function_row(function, address)).collect();
    let event_rows: Vec<AbiItemRow> = abi.events().map(|event| create_event_row(event, address)).collect();
    [function_rows, event_rows].concat()
    
}

fn extract_address_from_path(path: &Path) -> Option<Address> {
    path.extension().and_then(|s| s.to_str()).filter(|&ext| ext == "json")
        .and_then(|_| path.file_stem())
        .and_then(|s| s.to_str())
        .and_then(|str| Address::from_str(str).ok())
}

fn create_event_row(event: &alloy::json_abi::Event, address: Address) -> AbiItemRow {
    let event_row = AbiItemRow {
        address: address.to_string(),
        hash: event.selector().,
        full_signature: event.full_signature(),
        name: event.name.to_string(),
        anonymous: Some(event.anonymous),
        statemutability: None,
        id: event.selector().to_string() +" - "+ &event.full_signature()[..],
    };
    event_row
}

fn create_function_row(function: &alloy::json_abi::Function, address: Address) -> AbiItemRow {
    let state_mutability = match function.state_mutability {
        alloy::json_abi::StateMutability::Pure => "pure".to_owned(),
        alloy::json_abi::StateMutability::View => "view".to_owned(),
        alloy::json_abi::StateMutability::NonPayable => "nonpayable".to_owned(),
        alloy::json_abi::StateMutability::Payable => "payable".to_owned(),
    };
    let function_row = AbiItemRow {
        address: address.to_string(),
        hash: FixedBytes::from_slice(&[&[0u8; 28], function.selector().as_slice()].concat()),
        full_signature: function.full_signature(),
        name: function.name.to_string(),
        anonymous: None,
        statemutability: Some(state_mutability),
        id: function.selector().to_string() +" - "+ &function.full_signature()[..],
    };
    function_row
}

pub fn create_dataframe_from_rows(rows: Vec<AbiItemRow>) -> PolarsResult<DataFrame> {
    let columns = vec![
        Series::new("address", rows.iter().map(|r| r.address.clone()).collect::<Vec<String>>()),
        Series::new("hash", rows.iter().map(|r| r.hash.as_slice()).collect::<Vec<&[u8]>>()),
        Series::new("full_signature", rows.iter().map(|r| r.full_signature.clone()).collect::<Vec<String>>()),
        Series::new("name", rows.iter().map(|r| r.name.clone()).collect::<Vec<String>>()),
        Series::new("anonymous", rows.iter().map(|r| r.anonymous).collect::<Vec<Option<bool>>>()),
        Series::new("state_mutability", rows.iter().map(|r| r.statemutability.clone()).collect::<Vec<Option<String>>>()),
        Series::new("id", rows.iter().map(|r| r.id.clone()).collect::<Vec<String>>()),
    ];

    DataFrame::new(columns)
}

fn concat_dataframes(dfs: Vec<LazyFrame>) -> PolarsResult<DataFrame> {
    let df = concat(dfs, UnionArgs::default())?;
    let df = df.unique(Some(vec!["id".to_string()]), UniqueKeepStrategy::First).collect();
    df
}
