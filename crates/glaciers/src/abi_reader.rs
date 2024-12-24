use std::{fs::{self, File}, str::FromStr, path::Path};
use alloy::{json_abi::JsonAbi, primitives::{Address, FixedBytes}};
use polars::prelude::*;
use chrono::Local;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum AbiReadError {
    #[error("Polars error: {0}")]
    PolarsError(#[from] polars::prelude::PolarsError),
    #[error("Invalid ABI file: {0}")]
    InvalidAbiFile(String),
    #[error("Invalid folder path: {0}")]
    InvalidFolder(String),
    #[error("Invalid ABI DF path: {0}")]
    InvalidAbiDf(String),
}

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

pub fn read_abis_df(topic0_path: String, abi_folder_path: String) -> Result<DataFrame, AbiReadError> {
    let path = Path::new(&topic0_path);
    let existing_df = if path.exists() {
        read_parquet_file(path)?
    } else {
        return Ok(DataFrame::new(vec![
            Series::new("address", Vec::<String>::new()),
            Series::new("hash", Vec::<Vec<u8>>::new()),
            Series::new("full_signature", Vec::<String>::new()),
            Series::new("name", Vec::<String>::new()),
            Series::new("anonymous", Vec::<Option<bool>>::new()),
            Series::new("statemutability", Vec::<Option<String>>::new()),
            Series::new("id", Vec::<String>::new()),
        ])?);
    };

    let new_df = read_new_abi_files(&abi_folder_path)?;
    let diff_df = new_df.clone().join(
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
    let mut combined_df = if existing_df.height() > 0 {
        concat_dataframes(vec![existing_df.lazy(), diff_df.lazy()])?
    } else {
        new_df
    };
    let mut file = File::create(path).map_err(|e| AbiReadError::InvalidAbiDf(e.to_string()))?;
    ParquetWriter::new(&mut file).finish(&mut combined_df).map_err(AbiReadError::PolarsError)?;

    Ok(combined_df)
}

fn read_parquet_file(path: &Path) -> Result<DataFrame, AbiReadError> {
    ParquetReader::new(File::open(path).map_err(|e| AbiReadError::InvalidAbiDf(e.to_string()))?)
        .finish()
        .map_err(AbiReadError::PolarsError)
}

pub fn read_new_abi_files(abi_folder_path: &str) -> Result<DataFrame, AbiReadError> {
    let paths = fs::read_dir(abi_folder_path)
        .map_err(|e| AbiReadError::InvalidFolder(e.to_string()))?;
    
    // Process each file and collect successful results
    let processed_frames: Vec<DataFrame> = paths
        .filter_map(|entry| {
            let path = entry.ok()?.path();
            match process_abi_file(path) {
                Ok(df) => Some(df),
                Err(_) => None,  // Silently skip invalid files
            }
        })
        .collect();
    
    // Handle case where no valid files were processed
    if processed_frames.is_empty() {
        // Return empty Dataframe with schema
        return Ok(DataFrame::new(vec![
            Series::new("address", Vec::<String>::new()),
            Series::new("hash", Vec::<Vec<u8>>::new()),
            Series::new("full_signature", Vec::<String>::new()),
            Series::new("name", Vec::<String>::new()),
            Series::new("anonymous", Vec::<Option<bool>>::new()),
            Series::new("statemutability", Vec::<Option<String>>::new()),
            Series::new("id", Vec::<String>::new()),
        ])?);
    }
    
    // Combine all DataFrames
    let mut combined_df = processed_frames[0].clone();
    for df in processed_frames.into_iter().skip(1) {
        // concatenate each file dataframe
        combined_df = combined_df.vstack(&df).map_err(AbiReadError::PolarsError)?;
    }
    
    Ok(combined_df)
}

fn process_abi_file(path: std::path::PathBuf) -> Result<DataFrame, AbiReadError> {
    let address = extract_address_from_path(&path);
    if let Some(address) = address {
        println!(
            "[{}] Reading ABI file: {:?}",
            Local::now().format("%Y-%m-%d %H:%M:%S"),
            path
        );

        let json = fs::read_to_string(&path).map_err(|e| AbiReadError::InvalidAbiFile(e.to_string()))?;
        let abi: JsonAbi = serde_json::from_str(&json).map_err(|e| AbiReadError::InvalidAbiFile(e.to_string()))?;
        // let a = Some(abi.events().map(|event| create_event_row(event)).collect());
        process_abi_itens(abi, address)
    } else {
        //skip file if it's not a .json or couldn't be parsed into an address by the extract_address_from_path function
        println!(
            "[{}] Skipping ABI file: {:?}. It's not a .json or filename couldn't be parsed into an address",
            Local::now().format("%Y-%m-%d %H:%M:%S"),
            path
        );
        Err(AbiReadError::InvalidAbiFile(
            "File is not a JSON format or filename couldn't be parsed into an address".to_string()
        ))
    }
}

fn process_abi_itens(abi: JsonAbi, address: Address) -> Result<DataFrame, AbiReadError>{
    let function_rows: Vec<AbiItemRow> = abi.functions().map(|function| create_function_row(function, address)).collect();
    let event_rows: Vec<AbiItemRow> = abi.events().map(|event| create_event_row(event, address)).collect();
    let abi_rows = [function_rows, event_rows].concat();
    
    create_dataframe_from_rows(abi_rows)
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
        hash: event.selector(),
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

pub fn create_dataframe_from_rows(rows: Vec<AbiItemRow>) -> Result<DataFrame, AbiReadError> {
    let columns = vec![
        Series::new("address", rows.iter().map(|r| r.address.clone()).collect::<Vec<String>>()),
        Series::new("hash", rows.iter().map(|r| r.hash.as_slice()).collect::<Vec<&[u8]>>()),
        Series::new("full_signature", rows.iter().map(|r| r.full_signature.clone()).collect::<Vec<String>>()),
        Series::new("name", rows.iter().map(|r| r.name.clone()).collect::<Vec<String>>()),
        Series::new("anonymous", rows.iter().map(|r| r.anonymous).collect::<Vec<Option<bool>>>()),
        Series::new("state_mutability", rows.iter().map(|r| r.statemutability.clone()).collect::<Vec<Option<String>>>()),
        Series::new("id", rows.iter().map(|r| r.id.clone()).collect::<Vec<String>>()),
    ];

    DataFrame::new(columns).map_err(AbiReadError::PolarsError)
}

fn concat_dataframes(dfs: Vec<LazyFrame>) -> Result<DataFrame, AbiReadError> {
    let df = concat(dfs, UnionArgs::default())?;
    let df = df.unique(Some(vec!["id".to_string()]), UniqueKeepStrategy::First).collect();
    df.map_err(AbiReadError::PolarsError)
}
