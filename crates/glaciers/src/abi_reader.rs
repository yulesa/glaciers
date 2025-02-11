//! Module for reading and processing EVM ABIs for Glaciers.
//! 
//! This module provides functions to:
//! - Maintain a database of ABI signatures
//! - Read through ABI files in a directory
//! - Read a single ABI file
//! - Parse through the JSON ABI
//! - Extract function and event signatures
//! - Convert ABI data into a structured DataFrame format

use std::path::PathBuf;
use std::{str::FromStr, path::Path};
use std::fs;
use alloy::{json_abi::JsonAbi, primitives::{Address, FixedBytes}};
use polars::prelude::*;
use chrono::Local;
use thiserror::Error;

use crate::configger::{self, get_config}; 
use crate::utils;

/// Errors that can occur during ABI reading and processing
#[derive(Error, Debug)]
pub enum AbiReaderError {
    #[error("Polars error: {0}")]
    PolarsError(#[from] polars::prelude::PolarsError),
    #[error("Invalid ABI file: {0}")]
    InvalidAbiFile(String),
    #[error("Invalid path: {0}")]
    InvalidPath(String),
    #[error("Invalid ABI DF path: {0}")]
    InvalidAbiDf(String),
    #[error("Invalid configs: {0}")]
    InvalidConfig(String),
}
/// Represents a row in the ABI database containing function or event information.
/// 
/// # Fields
/// * `address` - The address of the contract
/// * `hash` - The hash of the function or event signature. Topic0 for events, selector(4bytes) for functions.
/// * `full_signature` - The full signature of the function or event
/// * `name` - The name of the function or event
/// * `anonymous` - (Only for events) Whether the event is anonymous.
/// * `num_indexed_args` - (Only for events) The number of indexed arguments.
/// * `state_mutability` - (Only for functions) The state mutability of the function.
/// * `id` - The unique identifier for the function or event
#[derive(Debug, Clone)]
pub struct AbiItemRow {
    address: FixedBytes<20>,
    hash: Hash,
    full_signature: String,
    name: String,
    anonymous: Option<bool>,
    num_indexed_args: Option<usize>,
    state_mutability : Option<String>,
    id: String,
}

/// Internal representation of function/event hashes
#[derive(Debug, Clone)]
enum Hash {
    Hash32(FixedBytes<32>), // Event topic hash
    Hash4(FixedBytes<4>)    // Function selector
}

impl Hash {
    fn as_bytes(&self) -> Vec<u8> {
        match self {
            Hash::Hash32(h) => h.as_slice().to_vec(),
            Hash::Hash4(h) => h.as_slice().to_vec(),
        }
    }
}

/// Updates or creates an ABI database by processing new ABI files in a folder.
///
/// # Arguments
/// * `abi_db_path` - Path to the existing or new ABI database file
/// * `abi_folder_path` - Path to the folder containing ABI files to process
///
/// # Returns
/// Returns a DataFrame containing only unique ABI information, based on the unique_key in the config.
///
/// # Examples
/// ```no_run
/// use glaciers::abi_reader::update_abi_db;
///
/// let result = update_abi_db(
///     "path/to/abi_db.parquet".to_string(),
///     "path/to/abi/folder".to_string()
/// );
/// ```
pub fn update_abi_db(abi_db_path: String, abi_folder_path: String) -> Result<DataFrame, AbiReaderError> {
    let path = Path::new(&abi_db_path);
    let existing_df = if path.exists() {
        utils::read_df_file(path)?
    } else {
        // Create a empty dataframe with a schema so joins don't fail for missing id field.
        DataFrame::new(vec![
            Series::new_empty("address", &DataType::String),
            Series::new_empty("hash", &DataType::Binary),
            Series::new_empty("full_signature", &DataType::String),
            Series::new_empty("name", &DataType::String),
            Series::new_empty("anonymous", &DataType::Boolean),
            Series::new_empty("num_indexed_args", &DataType::Int8),
            Series::new_empty("state_mutability", &DataType::String),
            Series::new_empty("id", &DataType::String),
        ])?
    };

    let new_df = read_new_abi_folder(&abi_folder_path)?;
    let diff_df = new_df.clone().join(
        &existing_df,
        ["id"],
        ["id"],
        JoinArgs::new(JoinType::Anti))?;
    if diff_df.height() == 0 {
        println!(
            "[{}] No new event signatures found in the scanned files.",
            Local::now().format("%Y-%m-%d %H:%M:%S"),
        );
    } else {
        println!(
            "[{}] New event signatures found found. 10 new lines example: {}",
            Local::now().format("%Y-%m-%d %H:%M:%S"),
            diff_df
        );
    }

    // Concatenate the existing and new dataframes
    let mut combined_df = if existing_df.height() > 0 {
        concat_dataframes(vec![existing_df.lazy(), diff_df.lazy()])?
    } else {
        new_df
    };

    utils::write_df_file(&mut combined_df, path)?;

    Ok(combined_df)
}

/// Processes all ABI files in a folder and combines them into a single DataFrame
///
/// # Arguments
/// * `abi_folder_path` - Path to the folder containing ABI files
///
/// # Returns
/// Returns a DataFrame containing the processed ABI information
///
/// # Errors
/// Returns an error if the path doesn't exist or if there are issues reading the files
pub fn read_new_abi_folder(abi_folder_path: &str) -> Result<DataFrame, AbiReaderError> {
    let abi_folder_path  = Path::new(abi_folder_path);
    if !abi_folder_path.exists() {
        return Err(AbiReaderError::InvalidPath(format!("Path does not exist: {}", abi_folder_path.display())));
    }

    let combined_df = if abi_folder_path.is_dir() {
        let paths = fs::read_dir(abi_folder_path)
            .map_err(|e| AbiReaderError::InvalidPath(e.to_string()))?;
        
        // Process each file and collect successful results
        let processed_frames: Vec<DataFrame> = paths
            .filter_map(|entry| {
                let path = entry.ok()?.path();
                if path.is_dir() {
                    return None;
                }
                match read_new_abi_file(path) {
                    Ok(df) => Some(df),
                    Err(_) => None,  // Silently skip invalid files
                }
            })
            .collect();
        
        // Handle case where no valid files were processed
        if processed_frames.is_empty() {
            return Ok(DataFrame::new(vec![
                Series::new_empty("address", &DataType::Binary),
                Series::new_empty("hash", &DataType::Binary),
                Series::new_empty("full_signature", &DataType::String),
                Series::new_empty("name", &DataType::String),
                Series::new_empty("anonymous", &DataType::Boolean),
                Series::new_empty("state_mutability", &DataType::String),
                Series::new_empty("id", &DataType::String),
            ])?);
        }
        
        // Combine all DataFrames
        let mut combined_df = processed_frames[0].clone();
        for df in processed_frames.into_iter().skip(1) {
            // concatenate each file dataframe
            combined_df = combined_df.vstack(&df).map_err(AbiReaderError::PolarsError)?;
        }
        combined_df
    } else {
        read_new_abi_file(abi_folder_path.to_path_buf())?
    };

    Ok(combined_df)
}

/// Reads and processes a single ABI file
///
/// # Arguments
/// * `path` - Path to the ABI file
///
/// # Returns
/// Returns a DataFrame containing the processed ABI information
///
/// # Notes
/// The filename should be a valid contract address and needs to be a .json extension. 
/// The function will skip the file if it's not a .json or couldn't be parsed into an address by the extract_address_from_path function.
pub fn read_new_abi_file(path: PathBuf) -> Result<DataFrame, AbiReaderError> {
    let address = extract_address_from_path(&path);
    if let Some(address) = address {
        println!(
            "[{}] Reading ABI file: {:?}",
            Local::now().format("%Y-%m-%d %H:%M:%S"),
            path
        );

        let json = fs::read_to_string(&path).map_err(|e| AbiReaderError::InvalidAbiFile(e.to_string()))?;
        let abi: JsonAbi = serde_json::from_str(&json).map_err(|e| AbiReaderError::InvalidAbiFile(e.to_string()))?;
        // let a = Some(abi.events().map(|event| create_event_row(event)).collect());
        read_new_abi_json(abi, address)
    } else {
        //skip file if it's not a .json or couldn't be parsed into an address by the extract_address_from_path function
        println!(
            "[{}] Skipping ABI file: {:?}. It's not a .json or filename couldn't be parsed into an address",
            Local::now().format("%Y-%m-%d %H:%M:%S"),
            path
        );
        Err(AbiReaderError::InvalidAbiFile(
            "File is not a JSON format or filename couldn't be parsed into an address".to_string()
        ))
    }
}

/// Processes a parsed ABI JSON structure into a DataFrame
///
/// # Arguments
/// * `abi` - Parsed JsonAbi structure
/// * `address` - Contract address associated with the ABI
///
/// # Returns
/// Returns a DataFrame containing function and/or event signatures.
/// 
/// # Notes
/// This function gets the abi_read_mode from the config and uses it to filter the items to read.
pub fn read_new_abi_json(abi: JsonAbi, address: Address) -> Result<DataFrame, AbiReaderError>{
    let abi_read_mode = get_config().abi_reader.abi_read_mode;
    // inverted logic because we want to read all items except the ones specified in the abi_read_mode
    let function_rows: Vec<AbiItemRow> = if abi_read_mode != configger::AbiReadMode::Events {
        abi.functions().map(|function| create_function_row(function, address)).collect()
    } else {
        vec![]
    };
    let event_rows: Vec<AbiItemRow> = if abi_read_mode != configger::AbiReadMode::Functions {
        abi.events().map(|event| create_event_row(event, address)).collect()
    } else {
        vec![]
    };
    let abi_rows = [function_rows, event_rows].concat();
    
    create_dataframe_from_rows(abi_rows)
}

/// Auxiliary function to extract an Ethereum address from a file path
///
/// # Arguments
/// * `path` - Path to process
///
/// # Returns
/// Returns Some(Address) if the filename (without extension) is a valid Ethereum address,
/// None otherwise
fn extract_address_from_path(path: &Path) -> Option<Address> {
    path.extension().and_then(|s| s.to_str()).filter(|&ext| ext == "json")
        .and_then(|_| path.file_stem())
        .and_then(|s| s.to_str())
        .and_then(|str| Address::from_str(str).ok())
}

/// Creates an AbiItemRow from an Event
///
/// # Arguments
/// * `event` - An alloy "Event" to process
/// * `address` - Contract address associated with the event
///
/// # Returns
/// Returns an AbiItemRow containing the event information
/// 
/// # Notes
/// The function takes the unique_key from the config and uses it to create the id. 
/// Later on, the id is used to filter unique entries in the database.
fn create_event_row(event: &alloy::json_abi::Event, address: Address) -> AbiItemRow {
    let unique_key = get_config().abi_reader.unique_key;
    let mut id = event.selector().to_string();
    if unique_key.contains(&"full_signature".to_string()) {
        id = id + " - " + &event.full_signature()[..];
    }
    if unique_key.contains(&"address".to_string()) {
        id = id + " - " + address.to_string().as_str();
    }
    let event_row = AbiItemRow {
        address: address.0,
        hash: Hash::Hash32(event.selector()),
        full_signature: event.full_signature(),
        name: event.name.to_string(),
        anonymous: Some(event.anonymous),
        num_indexed_args: Some(event.num_topics()),
        state_mutability: None,
        id: id,
    };
    event_row
}

/// Creates an AbiItemRow from a Function
///
/// # Arguments
/// * `function` - An alloy "Function" to process
/// * `address` - Contract address associated with the function
///
/// # Returns
/// Returns an AbiItemRow containing the function information
/// 
/// # Notes
/// The function takes the unique_key from the config and uses it to create the id. 
/// Later on, the id is used to filter unique entries in the database.
fn create_function_row(function: &alloy::json_abi::Function, address: Address) -> AbiItemRow {
    let state_mutability = match function.state_mutability {
        alloy::json_abi::StateMutability::Pure => "pure".to_owned(),
        alloy::json_abi::StateMutability::View => "view".to_owned(),
        alloy::json_abi::StateMutability::NonPayable => "nonpayable".to_owned(),
        alloy::json_abi::StateMutability::Payable => "payable".to_owned(),
    };
    
    let unique_key = get_config().abi_reader.unique_key;
    let mut id = function.selector().to_string();
    if unique_key.contains(&"full_signature".to_string()) {
        id = id + " - " + &function.full_signature()[..];
    }
    if unique_key.contains(&"address".to_string()) {
        id = id + " - " + address.to_string().as_str();
    }

    let function_row = AbiItemRow {
        address: address.0,
        hash: Hash::Hash4(function.selector()),
        full_signature: function.full_signature(),
        name: function.name.to_string(),
        anonymous: None,
        num_indexed_args: None,
        state_mutability: Some(state_mutability),
        id: id
    };
    function_row
}

/// Converts a vector of AbiItemRows into a DataFrame
///
/// # Arguments
/// * `rows` - Vector of AbiItemRows to convert
///
/// # Returns
/// Returns a DataFrame containing the ABI information
///
/// # Notes
/// The output format (binary/hex) of some columns is determined by configuration
fn create_dataframe_from_rows(rows: Vec<AbiItemRow>) -> Result<DataFrame, AbiReaderError> {
    let columns = vec![
        Series::new("address".into(), rows.iter().map(|r| r.address.as_slice().to_vec()).collect::<Vec<Vec<u8>>>()),
        Series::new("hash".into(), rows.iter().map(|r| r.hash.as_bytes()).collect::<Vec<Vec<u8>>>()),
        Series::new("full_signature".into(), rows.iter().map(|r| r.full_signature.clone()).collect::<Vec<String>>()),
        Series::new("name".into(), rows.iter().map(|r| r.name.clone()).collect::<Vec<String>>()),
        Series::new("anonymous".into(), rows.iter().map(|r| r.anonymous).collect::<Vec<Option<bool>>>()),
        Series::new("num_indexed_args".into(), rows.iter().map(|r| r.num_indexed_args.map(|n| n as u32)).collect::<Vec<Option<u32>>>()),
        Series::new("state_mutability".into(), rows.iter().map(|r| r.state_mutability.clone()).collect::<Vec<Option<String>>>()),
        Series::new("id".into(), rows.iter().map(|r| r.id.clone()).collect::<Vec<String>>()),
    ];

    let df = DataFrame::new(columns).map_err(AbiReaderError::PolarsError)?;
    Ok(if get_config().abi_reader.output_hex_string_encoding {
        utils::binary_columns_to_hex_string(df)?
    } else {
        df
    })
}

/// Auxiliary function to concatenate multiple DataFrames while maintaining unique IDs
///
/// # Arguments
/// * `dfs` - Vector of LazyFrames to concatenate
///
/// # Returns
/// Returns a combined DataFrame with duplicate IDs removed
fn concat_dataframes(dfs: Vec<LazyFrame>) -> Result<DataFrame, AbiReaderError> {
    let df = concat(dfs, UnionArgs::default())?;
    let df = df.unique(Some(vec!["id".to_string()]), UniqueKeepStrategy::First).collect();
    df.map_err(AbiReaderError::PolarsError)
}