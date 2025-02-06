use std::str::FromStr;
use std::path::PathBuf;
use alloy::primitives::Address;
use alloy::json_abi::JsonAbi;
use pyo3::prelude::*;
use pyo3::exceptions::PyValueError;
use pyo3_polars::PyDataFrame;
use polars::prelude::*;
use glaciers::abi_reader;
use glaciers::configger;
use glaciers::miscellaneous;
use glaciers::decoder::{self, DecoderType};

/// Register in the Python module the functions tbelow hat can be called in Python
#[pymodule]
#[pyo3(name = "_glaciers_python")]
fn glaciers_python(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(get_config, m)?)?;
    m.add_function(wrap_pyfunction!(set_config, m)?)?;
    m.add_function(wrap_pyfunction!(set_config_toml, m)?)?;
    m.add_function(wrap_pyfunction!(update_abi_db, m)?)?;
    m.add_function(wrap_pyfunction!(read_new_abi_folder, m)?)?;
    m.add_function(wrap_pyfunction!(read_new_abi_file, m)?)?;
    m.add_function(wrap_pyfunction!(read_new_abi_json, m)?)?;
    m.add_function(wrap_pyfunction!(decode_folder, m)?)?;
    m.add_function(wrap_pyfunction!(decode_file, m)?)?;
    m.add_function(wrap_pyfunction!(decode_df, m)?)?;
    m.add_function(wrap_pyfunction!(decode_df_with_abi_df, m)?)?;
    m.add_function(wrap_pyfunction!(decode_df_using_single_contract, m)?)?;
    Ok(())
}

/// Get the current GLACIERS_CONFIG as a TOML string
#[pyfunction]
pub fn get_config() -> PyResult<String> {
    let config = configger::get_config();
    toml::to_string_pretty(&config)
        .map_err(|e| PyValueError::new_err(format!("Error serializing config to TOML: {}", e)))
}

/// Set the current GLACIERS_CONFIG using field and value
/// 
/// # Arguments
/// - `field`: The field to set (in the format "section.field", e.g. "main.events_abi_db_file_path")
/// - `value`: The value to set the field to
///
/// # Returns
/// No return
///
/// # Errors
/// Returns a `PyValueError` if there are issues setting the GLACIERS_CONFIG
#[pyfunction]
pub fn set_config(py: Python<'_>, field: String, value: PyObject) -> PyResult<()> {
    let value: configger::ConfigValue = value.extract(py)?;
    configger::set_config(&field, value)
        .map_err(|e| PyValueError::new_err(e.to_string())) 
}

/// Set the current GLACIERS_CONFIG using a TOML file
/// 
/// # Arguments
/// - `path`: The path to the TOML file
///
/// # Returns
/// No return
///
/// # Errors
/// Returns a `PyValueError` if there are issues setting the GLACIERS_CONFIG
#[pyfunction]
pub fn set_config_toml(path: String) -> PyResult<()> {
    configger::set_config_toml(&path)
        .map_err(|e| PyValueError::new_err(e.to_string()))
}

/// Reads ABIs (Application Binary Interface) in a folder and append to the ABI parquet file
///
/// This function loads ABI definitions from a folder and append the new itens (functions and events)
/// into an existing DataFrame (parquet file) of unique entries.
///
/// # Arguments
/// - `abi_db_path`: Path to the parquet file containing the existing DataFrame.
/// - `abi_folder_path`: Path to the folder containing ABI JSON files
///
/// # Returns
/// A `PyResult` containing a `PyDataFrame` with all unique itens (functions and events)
///
/// # Errors
/// Returns a `PyValueError` if there are issues reading or processing the ABIs

#[pyfunction]
pub fn update_abi_db(abi_db_path: String, abi_folder_path: String) -> PyResult<PyDataFrame> {
    abi_reader::update_abi_db(abi_db_path, abi_folder_path)
        .map_err(|e| PyValueError::new_err(format!("Error reading ABIs: {}", e)))
        .map(|df| PyDataFrame(df))
}

/// Reads ABIs (Application Binary Interface) from a folder
///
/// This function loads ABI definitions from a folder and creates a DataFrame containing
/// all functions and events found in the ABI files.
///
/// # Arguments
/// - `abi_folder_path`: Path to the folder containing ABI JSON files
///
/// # Returns
/// A `PyResult` containing a `PyDataFrame` with all functions and events
///
/// # Errors
/// Returns a `PyValueError` if there are issues reading or processing the ABIs
#[pyfunction]
pub fn read_new_abi_folder(abi_folder_path: String) -> PyResult<PyDataFrame> {
    abi_reader::read_new_abi_folder(&abi_folder_path)
        .map_err(|e| PyValueError::new_err(format!("Error reading ABI folder: {}", e)))
        .map(|df| PyDataFrame(df))
}

/// Reads an ABI (Application Binary Interface) from a file
///
/// This function loads an ABI definition from a JSON file and creates a DataFrame
/// containing all functions and events found in the ABI.
///
/// # Arguments
/// - `path`: Path to the ABI JSON file
///
/// # Returns
/// A `PyResult` containing a `PyDataFrame` with all functions and events
///
/// # Errors
/// Returns a `PyValueError` if there are issues reading or processing the ABI
#[pyfunction]
pub fn read_new_abi_file(path: String) -> PyResult<PyDataFrame> {
    abi_reader::read_new_abi_file(PathBuf::from(path))
        .map_err(|e| PyValueError::new_err(format!("Error reading ABI file: {}", e)))
        .map(|df| PyDataFrame(df))
}

/// Reads functions and events from an ABI JSON (Application Binary Interface)
///
/// This function processes an ABI JSON definition and creates a DataFrame containing
/// all functions and events found in the ABI.
///
/// # Arguments
/// - `abi`: JSON string containing the ABI definition
/// - `address`: Contract address as a hex string
///
/// # Returns
/// A `PyResult` containing a `PyDataFrame` with all functions and events
///
/// # Errors
/// Returns a `PyValueError` if there are issues processing the ABI
#[pyfunction]
pub fn read_new_abi_json(abi: String, address: String) -> PyResult<PyDataFrame> {
    let abi: JsonAbi = serde_json::from_str(&abi)
        .map_err(|e| PyValueError::new_err(format!("Invalid ABI JSON: {}", e)))?;
    let address = Address::from_str(&address)
        .map_err(|e| PyValueError::new_err(format!("Invalid address: {}", e)))?;
    abi_reader::read_new_abi_json(abi, address)
        .map_err(|e| PyValueError::new_err(format!("Error processing ABI: {}", e)))
        .map(|df| PyDataFrame(df))
}


/// Decode a folder of logs/traces   in parquet format.
///
/// This function takes a logs/traces folder path and and abi parquet file path and decode it
/// to a decoded logs/traces' parquet files
///
/// # Arguments
/// - `decoder_type`: Type of the decoder to use, allowed values = ["log", "trace"]
/// - `folder_path`: Path to a folder containing the logs/traces parquet files
/// - `abi_db_path`: Path to the abi file containing the topic0 and event signatures
///
/// # Returns
/// No Return
///
/// # Errors
/// Returns a `PyValueError` if there are issues processing the logs
#[pyfunction]
pub fn decode_folder(py: Python<'_>, decoder_type: String, folder_path: String, abi_db_path: String) -> PyResult<&PyAny> {
    let decoder_type = match decoder_type.as_str() {
        "log" => DecoderType::Log,
        "trace" => DecoderType::Trace,
        _ => return Err(PyValueError::new_err("Invalid decoder type")),
    };
    pyo3_asyncio::tokio::future_into_py(py, async move {
        decoder::decode_folder(folder_path, abi_db_path, decoder_type).await
        .map_err(|e| PyValueError::new_err(format!("Decoding error: {}", e)))
    })
}

/// Decode a log/trace file
///
/// This function takes a log/trace file path and a abi parquet file path and decode it
/// to a decoded logs/traces' DataFrame.
///
/// # Arguments
/// - `decoder_type`: Type of the decoder to use, allowed values = ["log", "trace"]
/// - `file_path`: Path to the log/trace file
/// - `abi_db_path`: Path to the abi file containing the topic0 and event signatures
///
/// # Returns
/// A `PyResult` containing a decoded logs' `PyDataFrame` or an error
///
/// # Errors
/// Returns a `PyValueError` if there are issues processing the logs
#[pyfunction]
pub fn decode_file(py: Python<'_>, decoder_type: String, file_path: String, abi_db_path: String) -> PyResult<&PyAny> {
    let decoder_type = match decoder_type.as_str() {
        "log" => DecoderType::Log,
        "trace" => DecoderType::Trace,
        _ => return Err(PyValueError::new_err("Invalid decoder type")),
    };
    let file_path = PathBuf::from(file_path);
    let result = pyo3_asyncio::tokio::future_into_py(py, async move {
        match decoder::decode_file(file_path, abi_db_path, decoder_type).await {
            Ok(df) => Ok(PyDataFrame(df)),
            Err(e) => Err(PyValueError::new_err(format!("Decoding error: {}", e))),
        }
    })?;
    Ok(result)
}

/// Decode a DataFrame of logs/traces
///
/// This function takes a raw logs/traces' DataFrame and a abi parquet file path and decode it
/// to a decoded logs/traces' DataFrame.
///
/// # Arguments
/// - `decoder_type`: Type of the decoder to use, allowed values = ["log", "trace"]
/// - `df`: A DataFrame containing raw blockchain logs/traces
/// - `abi_db_path`: Path to the abi file containing the topic0 and event signatures
///
/// # Returns
/// A `PyResult` containing a decoded logs/traces' `PyDataFrame` or an error
///
/// # Errors
/// Returns a `PyValueError` if there are issues processing the logs
#[pyfunction]
pub fn decode_df(py: Python<'_>, decoder_type: String, df: PyDataFrame, abi_db_path: String) -> PyResult<&PyAny> {
    let decoder_type = match decoder_type.as_str() {
        "log" => DecoderType::Log,
        "trace" => DecoderType::Trace,
        _ => return Err(PyValueError::new_err("Invalid decoder type")),
    };
    // Convert PyDataFrame to native polars DataFrame
    let df:DataFrame = df.into();
        let result = pyo3_asyncio::tokio::future_into_py(py, async move {
            match decoder::decode_df(df, abi_db_path, decoder_type).await {
            Ok(df) => Ok(PyDataFrame(df)),
            Err(e) => Err(PyValueError::new_err(format!("Decoding error: {}", e))),
        }
    })?;
    Ok(result)
}

/// Decode a DataFrame of logs/traces using an ABI DataFrame
/// 
/// This function takes a raw logs/traces' DataFrame and an ABI DataFrame and decode it
/// to a decoded logs/traces' DataFrame.
///
/// # Arguments
/// - `decoder_type`: Type of the decoder to use, allowed values = ["log", "trace"]
/// - `df`: A DataFrame containing raw blockchain logs/traces
/// - `abi_df`: A DataFrame containing:
///         - topic0: The topic0 (event signature hash) as bytes
///         - full_signature: The full event signature as string (e.g. "Transfer(address indexed from, address indexed to, uint256 value)")
///
/// # Returns
/// A `PyResult` containing a decoded logs' `PyDataFrame` or an error
///
/// # Errors
/// Returns a `PyValueError` if there are issues processing the logs
#[pyfunction]
pub fn decode_df_with_abi_df(py: Python<'_>, decoder_type: String, df: PyDataFrame, abi_df: PyDataFrame) -> PyResult<&PyAny> {
    let decoder_type = match decoder_type.as_str() {
        "log" => DecoderType::Log,
        "trace" => DecoderType::Trace,
        _ => return Err(PyValueError::new_err("Invalid decoder type")),
    };
    // Convert PyDataFrame to native polars DataFrame
    let df:DataFrame = df.into();
    let abi_df:DataFrame = abi_df.into();
    let result = pyo3_asyncio::tokio::future_into_py(py, async move {
        match decoder::decode_df_with_abi_df(df, abi_df, decoder_type).await {
            Ok(df) => Ok(PyDataFrame(df)),
            Err(e) => Err(PyValueError::new_err(format!("Decoding error: {}", e))),
        }
    })?;
    Ok(result)
}

/// Decode a DataFrame of logs/traces using a single contract address
///
/// This function takes a raw logs/traces' DataFrame and a contract address, download the ABI from Sourcify
/// and decode it to a decoded logs/traces' DataFrame.
///
/// # Arguments
/// - `decoder_type`: Type of the decoder to use, allowed values = ["log", "trace"]
/// - `df`: A DataFrame containing raw blockchain logs/traces
/// - `contract_address`: The contract address as a hex string
///
/// # Returns
/// A `PyResult` containing a decoded logs' `PyDataFrame` or an error
///
/// # Errors
/// Returns a `PyValueError` if there are issues processing the logs
#[pyfunction]
pub fn decode_df_using_single_contract(py: Python<'_>, decoder_type: String, df: PyDataFrame, contract_address: String) -> PyResult<&PyAny> {
    let decoder_type = match decoder_type.as_str() {
        "log" => DecoderType::Log,
        "trace" => DecoderType::Trace,
        _ => return Err(PyValueError::new_err("Invalid decoder type")),
    };
    // Convert PyDataFrame to native polars DataFrame
    let df = DataFrame::from(df);
    let result = pyo3_asyncio::tokio::future_into_py(py, async move {
        match miscellaneous::decode_df_using_single_contract(df, contract_address, decoder_type).await {
            Ok(df) => Ok(PyDataFrame(df)),
            Err(e) => Err(PyValueError::new_err(format!("Decoding error: {}", e))),
        }
    })?;

    Ok(result)
}