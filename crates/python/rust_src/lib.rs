use std::str::FromStr;
use std::path::PathBuf;
use alloy::primitives::Address;
use pyo3::prelude::*;
use pyo3_polars::PyDataFrame;
use polars::prelude::*;
use pyo3::exceptions::PyValueError;
use glaciers::decoder;
use glaciers::abi_reader;
use alloy::json_abi::JsonAbi;

/// Register in the Python module the functions tbelow hat can be called in Python
#[pymodule]
#[pyo3(name = "_glaciers_python")]
fn glaciers_python(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(update_abi_df, m)?)?;
    m.add_function(wrap_pyfunction!(read_new_abi_folder, m)?)?;
    m.add_function(wrap_pyfunction!(read_new_abi_file, m)?)?;
    m.add_function(wrap_pyfunction!(read_new_abi_json, m)?)?;
    m.add_function(wrap_pyfunction!(decode_log_folder, m)?)?;
    m.add_function(wrap_pyfunction!(decode_log_file, m)?)?;
    m.add_function(wrap_pyfunction!(decode_log_df, m)?)?;
    m.add_function(wrap_pyfunction!(decode_log_df_with_abi_df, m)?)?;
    m.add_function(wrap_pyfunction!(polars_decode_logs, m)?)?;
    Ok(())
}

/// Reads ABIs (Application Binary Interface) in a folder and append to the ABI parquet file
///
/// This function loads ABI definitions from a folder and append the new itens (functions and events)
/// into an existing DataFrame (parquet file) of unique entries.
///
/// # Arguments
/// - `abi_df_path`: Path to the parquet file containing the existing DataFrame.
/// - `abi_folder_path`: Path to the folder containing ABI JSON files
///
/// # Returns
/// A `PyResult` containing a `PyDataFrame` with all unique itens (functions and events)
///
/// # Errors
/// Returns a `PyValueError` if there are issues reading or processing the ABIs

#[pyfunction]
pub fn update_abi_df(abi_df_path: String, abi_folder_path: String) -> PyResult<PyDataFrame> {
    abi_reader::update_abi_df(abi_df_path, abi_folder_path)
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


/// Decode a folder of logs in parquet format.
///
/// This function takes a logs folder path and and abi parquet file path and decode it
/// to a decoded logs' parquet files
///
/// # Arguments
/// - `log_folder_path`: Path to a folder containing the logs parquet files
/// - `abi_df_path`: Path to the abi file containing the topic0 and event signatures
///
/// # Returns
/// No Return
///
/// # Errors
/// Returns a `PyValueError` if there are issues processing the logs
#[pyfunction]
pub fn decode_log_folder(py: Python<'_>, log_folder_path: String, abi_df_path: String) -> PyResult<&PyAny> {
    pyo3_asyncio::tokio::future_into_py(py, async move {
        decoder::decode_log_folder(log_folder_path, abi_df_path).await
        .map_err(|e| PyValueError::new_err(format!("Decoding error: {}", e)))
    })
}

/// Decode a log file
///
/// This function takes a log file path and a abi parquet file path and decode it
/// to a decoded logs' DataFrame.
///
/// # Arguments
/// - `log_file_path`: Path to the log file
/// - `abi_df_path`: Path to the abi file containing the topic0 and event signatures
///
/// # Returns
/// A `PyResult` containing a decoded logs' `PyDataFrame` or an error
///
/// # Errors
/// Returns a `PyValueError` if there are issues processing the logs
#[pyfunction]
pub fn decode_log_file(py: Python<'_>, log_file_path: String, abi_df_path: String) -> PyResult<&PyAny> {
    let log_file_path = PathBuf::from(log_file_path);
    let result = pyo3_asyncio::tokio::future_into_py(py, async move {
        match decoder::decode_log_file(log_file_path, abi_df_path).await {
            Ok(df) => Ok(PyDataFrame(df)),
            Err(e) => Err(PyValueError::new_err(format!("Decoding error: {}", e))),
        }
    })?;
    Ok(result)
}

/// Decode a DataFrame of logs
///
/// This function takes a raw logs' DataFrame and a abi parquet file path and decode it
/// to a decoded logs' DataFrame.
///
/// # Arguments
/// - `logs_df`: A DataFrame containing raw blockchain logs
/// - `abi_df_path`: Path to the abi file containing the topic0 and event signatures
///
/// # Returns
/// A `PyResult` containing a decoded logs' `PyDataFrame` or an error
///
/// # Errors
/// Returns a `PyValueError` if there are issues processing the logs
#[pyfunction]
pub fn decode_log_df(py: Python<'_>, logs_df: PyDataFrame, abi_df_path: String) -> PyResult<&PyAny> {
    // Convert PyDataFrame to native polars DataFrame
    let logs_df:DataFrame = logs_df.into();
    let result = pyo3_asyncio::tokio::future_into_py(py, async move {
        match decoder::decode_log_df(logs_df, abi_df_path).await {
            Ok(df) => Ok(PyDataFrame(df)),
            Err(e) => Err(PyValueError::new_err(format!("Decoding error: {}", e))),
        }
    })?;
    Ok(result)
}

/// Decode a DataFrame of logs using an ABI DataFrame
/// 
/// This function takes a raw logs' DataFrame and an ABI DataFrame and decode it
/// to a decoded logs' DataFrame.
///
/// # Arguments
/// - `logs_df`: A DataFrame containing raw blockchain logs
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
pub fn decode_log_df_with_abi_df(py: Python<'_>, logs_df: PyDataFrame, abi_df: PyDataFrame) -> PyResult<&PyAny> {
    // Convert PyDataFrame to native polars DataFrame
    let logs_df:DataFrame = logs_df.into();
    let abi_df:DataFrame = abi_df.into();
    let result = pyo3_asyncio::tokio::future_into_py(py, async move {
        match decoder::decode_log_df_with_abi_df(logs_df, abi_df).await {
            Ok(df) => Ok(PyDataFrame(df)),
            Err(e) => Err(PyValueError::new_err(format!("Decoding error: {}", e))),
        }
    })?;
    Ok(result)
}

/// Decode dataframe event logs using ABI definitions dataframe, without multi-threading
///
/// Args:
///     logs_df: A polars DataFrame containing the raw logs with topic0, topic1, topic2, topic3, and data columns
///     abi_df: A polars DataFrame containing:
///         - topic0: The topic0 (event signature hash) as bytes
///         - full_signature: The full event signature as string (e.g. "Transfer(addreds indexed from, address indexed to, uint256 value)")
///
/// Returns:
///     A polars DataFrame containing the decoded events with additional columns:
///     - event_values: The decoded parameter values
///     - event_keys: The parameter names
///     - event_json: JSON representation of the decoded event
#[pyfunction]
pub fn polars_decode_logs(logs_df: PyDataFrame, abi_df: PyDataFrame) -> PyResult<PyDataFrame> {
    // Convert PyDataFrame to native polars DataFrame
    let logs_df = DataFrame::from(logs_df);
    let abi_df = DataFrame::from(abi_df);
    let abi_df = abi_df.lazy().with_column(col("topic0")
            .str()
            .to_lowercase()
            .str()
            .strip_prefix(lit("0x"))
            .str()
            .hex_decode(true)
            .alias("topic0"))
        .collect()
        .map_err(|e| PyValueError::new_err(format!("Decoding error: {}", e)))?;

    // Join logs with ABI
    let logs_with_abi = logs_df
        .lazy()
        .join(
            abi_df.lazy(),
            [col("topic0")],
            [col("topic0")],
            JoinArgs::new(JoinType::Left),
        )
        .collect()
        .map_err(|e| PyValueError::new_err(format!("Join error: {}", e)))?;

    // Process the logs using our decoder
    let result = decoder::polars_decode_logs(logs_with_abi)
        .map_err(|e| PyValueError::new_err(format!("Decoding error: {}", e)))?;

    Ok(PyDataFrame(result))
}