use pyo3::prelude::*;
use pyo3_polars::PyDataFrame;
use polars::prelude::*;
use pyo3::exceptions::PyValueError;
use glaciers_decoder::decoder;
use glaciers_decoder::abi_reader;

/// Register in the Python module the functions tbelow hat can be called in Python
#[pymodule]
#[pyo3(name = "_glaciers_rust")]
fn glaciers(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(read_abis_topic0, m)?)?;
    m.add_function(wrap_pyfunction!(decode_log_files, m)?)?;
    m.add_function(wrap_pyfunction!(decode_log_df, m)?)?;
    m.add_function(wrap_pyfunction!(polars_decode_logs, m)?)?;
    Ok(())
}

/// Reads ABIs (Application Binary Interface) in a folder and append to the topic0 parquet file
///
/// This function loads ABI definitions from a folder and append the new topic0 and event signatures
/// into an existing DataFrame (parquet file) of unique entries.
///
/// # Arguments
/// - `topic0_path`: Path to the parquet file containing the existing DataFrame.
/// - `abi_folder_path`: Path to the folder containing ABI JSON files
///
/// # Returns
/// A `PyResult` containing a `PyDataFrame` with all unique topic0 and event signatures
///
/// # Errors
/// Returns a `PyValueError` if there are issues reading or processing the ABIs

#[pyfunction]
pub fn read_abis_topic0(topic0_path: String, abi_folder_path: String) -> PyResult<PyDataFrame> {
    abi_reader::read_abis_topic0(topic0_path, abi_folder_path)
        .map_err(|e| PyValueError::new_err(format!("Error reading ABIs: {}", e)))
        .map(|df| PyDataFrame(df))
}

/// Decode a folder of logs in parquet format.
///
/// This function takes a logs folder path and a topic0 parquet file path and decode it
/// to a decoded logs' parquet files
///
/// # Arguments
/// - `log_folder_path`: Path to a folder containing the logs parquet files
/// - `topic0_path`: Path to the topic0 file containing the topic0 and event signatures
///
/// # Returns
/// No Return
///
/// # Errors
/// Returns a `PyValueError` if there are issues processing the logs
#[pyfunction]
pub fn decode_log_files(py: Python<'_>, log_folder_path: String, topic0_path: String) -> PyResult<&PyAny> {
    pyo3_asyncio::tokio::future_into_py(py, async move {
        decoder::process_log_files(log_folder_path, topic0_path).await
        .map_err(|e| PyValueError::new_err(format!("Decoding error: {}", e)))
    })
}

/// Decode a DataFrame of logs
///
/// This function takes a raw logs' DataFrame and a topic0 parquet file path and decode it
/// to a decoded logs' DataFrame.
///
/// # Arguments
/// - `logs_df`: A DataFrame containing raw blockchain logs
/// - `topic0_path`: Path to the topic0 file containing the topic0 and event signatures
///
/// # Returns
/// A `PyResult` containing a decoded logs' `PyDataFrame` or an error
///
/// # Errors
/// Returns a `PyValueError` if there are issues processing the logs
#[pyfunction]
pub fn decode_log_df(py: Python<'_>, logs_df: PyDataFrame, topic0_path: String) -> PyResult<&PyAny> {
    // Convert PyDataFrame to native polars DataFrame
    let logs_df: DataFrame = logs_df.into();
    let result = pyo3_asyncio::tokio::future_into_py(py, async move {
        match decoder::process_log_df(logs_df, topic0_path).await{
            Ok(df) => Ok(PyDataFrame(df)),
            Err(e) => Err(PyValueError::new_err(format!("Decoding error: {}", e))),
        }
    })?;
    Ok(result)
}

/// Decode dataframe event logs using ABI definitions dataframe
///
/// Args:
///     logs_df: A polars DataFrame containing the raw logs with topic0, topic1, topic2, topic3, and data columns
///     abi_df: A polars DataFrame containing:
///         - topic0: The topic0 (event signature hash) as bytes
///         - full_signature: The full event signature as string (e.g. "Transfer(address indexed from, address indexed to, uint256 value)")
///
/// Returns:
///     A polars DataFrame containing the decoded events with additional columns:
///     - event_values: The decoded parameter values
///     - event_keys: The parameter names
///     - event_json: JSON representation of the decoded event
#[pyfunction]
pub fn polars_decode_logs(logs_df: PyDataFrame, abi_df: PyDataFrame) -> PyResult<PyDataFrame> {
    // Convert PyDataFrame to native polars DataFrame
    let logs_df: DataFrame = logs_df.into();
    let abi_df: DataFrame = abi_df.into();
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