use pyo3::prelude::*;
use pyo3_polars::PyDataFrame;
use polars::prelude::*;
use pyo3::exceptions::PyValueError;
use glaciers_decoder::decoder;
use glaciers_decoder::abi_reader;

/// Register in the Python module the functions tbelow hat can be called in Python
#[pymodule]
fn glaciers(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(polars_decode_logs, m)?)?;
    m.add_function(wrap_pyfunction!(read_abis_topic0, m)?)?;
    Ok(())
}

#[pyfunction]
pub fn read_abis_topic0(topic0_path: String, abi_folder_path: String) -> PyResult<PyDataFrame> {
    abi_reader::read_abis_topic0(topic0_path, abi_folder_path)
        .map_err(|e| PyValueError::new_err(format!("Error reading ABIs: {}", e)))
        .map(|df| PyDataFrame(df))
}

/// Decode blockchain event logs using ABI definitions
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