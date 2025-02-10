//! Utility functions for the Glaciers.
//! 
//! This module provides utility functions that are not part of the main functionality of the Glaciers.
//! 
//! The module provides the following functions:
//!  - binary_columns_to_hex_string: Converts binary columns to hex string columns.
//!  - hex_string_columns_to_binary: Converts hex string columns to binary columns.
//!  - abi_df_hex_string_columns_to_binary: Converts hex string columns to binary columns in an ABI DataFrame.

use std::{ffi::OsStr, fs::File, path::Path};
use polars::{error::ErrString, prelude::*};
use alloy::dyn_abi::DynSolValue;
use crate::configger::{self, get_config};
use crate::decoder::DecoderType;

/// Converts binary columns to hex string columns. Used when outputting hex strings, instead of binary.
/// 
/// # Arguments
/// * `df` - The DataFrame to convert
/// 
/// # Returns
/// * If successful, a DataFrame with the converted columns.
pub fn binary_columns_to_hex_string(df: DataFrame) -> Result<DataFrame, PolarsError> {
    // Get names of binary columns
    let binary_cols: Vec<String> = df.schema()
        .iter()
        .filter(|(_, dtype)| matches!(dtype, DataType::Binary))
        .map(|(name, _)| name.to_string())
        .collect();

    // Return early if no binary columns
    if binary_cols.is_empty() {
        return Ok(df.clone());
    }

    // Create hex encode expressions
    let hex_exprs: Vec<Expr> = binary_cols.iter()
        .map(|name| col(name).binary().hex_encode().alias(name))
        .collect();

    // Create 0x prefix expressions
    let prefix_exprs: Vec<Expr> = binary_cols.iter()
        .map(|name| concat_str([lit("0x"), col(name)], "", true).alias(name))
        .collect();

    // Apply transformations
    df.lazy()
        .with_columns(hex_exprs)
        .with_columns(prefix_exprs)
        .collect()
}

/// Converts columns from logs/traces dataframes from hex string to binary columns.
/// Only the necessary columns are converted, based on the input schema in the configs.
/// 
/// # Arguments
/// * `df` - The DataFrame to convert
/// * `decoder_type` - The type of decoder to use
/// 
/// # Returns
/// * If successful, a DataFrame with the converted columns.
pub fn hex_string_columns_to_binary(df: DataFrame, decoder_type: &DecoderType) -> Result<DataFrame, PolarsError> {
    let (input_schema_datatype, input_schema_alias) = match decoder_type {
        DecoderType::Log => (get_config().log_decoder.log_schema.log_datatype.as_array(), get_config().log_decoder.log_schema.log_alias.as_array()),
        DecoderType::Trace => (get_config().trace_decoder.trace_schema.trace_datatype.as_array(), get_config().trace_decoder.trace_schema.trace_alias.as_array()),
    };

    let bin_exprs: Vec<Expr> = input_schema_datatype
        .iter()
        .zip(input_schema_alias)
        .filter(|(f, _alias)| matches!(f, configger::DataType::HexString))
        .map(|(_f, alias)| col(alias.as_str()).str().strip_prefix(lit("0x")).str().hex_decode(true).alias(alias.as_str()))
        .collect();
    df.lazy().with_columns(bin_exprs).collect()   
}

/// Converts columns from hex string to binary columns if the ABI DB was saved as hex strings.
/// 
/// # Arguments
/// * `abi_df` - The DataFrame to convert
/// 
/// # Returns
/// * If successful, a DataFrame with the converted columns.
pub fn abi_df_hex_string_columns_to_binary(mut abi_df: DataFrame) -> Result<DataFrame, PolarsError> {
   // Convert hash and address columns to binary if they aren't already
   let columns_to_convert = ["hash", "address"];

   for col_name in columns_to_convert {
       if abi_df
           .column(col_name)?
           .dtype() != &DataType::Binary {
               abi_df = abi_df
                   .lazy()
                   .with_columns([
                       col(col_name)
                           .str()
                           .strip_prefix(lit("0x"))
                           .str()
                           .hex_decode(true)
                           .alias(col_name)
                   ])
                   .collect()?;
       }
   }
   Ok(abi_df)
}

/// Reads a DataFrame from a file.
/// 
/// # Arguments
/// * `path` - The path to the file to read
/// 
/// # Returns
/// * If successful, a DataFrame with the read data.
pub fn read_df_file(path: &Path) -> Result<DataFrame, PolarsError> {
    let path_ext = path.extension();
    if path_ext == Some(OsStr::new("parquet")) {
        ParquetReader::new(File::open(path).map_err(|e| PolarsError::ComputeError(ErrString::from(format!("Error opening path {}: {}" , path.display(), e.to_string()))))?)
            .finish()
    } else if path_ext == Some(OsStr::new("csv")) {
        CsvReader::new(File::open(path).map_err(|e| PolarsError::ComputeError(ErrString::from(format!("Error opening path {}: {}" , path.display(), e.to_string()))))?)
            .finish()
    } else {
        Err(PolarsError::ComputeError(ErrString::from(format!("In the path {}, a file extension was not provided (csv or parquet)", path.display()))))
    }
}

/// Writes a DataFrame to a file.
/// 
/// # Arguments
/// * `df` - The DataFrame to write
/// * `path` - The path to the file to write
/// 
/// # Returns
/// * If successful, a DataFrame with the read data.
pub fn write_df_file(df: &mut DataFrame, path: &Path) -> Result<(), PolarsError> {
    let mut file = File::create(path).map_err(|e| PolarsError::ComputeError(ErrString::from(e.to_string())))?;
    
    match path.extension().and_then(|ext| ext.to_str()) {
        Some("parquet") => ParquetWriter::new(&mut file).finish(df).map(|_| ()),
        Some("csv") => CsvWriter::new(&mut file).finish(df),
        _ => Err(PolarsError::ComputeError(ErrString::from(format!("In the path {}, a file extension was not provided (csv or parquet)", path.display()))))
    }?;
    Ok(())
}

/// Wrapper type around DynSolValue, to implement to_string function.
pub struct StrDynSolValue(DynSolValue);

impl StrDynSolValue {
    pub fn to_string(&self) -> Option<String> {
        match &self.0 {
            DynSolValue::Bool(b) => Some(b.to_string()),
            DynSolValue::Int(i, _) => Some(i.to_string()),
            DynSolValue::Uint(u, _) => Some(u.to_string()),
            DynSolValue::FixedBytes(w, _) => Some(format!("0x{}", w.to_string())),
            DynSolValue::Address(a) => Some(a.to_string()),
            DynSolValue::Function(f) => Some(f.to_string()),
            DynSolValue::Bytes(b) => Some(format!("0x{}", b.iter().map(|b| format!("{:02x}", b)).collect::<String>())),
            DynSolValue::String(s) => Some(s.clone()),
            DynSolValue::Array(arr) => Some(format!(
                "[{}]",
                arr.iter()
                    .filter_map(|v| Self::from(v.clone()).to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )),
            DynSolValue::FixedArray(arr) => Some(format!(
                "[{}]",
                arr.iter()
                    .filter_map(|v| Self::from(v.clone()).to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )),
            DynSolValue::Tuple(tuple) => Some(format!(
                "({})",
                tuple
                    .iter()
                    .filter_map(|v| Self::from(v.clone()).to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )),
        }
    }
}

impl From<DynSolValue> for StrDynSolValue {
    fn from(value: DynSolValue) -> Self {
        StrDynSolValue(value)
    }
}
