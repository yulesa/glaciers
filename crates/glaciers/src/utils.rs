use std::{ffi::OsStr, fs::File, path::Path};
use polars::{error::ErrString, prelude::*};
use alloy::dyn_abi::DynSolValue;
use crate::configger::{self, get_config};

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

pub fn hex_string_columns_to_binary(df: DataFrame) -> Result<DataFrame, PolarsError> {
    let input_schema_datatype = get_config().decoder.schema.datatype;
    let input_schema_alias = get_config().decoder.schema.alias;

    let bin_exprs: Vec<Expr> = input_schema_datatype.as_array()
        .iter()
        .zip(input_schema_alias.as_array())
        .filter(|(f, _alias)| matches!(f, configger::DataType::HexString))
        .map(|(_f, alias)| col(alias.as_str()).str().strip_prefix(lit("0x")).str().hex_decode(true).alias(alias.as_str()))
        .collect();
    let df = df.lazy().with_columns(bin_exprs).collect()?;
    Ok(df)

}

pub fn read_df_file(path: &Path) -> Result<DataFrame, PolarsError> {
    let path_ext = path.extension();
    if path_ext == Some(OsStr::new("parquet")) {
        ParquetReader::new(File::open(path)?)
            .finish()
    } else if path_ext == Some(OsStr::new("csv")) {
        CsvReader::new(File::open(path)?)
            .finish()
    } else {
        Err(PolarsError::ComputeError(ErrString::from("Invalid file extension")))
    }
}

pub fn write_df_file(df: &mut DataFrame, path: &Path) -> Result<(), PolarsError> {
    let mut file = File::create(path).map_err(|e| PolarsError::ComputeError(ErrString::from(e.to_string())))?;
    
    match path.extension().and_then(|ext| ext.to_str()) {
        Some("parquet") => ParquetWriter::new(&mut file).finish(df).map(|_| ()),
        Some("csv") => CsvWriter::new(&mut file).finish(df),
        _ => Err(PolarsError::ComputeError(ErrString::from("Invalid file extension")))
    }?;
    Ok(())
}

//Wrapper type around DynSolValue, to implement to_string function.
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
            DynSolValue::Bytes(b) => Some(format!("0x{}", String::from_utf8_lossy(b))),
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
