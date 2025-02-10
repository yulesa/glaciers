//! Miscellaneous functions for the Glaciers.
//! 
//! This module provides miscellaneous functions that are not part of the main functionality of the Glaciers.
//! 
//! The module provides the following functions:
//!  - decode_df_using_single_contract: Decodes a DataFrame with only a single contract address, by downloading the ABI from Sourcify.

use std::str::FromStr;
use reqwest::Client;
use alloy::{json_abi::JsonAbi, primitives::Address};
use polars::prelude::*;
use thiserror::Error;

use crate::abi_reader;
use crate::decoder::{self, DecoderType};

/// Error types that can occur during miscellaneous operations
#[derive(Error, Debug)]
pub enum MiscellaneousError {
    #[error("Unable to download ABI from Sourcify, Reqwest error: {0}")]
    ReqwestError(#[from] reqwest::Error),
    #[error("Unable to download ABI from Sourcify, invalid JSON response: {0}")]
    InvalidJsonResponse(String),
    #[error("Alloy error, invalid address: {0}")]
    InvalidAddress(String),
    #[error("Abi reader error: {0}")]
    AbiReaderError(#[from] abi_reader::AbiReaderError),
    #[error("Decoder error: {0}")]
    DecoderError(#[from] decoder::DecoderError),
}

/// Decodes a DataFrame with only a single contract address, by downloading the ABI from Sourcify.
/// 
/// # Arguments
/// * `df` - The DataFrame to decode
/// * `contract_address` - The contract address to decode
/// * `decoder_type` - The type of decoder to use
///
/// # Returns
/// * If successful, a DataFrame with the decoded data.
/// 
/// # Notes
/// - This is a shortcuting function that automatically downloads the ABI from Sourcify, reads it and decodes the DataFrame.
/// - Nevertheless, we recommend following the normal flow and creating the ABI DB first.
pub async fn decode_df_using_single_contract(df: DataFrame, contract_address: String, decoder_type: DecoderType) -> Result<DataFrame, MiscellaneousError> {
    // Download the ABI from Sourcify
    let client = Client::new();
    let response = client
        .get(format!("https://repo.sourcify.dev/contracts/partial_match/1/{}/metadata.json", contract_address))
        .send().await?;
    let json_response: serde_json::Value = response.json().await?;
    let abi_value = json_response
        .get("output")
        .ok_or(MiscellaneousError::InvalidJsonResponse(json_response.to_string()))?
        .get("abi")
        .ok_or(MiscellaneousError::InvalidJsonResponse(json_response.to_string()))?;
    let abi: JsonAbi = serde_json::from_str(&abi_value.to_string()).map_err(|e| MiscellaneousError::InvalidJsonResponse(e.to_string()))?;

    let contract_address = contract_address.to_lowercase();
    let address = Address::from_str(&contract_address).map_err(|e| MiscellaneousError::InvalidAddress(e.to_string()))?;

    let abi_df = abi_reader::read_new_abi_json(abi, address)?;
    let decoded_df = decoder::decode_df_with_abi_df(df, abi_df, decoder_type).await?;

    Ok(decoded_df)
}