use std::str::FromStr;
use reqwest::Client;
use alloy::{json_abi::JsonAbi, primitives::Address};
use polars::prelude::*;
use thiserror::Error;

use crate::abi_reader;
use crate::decoder::{self, DecoderType};

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