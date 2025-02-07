import pytest
import polars as pl
import pandas as pd
from glaciers import (
    decode_df,
    decode_df_with_abi_df,
    decode_df_using_single_contract
)

@pytest.fixture
def sample_logs_df():
    # Create sample logs DataFrame similar to what's in the e2e example
    return pl.DataFrame({
        "address": ["0xE672E0E0101A7F58d728751E2a5e6Da5Ff1FDa64"],
        "topics": [["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"]],
        "data": ["0x0000000000000000000000000000000000000000000000000de0b6b3a7640000"]
    })

@pytest.fixture
def sample_abi_df():
    # Create sample ABI DataFrame similar to what would be read from the parquet file
    return pl.DataFrame({
        "address": ["0xE672E0E0101A7F58d728751E2a5e6Da5Ff1FDa64"],
        "name": ["Transfer"],
        "full_signature": ["event Transfer(address indexed from, address indexed to, uint256 value)"],
        "topic0": ["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"]
    })

def test_decode_df_basic(sample_logs_df, tmp_path):
    # Create temporary ABI DB file
    abi_db_path = tmp_path / "events_abi.parquet"
    sample_abi_df().write_parquet(abi_db_path)
    
    result = decode_df("log", sample_logs_df, str(abi_db_path))
    assert isinstance(result, (pl.DataFrame, pd.DataFrame))
    assert not result.is_empty()
    assert "name" in result.columns
    assert "Transfer" in result["name"].to_list()

def test_decode_df_with_abi_df_basic(sample_logs_df, sample_abi_df):
    result = decode_df_with_abi_df("log", sample_logs_df, sample_abi_df)
    assert isinstance(result, (pl.DataFrame, pd.DataFrame))
    assert not result.is_empty()
    assert "name" in result.columns
    assert "Transfer" in result["name"].to_list()

def test_decode_df_using_single_contract(sample_logs_df):
    result = decode_df_using_single_contract(
        "log",
        sample_logs_df,
        "0xE672E0E0101A7F58d728751E2a5e6Da5Ff1FDa64"
    )
    assert isinstance(result, (pl.DataFrame, pd.DataFrame))
    assert not result.is_empty()

def test_invalid_decoder_type(sample_logs_df):
    with pytest.raises(ValueError, match="Decoder type must be one of"):
        decode_df("invalid_type", sample_logs_df) 