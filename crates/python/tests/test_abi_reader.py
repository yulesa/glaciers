import pytest
import polars as pl
import pandas as pd
import json
from pathlib import Path
import os
from glaciers import (
    read_new_abi_json,
    read_new_abi_file,
    read_new_abi_folder,
    update_abi_db
)

@pytest.fixture
def sample_abi():
    return """
        [{
            "anonymous": false,
            "inputs": [
                {
                    "indexed": true,
                    "internalType": "address",
                    "name": "from",
                    "type": "address"
                },
                {
                    "indexed": true,
                    "internalType": "address",
                    "name": "to",
                    "type": "address"
                },
                {
                    "indexed": false,
                    "internalType": "uint256",
                    "name": "value",
                    "type": "uint256"
                }
            ],
            "name": "Transfer",
            "type": "event"
        }]
    """

@pytest.fixture
def sample_address():
    return "0xE672E0E0101A7F58d728751E2a5e6Da5Ff1FDa64"

def test_read_new_abi_json_returns_dataframe(sample_abi, sample_address):
    df = read_new_abi_json(sample_abi, sample_address)
    assert isinstance(df, (pl.DataFrame, pd.DataFrame))
    assert not df.is_empty()
    assert "address" in df.columns
    assert "name" in df.columns
    assert len(df) >= 1
    # Verify Transfer event details
    assert "Transfer" in df["name"].to_list()

def test_read_new_abi_file(tmp_path, sample_abi):
    # Create temporary ABI file
    abi_file = tmp_path / "0xE672E0E0101A7F58d728751E2a5e6Da5Ff1FDa64.json"
    abi_content = json.loads(sample_abi)
    abi_file.write_text(json.dumps(abi_content))
    
    df = read_new_abi_file(str(abi_file))
    assert isinstance(df, (pl.DataFrame, pd.DataFrame))
    assert not df.is_empty()
    assert "Transfer" in df["name"].to_list()

def test_read_new_abi_folder(tmp_path, sample_abi):
    # Create ABI folder
    abi_folder = tmp_path / "abis"
    abi_folder.mkdir()
    
    # Create multiple ABI files with valid addresses
    addresses = [
        "0xE672E0E0101A7F58d728751E2a5e6Da5Ff1FDa64",
        "0x1234567890123456789012345678901234567890",
        "0xabcdefABCDEF1234567890abcdefABCDEF123456"
    ]
    
    for address in addresses:
        abi_file = abi_folder / f"{address}.json"
        abi_content = json.loads(sample_abi)
        abi_file.write_text(json.dumps(abi_content))
    
    df = read_new_abi_folder(str(abi_folder))
    assert isinstance(df, (pl.DataFrame, pd.DataFrame))
    assert not df.is_empty()
    assert len(df) == 3
    assert all("Transfer" in name for name in df["name"].to_list())

def test_update_abi_db(tmp_path, sample_abi):
    # Create ABI folder and DB path
    abi_folder = tmp_path / "abis"
    abi_folder.mkdir()
    db_path = tmp_path / "abi_db.parquet"
    
    # Create sample ABI file
    abi_file = abi_folder / "0xE672E0E0101A7F58d728751E2a5e6Da5Ff1FDa64.json"
    abi_content = json.loads(sample_abi)
    abi_file.write_text(json.dumps(abi_content))
    
    df = update_abi_db(str(db_path), str(abi_folder))
    assert isinstance(df, (pl.DataFrame, pd.DataFrame))
    assert not df.is_empty()
    assert db_path.exists()
    assert "Transfer" in df["name"].to_list() 