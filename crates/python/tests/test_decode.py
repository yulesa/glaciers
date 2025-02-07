import os
from os.path import dirname
import polars as pl
import pandas as pd
import toml
from io import StringIO
import pytest
from glaciers import (
    decode_df,
    decode_df_with_abi_df,
    decode_df_using_single_contract,
    get_config,
    set_config_toml,
    decode_folder,
    decode_file
)

@pytest.fixture
def setup_paths(tmp_path):
    # Get the project directory
    python_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    project_dir = dirname(dirname(python_dir))+"/"
    
    # Create temporary directories
    tmp_logs_dir = tmp_path / "logs"
    tmp_traces_dir = tmp_path / "traces"
    tmp_abi_dir = tmp_path / "abi"
    
    for dir in [tmp_logs_dir, tmp_traces_dir, tmp_abi_dir]:
        dir.mkdir()
    
    # Set config and get paths
    set_config_toml(project_dir+"glaciers_config_edit_example.toml")
    config = get_config()
    config = toml.load(StringIO(config))
    
    # Copy real files to temporary locations
    real_logs_dir = project_dir + config['main']['raw_logs_folder_path']
    real_traces_dir = project_dir + config['main']['raw_traces_folder_path']
    real_events_abi = project_dir + config['main']['events_abi_db_file_path']
    real_functions_abi = project_dir + config['main']['functions_abi_db_file_path']
    
    # Copy the first log and trace files
    if os.listdir(real_logs_dir):
        sample_log = os.path.join(real_logs_dir, os.listdir(real_logs_dir)[0])
        pl.read_parquet(sample_log).write_parquet(tmp_logs_dir / "sample_log.parquet")
    
    if os.listdir(real_traces_dir):
        sample_trace = os.path.join(real_traces_dir, os.listdir(real_traces_dir)[0])
        pl.read_parquet(sample_trace).write_parquet(tmp_traces_dir / "sample_trace.parquet")
    
    # Copy ABI files
    pl.read_parquet(real_events_abi).write_parquet(tmp_abi_dir / "events_abi.parquet")
    pl.read_parquet(real_functions_abi).write_parquet(tmp_abi_dir / "functions_abi.parquet")
    
    return {
        'events_abi_path': str(tmp_abi_dir/"events_abi.parquet"),
        'functions_abi_path': str(tmp_abi_dir/"functions_abi.parquet"),
        'logs_folder_path': str(tmp_logs_dir),
        'traces_folder_path': str(tmp_traces_dir)
    }

@pytest.fixture
def sample_logs_df(setup_paths):
    # Read the sample log file from the temporary folder
    logs_file = os.path.join(setup_paths['logs_folder_path'], "sample_log.parquet")
    return pl.read_parquet(logs_file)

@pytest.fixture
def sample_traces_df(setup_paths):
    # Read the sample trace file from the temporary folder
    traces_file = os.path.join(setup_paths['traces_folder_path'], "sample_trace.parquet")
    return pl.read_parquet(traces_file)

@pytest.fixture
def sample_events_abi_df(setup_paths):
    # Read the ABI DataFrame from the temporary parquet file
    return pl.read_parquet(setup_paths['events_abi_path'])

@pytest.fixture
def sample_functions_abi_df(setup_paths):
    # Read the ABI DataFrame from the temporary parquet file
    return pl.read_parquet(setup_paths['functions_abi_path'])



def test_decode_folder(setup_paths):
    # Test logs decoding
    decode_folder(
        decoder_type="log", 
        abi_db_path=setup_paths['events_abi_path'], 
        folder_path=setup_paths['logs_folder_path']
    )
    
    # Verify that decoded files were created
    decoded_folder = os.path.join(os.path.dirname(setup_paths['logs_folder_path']), "decoded")
    assert os.path.exists(decoded_folder)
    assert len(os.listdir(decoded_folder)) > 0
    
    # Test traces decoding
    decode_folder(
        decoder_type="trace", 
        abi_db_path=setup_paths['functions_abi_path'], 
        folder_path=setup_paths['traces_folder_path']
    )
    
    # Verify that decoded files were created
    decoded_folder = os.path.join(os.path.dirname(setup_paths['traces_folder_path']), "decoded")
    assert os.path.exists(decoded_folder)
    assert len(os.listdir(decoded_folder)) > 0

def test_decode_file(setup_paths):
    # Test log file decoding
    log_file = os.path.join(setup_paths['logs_folder_path'], os.listdir(setup_paths['logs_folder_path'])[0])
    decoded_logs = decode_file(
        decoder_type="log",
        file_path=log_file,
        abi_db_path=setup_paths['events_abi_path']
    )
    
    assert isinstance(decoded_logs, pl.DataFrame)
    assert not decoded_logs.is_empty()
    for col in ["event_keys", "event_values", "event_json"]:
        assert col in decoded_logs.columns
    
    # Test trace file decoding
    trace_file = os.path.join(setup_paths['traces_folder_path'], os.listdir(setup_paths['traces_folder_path'])[0])
    decoded_traces = decode_file(
        decoder_type="trace",
        file_path=trace_file,
        abi_db_path=setup_paths['functions_abi_path']
    )
    
    assert isinstance(decoded_traces, pl.DataFrame)
    assert not decoded_traces.is_empty()
    for col in ["input_keys", "input_values", "input_json", "output_keys", "output_values", "output_json"]:
        assert col in decoded_traces.columns 

def test_decode_df(sample_logs_df, sample_traces_df, setup_paths):    
    result = decode_df("log", sample_logs_df, abi_db_path=setup_paths['events_abi_path'])
    assert isinstance(result, (pl.DataFrame, pd.DataFrame))
    assert not result.is_empty()
    assert "name" in result.columns
    assert "Transfer" in result["name"].to_list()
    for col in ["event_keys", "event_values", "event_json"]:
        assert col in result.columns
    
    result = decode_df("trace", sample_traces_df, abi_db_path=setup_paths['functions_abi_path'])
    assert isinstance(result, (pl.DataFrame, pd.DataFrame))
    assert not result.is_empty()
    assert "name" in result.columns
    assert "transfer" in result["name"].to_list()
    for col in ["input_keys", "input_values", "input_json", "output_keys", "output_values", "output_json"]:
        assert col in result.columns

def test_decode_df_with_abi_df(sample_logs_df, sample_traces_df, sample_events_abi_df, sample_functions_abi_df):
    result = decode_df_with_abi_df("log", sample_logs_df, sample_events_abi_df)
    assert isinstance(result, (pl.DataFrame, pd.DataFrame))
    assert not result.is_empty()
    assert "name" in result.columns
    assert "Transfer" in result["name"].to_list()
    for col in ["event_keys", "event_values", "event_json"]:
        assert col in result.columns

    result = decode_df_with_abi_df("trace", sample_traces_df, sample_functions_abi_df)
    assert isinstance(result, (pl.DataFrame, pd.DataFrame))
    assert not result.is_empty()
    assert "name" in result.columns
    assert "transfer" in result["name"].to_list()
    for col in ["input_keys", "input_values", "input_json", "output_keys", "output_values", "output_json"]:
        assert col in result.columns

def test_decode_df_using_single_contract(sample_logs_df, sample_traces_df):
    result = decode_df_using_single_contract(
        "log",
        sample_logs_df,
        "0xE672E0E0101A7F58d728751E2a5e6Da5Ff1FDa64"
    )
    assert isinstance(result, (pl.DataFrame, pd.DataFrame))
    assert not result.is_empty()
    for col in ["event_keys", "event_values", "event_json"]:
        assert col in result.columns

    result = decode_df_using_single_contract(
        "trace",
        sample_traces_df,
        "0xE672E0E0101A7F58d728751E2a5e6Da5Ff1FDa64"
    )
    assert isinstance(result, (pl.DataFrame, pd.DataFrame))
    assert not result.is_empty()
    for col in ["input_keys", "input_values", "input_json", "output_keys", "output_values", "output_json"]:
        assert col in result.columns

def test_invalid_decoder_type(sample_logs_df):
    with pytest.raises(ValueError, match="Decoder type must be one of"):
        decode_df("invalid_type", sample_logs_df)