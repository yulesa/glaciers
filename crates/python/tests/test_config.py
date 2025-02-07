import pytest
import toml
from glaciers import get_config, set_config, set_config_toml
import os

def test_default_config():
    """Test getting default configuration values"""
    config = get_config()
    config = toml.loads(config)
    # Test getting some default values
    assert config["glaciers"]["preferred_dataframe_type"] == "Polars"
    assert config["glaciers"]["unnesting_hex_string_encoding"] == False
    assert config["main"]["events_abi_db_file_path"] == "ABIs/ethereum__events__abis.parquet"
    assert config["main"]["functions_abi_db_file_path"] == "ABIs/ethereum__functions__abis.parquet"
    assert config["main"]["abi_folder_path"] == "ABIs/abi_database"
    assert config["main"]["raw_logs_folder_path"] == "data/logs"
    assert config["main"]["raw_traces_folder_path"] == "data/traces"
    assert config["abi_reader"]["abi_read_mode"] == "Events"
    assert config["abi_reader"]["unique_key"] == ["hash", "full_signature", "address"]
    assert config["decoder"]["algorithm"] == "Hash"
    assert config["decoder"]["output_hex_string_encoding"] == False
    assert config["decoder"]["output_file_format"] == "parquet"
    assert config["decoder"]["max_concurrent_files_decoding"] == 16
    assert config["decoder"]["max_chunk_threads_per_file"] == 16
    assert config["decoder"]["decoded_chunk_size"] == 500000
    assert config["log_decoder"]["log_schema"]["log_alias"] == {"topic0": "topic0", "topic1": "topic1", "topic2": "topic2", "topic3": "topic3", "data": "data", "address": "address"}
    assert config["log_decoder"]["log_schema"]["log_datatype"] == {"topic0": "Binary", "topic1": "Binary", "topic2": "Binary", "topic3": "Binary", "data": "Binary", "address": "Binary"}
    assert config["trace_decoder"]["trace_schema"]["trace_alias"] == {"selector": "selector", "action_input": "action_input", "result_output": "result_output", "action_to": "action_to"}
    assert config["trace_decoder"]["trace_schema"]["trace_datatype"] == {"selector": "Binary", "action_input": "Binary", "result_output": "Binary", "action_to": "Binary"}

def test_set_config_toml():
    """Test loading configuration from TOML file"""
    # Create a temporary test config file
    test_config = """
    [glaciers]
    preferred_dataframe_type = "pandas"
    unnesting_hex_string_encoding = true

    [decoder]
    algorithm = "hash_address"
    max_concurrent_files_decoding = 8
    """
    
    with open("test_config.toml", "w") as f:
        f.write(test_config)

    # Load the test config
    set_config_toml("test_config.toml")
    config = get_config()
    config = toml.loads(config)
    # Verify the values were updated
    assert config["glaciers"]["preferred_dataframe_type"] == "Pandas"
    assert config["glaciers"]["unnesting_hex_string_encoding"] == True
    assert config["decoder"]["algorithm"] == "HashAddress"
    assert config["decoder"]["max_concurrent_files_decoding"] == 8

    # Clean up
    os.remove("test_config.toml")

def test_set_config():
    """Test setting individual configuration values"""
    # Test setting single values
    set_config("glaciers.preferred_dataframe_type", "pandas")
    config = get_config()
    config = toml.loads(config)
    assert config["glaciers"]["preferred_dataframe_type"] == "Pandas"

    set_config("decoder.max_concurrent_files_decoding", 4)
    config = get_config()
    config = toml.loads(config)
    assert config["decoder"]["max_concurrent_files_decoding"] == 4

    set_config("decoder.output_hex_string_encoding", True)
    config = get_config()
    config = toml.loads(config)
    assert config["decoder"]["output_hex_string_encoding"] == True

def test_invalid_config():
    """Test error handling for invalid configurations"""
    # Test invalid value type
    with pytest.raises(ValueError):
        set_config("decoder.max_concurrent_files_decoding", "invalid")
        config = get_config()
        config = toml.loads(config)
        assert config["decoder"]["max_concurrent_files_decoding"] == "invalid"

    # Test invalid dataframe type
    with pytest.raises(ValueError):
        set_config("glaciers.preferred_dataframe_type", "invalid")
        config = get_config()
        config = toml.loads(config)
        assert config["glaciers"]["preferred_dataframe_type"] == "invalid"

def test_nested_config():
    """Test getting nested configuration values"""
    # Test getting nested schema values
    config = get_config()
    config = toml.loads(config)
    log_alias = config["log_decoder"]["log_schema"]["log_alias"]
    assert log_alias["topic0"] == "topic0"
    assert log_alias["data"] == "data"

    config = get_config()
    config = toml.loads(config)
    trace_datatype = config["trace_decoder"]["trace_schema"]["trace_datatype"]
    assert trace_datatype["selector"] == "Binary"
    assert trace_datatype["action_input"] == "Binary" 