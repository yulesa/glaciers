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
    set_config("glaciers.preferred_dataframe_type", "polars")
    set_config("glaciers.preferred_dataframe_type", "pandas")
    set_config("glaciers.unnesting_hex_string_encoding", False)
    set_config("glaciers.unnesting_hex_string_encoding", True)
    set_config("glaciers.unnesting_hex_string_encoding", 0)
    set_config("glaciers.unnesting_hex_string_encoding", 1)
    set_config("main.events_abi_db_file_path", "ABIs/ethereum__abis.parquet")
    set_config("main.functions_abi_db_file_path", "ABIs/ethereum__abis.parquet")
    set_config("main.abi_folder_path", "ABIs")
    set_config("main.raw_logs_folder_path", "data")
    set_config("main.raw_traces_folder_path", "data")
    set_config("abi_reader.abi_read_mode", "Events")
    set_config("abi_reader.abi_read_mode", "Functions")
    set_config("abi_reader.abi_read_mode", "Both")
    set_config("abi_reader.output_hex_string_encoding", False)
    set_config("abi_reader.output_hex_string_encoding", True)
    set_config("abi_reader.output_hex_string_encoding", 0)
    set_config("abi_reader.output_hex_string_encoding", 1)
    set_config("abi_reader.unique_key", ["hash", "full_signature", "address"])
    set_config("abi_reader.unique_key", ["hash", "full_signature"])
    set_config("abi_reader.unique_key", "hash")
    set_config("abi_reader.unique_key", "full_signature")
    set_config("abi_reader.unique_key", "address")
    set_config("decoder.algorithm", "Hash")
    set_config("decoder.algorithm", "Hash_Address")
    set_config("decoder.output_hex_string_encoding", False)
    set_config("decoder.output_hex_string_encoding", True)
    set_config("decoder.output_hex_string_encoding", 0)
    set_config("decoder.output_hex_string_encoding", 1)
    set_config("decoder.output_file_format", "parquet")
    set_config("decoder.output_file_format", "csv")
    set_config("decoder.max_concurrent_files_decoding", 1)
    set_config("decoder.max_chunk_threads_per_file", 1)
    set_config("decoder.decoded_chunk_size", 1)
    set_config("log_decoder.log_schema.log_alias.topic0", "t0")
    set_config("log_decoder.log_schema.log_alias.topic1", "t1")
    set_config("log_decoder.log_schema.log_alias.topic2", "t2")
    set_config("log_decoder.log_schema.log_alias.topic3", "t3")
    set_config("log_decoder.log_schema.log_alias.data", "d")
    set_config("log_decoder.log_schema.log_alias.address", "event_address")
    set_config("log_decoder.log_schema.log_datatype.topic0", "HexString")
    set_config("log_decoder.log_schema.log_datatype.topic1", "HexString")
    set_config("log_decoder.log_schema.log_datatype.topic2", "HexString")
    set_config("log_decoder.log_schema.log_datatype.topic3", "HexString")
    set_config("log_decoder.log_schema.log_datatype.data", "HexString")
    set_config("log_decoder.log_schema.log_datatype.address", "HexString")
    set_config("trace_decoder.trace_schema.trace_alias.selector", "4bytes")
    set_config("trace_decoder.trace_schema.trace_alias.action_input", "input")
    set_config("trace_decoder.trace_schema.trace_alias.result_output", "output")
    set_config("trace_decoder.trace_schema.trace_alias.action_to", "to")
    set_config("trace_decoder.trace_schema.trace_datatype.selector", "HexString")
    set_config("trace_decoder.trace_schema.trace_datatype.action_input", "HexString")
    set_config("trace_decoder.trace_schema.trace_datatype.result_output", "HexString")
    set_config("trace_decoder.trace_schema.trace_datatype.action_to", "HexString")
    expected_config = '''
        [glaciers]
        preferred_dataframe_type = "Pandas"
        unnesting_hex_string_encoding = true

        [main]
        events_abi_db_file_path = "ABIs/ethereum__abis.parquet"
        functions_abi_db_file_path = "ABIs/ethereum__abis.parquet"
        abi_folder_path = "ABIs"
        raw_logs_folder_path = "data"
        raw_traces_folder_path = "data"

        [abi_reader]
        abi_read_mode = "Both"
        output_hex_string_encoding = true
        unique_key = ["address"]

        [decoder]
        algorithm = "HashAddress"
        output_hex_string_encoding = true
        output_file_format = "csv"
        max_concurrent_files_decoding = 1
        max_chunk_threads_per_file = 1
        decoded_chunk_size = 1

        [log_decoder.log_schema]
        log_alias = { topic0 = "t0", topic1 = "t1", topic2 = "t2", topic3 = "t3", data = "d", address = "event_address" }
        log_datatype = { topic0 = "HexString", topic1 = "HexString", topic2 = "HexString", topic3 = "HexString", data = "HexString", address = "HexString" }

        [trace_decoder.trace_schema]
        trace_alias = { selector = "4bytes", action_input = "input", result_output = "output", action_to = "to" }
        trace_datatype = { selector = "HexString", action_input = "HexString", result_output = "HexString", action_to = "HexString" }
    '''
    expected_config = toml.loads(expected_config)
    config = get_config()
    config = toml.loads(config)
    print(config)
    print(expected_config)
    assert config == expected_config

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
