import pytest
import polars as pl
import pandas as pd
from glaciers import unnest_event, unnest_trace

@pytest.fixture
def sample_decoded_df():
    return pl.DataFrame({
        "log_index": [18, 155],
        "transaction_index": [18, 86],
        "block_number": [18426296, 18426286],
        "transaction_hash": [bytes.fromhex("96591a6a4ea24492a745464a72e1520774d2f543ea28126d274705a1726c4021"),
                             bytes.fromhex("b94c76f0b52be1e077dddcd759a4bba7fc8976d21bd6ae0f3401cc6bedc1c263")],
        "address": [bytes.fromhex("c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"),
                   bytes.fromhex("dac17f958d2ee523a2206206994597c13d831ec7")],
        "topic0": [bytes.fromhex("ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")] * 2,
        "topic1": [bytes.fromhex("000000000000000000000000eedff72a683058f8ff531e8c98575f920430fdc5"),
                   bytes.fromhex("0000000000000000000000009c0fdf1e074a8db4b3bfc78cf4d48a5518b7a0ba")],
        "topic2": [bytes.fromhex("0000000000000000007a250d5630b4cf539739df2c5dacb4c659f2488d"),
                  bytes.fromhex("000000000000000000a9d1e08c7793af67e9d92fe308d5697fb81d3e43")],
        "topic3": [bytes.fromhex("000000000000000000000000000000000000000002bbedcb06ef7528"),
                  None],
        "data": [bytes.fromhex("0000000000000000000000000000000000000000000000000000000000000000"),
                bytes.fromhex("000000000000000000000000000000000000000000000000000002e3070e")],
        "chain_id": [1, 1],
        "n_data_bytes": [32, 32],
        "num_indexed_args": [3, 3],
        "full_signature": ["event Transfer(address indexed from, address indexed to, uint256 value)",
                           "event Transfer(address indexed from, address indexed to, uint256 value)"],
        "name": ["Transfer", "Transfer"],
        "anonymous": [False, False],
        "state_mutability": ["", ""],
        "id": ["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef - event Transfer(address indexed from, address indexed to, uint256 value) - 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
               "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef - event Transfer(address indexed from, address indexed to, uint256 value) - 0xdAC17F958D2ee523a2206206994597C13D831ec7"],
        "event_values": ['["0xeEDfF72A683058F8FF531e8c98575f920430FdC5", "0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D", "3151936770479715624"]',
                        '["0x9C0fdF1e074A8DB4b3bfC78cf4d48A5518B7a0BA", "0xA9D1e08C7793af67e9d92fe308d5697FB81d3E43", "774926382"]'],
        "event_keys": ['["from", "to", "value"]', '["from", "to", "value"]'],
        "event_json": ['[{"name":"from","index":0,"value_type":"address","value":"0xeEDfF72A683058F8FF531e8c98575f920430FdC5"},{"name":"to","index":1,"value_type":"address","value":"0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D"},{"name":"value","index":2,"value_type":"uint256","value":"3151936770479715624"}]',
                      '[{"name":"from","index":0,"value_type":"address","value":"0x9C0fdF1e074A8DB4b3bfC78cf4d48A5518B7a0BA"},{"name":"to","index":1,"value_type":"address","value":"0xA9D1e08C7793af67e9d92fe308d5697FB81d3E43"},{"name":"value","index":2,"value_type":"uint256","value":"774926382"}]']
    })

@pytest.fixture
def sample_decoded_trace_df():
    return pl.DataFrame({
        "transaction_index": [37, 220],
        "block_number": [21000000, 21000010],
        "transaction_hash": [bytes.fromhex("001b62412265ea5494cd57df808f44e565d898196fc588132990bb167935b385"),
                           bytes.fromhex("20F83F863231A8E1C97F14D1B2E9379D389C57D3BB82D265CD6E5D806378BA9B")],
        "selector": [bytes.fromhex("a9059cbb"), bytes.fromhex("DD62ED3E")],
        "action_to": [bytes.fromhex("502ed02100ea8b10f8d7fc14e0f86633ec2ddada"), bytes.fromhex("F19308F923582A6F7C465E5CE7A9DC1BEC6665B1")],
        "full_signature": ['function transfer(address recipient, uint256 amount) returns (bool)',
                           'function allowance(address owner, address spender) view returns (uint256)'],
        "name": ["transfer", "execute"],
        "input_values": ['["0x00000000d7F382c0b1631e81AC73D0Abd5f6F376", "8969060750056848641449546896815068662213637182689432334179124713602964652196"]',
                         '["0x000000003f2B113D180Ecb1457E450B9EFCAC3df", "0x000000007a250D5630b4Cf539739df2c5dacB4c6"]'],
        "input_keys": ['["recipient", "amount"]',
                       '["owner", "spender"]'],
        "input_json": ['[{"name":"recipient","index":0,"value_type":"address","value":"0x00000000d7F382c0b1631e81AC73D0Abd5f6F376"},{"name":"amount","index":1,"value_type":"uint256","value":"8969060750056848641449546896815068662213637182689432334179124713602964652196"}]',
                      '[{"name":"owner","index":0,"value_type":"address","value":"0x000000003f2B113D180Ecb1457E450B9EFCAC3df"},{"name":"spender","index":1,"value_type":"address","value":"0x000000007a250D5630b4Cf539739df2c5dacB4c6"}]'],
        "output_values": ['["true"]',
                          '["0"]'],
        "output_keys": ['[""]',
                        '[""]'],
        "output_json": ['[{"name":"","index":0,"value_type":"bool","value":"true"}]',
                       '[{"name":"","index":0,"value_type":"uint256","value":"0"}]'],
    })

def test_unnest_event_with_full_signature(sample_decoded_df):
    result = unnest_event(
        sample_decoded_df,
        full_signature='event Transfer(address indexed from, address indexed to, uint256 value)'
    )
    assert not result.is_empty()
    assert all(col in result.columns for col in ["from", "to", "value"])

def test_unnest_event_with_event_name(sample_decoded_df):
    result = unnest_event(sample_decoded_df, event_name="Transfer")
    assert not result.is_empty()

def test_unnest_event_with_wrong_event_name(sample_decoded_df):
    with pytest.raises(ValueError, match="No event found after filtering"):
        unnest_event(sample_decoded_df, event_name="WrongEvent")

def test_unnest_event_with_address(sample_decoded_df):
    result = unnest_event(
        sample_decoded_df,
        event_address="0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
        topic0="0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
    )
    assert not result.is_empty()

def test_unnest_trace_with_full_signature(sample_decoded_trace_df):
    result = unnest_trace(
        decoded_trace_df=sample_decoded_trace_df,
        full_signature="function transfer(address recipient, uint256 amount) returns (bool)"
    )
    assert not result.is_empty()
    assert all(col in result.columns for col in ["recipient", "amount", "output_0"])

def test_unnest_trace_with_function_name_and_action_to(sample_decoded_trace_df):
    result = unnest_trace(
        sample_decoded_trace_df,
        function_name="transfer",
        action_to="0x502ed02100ea8b10f8d7fc14e0f86633ec2ddada"
    )
    assert not result.is_empty()

def test_unnest_trace_with_wrong_function_name(sample_decoded_trace_df):
    with pytest.raises(ValueError, match="No trace found after filtering with the given parameters"):
        unnest_trace(sample_decoded_trace_df, function_name="WrongFunction")

def test_unnest_trace_with_selector_and_action_to(sample_decoded_trace_df):
    result = unnest_trace(
        sample_decoded_trace_df,
        selector="DD62ED3E",
        action_to="0xF19308F923582A6F7C465E5CE7A9DC1BEC6665B1"
    )
    assert not result.is_empty()
    assert all(col in result.columns for col in ["owner", "spender", "output_0"])
