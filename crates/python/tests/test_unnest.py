import pytest
import polars as pl
import pandas as pd
from glaciers import unnest_event

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

def test_unnest_event_with_full_signature(sample_decoded_df):
    result = unnest_event(
        sample_decoded_df,
        full_signature='event Transfer(address indexed from, address indexed to, uint256 value)'
    )
    assert not result.is_empty()
    assert len(result) == len(sample_decoded_df)
    assert all(col in result.columns for col in ["from", "to", "value"])

def test_unnest_event_with_event_name(sample_decoded_df):
    result = unnest_event(sample_decoded_df, event_name="Transfer")
    assert not result.is_empty()
    assert len(result) == len(sample_decoded_df)

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