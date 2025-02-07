import pytest
import polars as pl
import pandas as pd
from glaciers import unnest_event

@pytest.fixture
def sample_decoded_df():
    return pl.DataFrame({
        "address": ["0xE672E0E0101A7F58d728751E2a5e6Da5Ff1FDa64"],
        "name": ["Transfer"],
        "full_signature": ["event Transfer(address indexed from, address indexed to, uint256 value)"],
        "topic0": ["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"],
        "event_json": ['[{"name":"from","value_type":"address"},{"name":"to","value_type":"address"},{"name":"value","value_type":"uint256"}]'],
        "event_values": ['["0x1234...","0x5678...", "1000000000000000000"]']
    })

def test_unnest_event_basic(sample_decoded_df):
    result = unnest_event(sample_decoded_df)
    assert isinstance(result, (pl.DataFrame, pd.DataFrame))
    assert not result.is_empty()
    assert all(col in result.columns for col in ["from", "to", "value"])

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
        event_address="0xE672E0E0101A7F58d728751E2a5e6Da5Ff1FDa64"
    )
    assert not result.is_empty()
    assert len(result) == len(sample_decoded_df) 