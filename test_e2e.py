import cryo
import polars as pl
from glaciers import decode_logs

# APE token Transfer event
TRANSFER_EVENT = "Transfer(address indexed from, address indexed to, uint256 value)"
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
APE_CONTRACT = "0x4d224452801ACEd8B2F0aebE155379bb5D594381"

# Collect raw Transfer events
logs_df = cryo.collect(
    "logs",
    blocks=["18735627:18735727"],
    topic0=[TRANSFER_TOPIC],
    address=[APE_CONTRACT],
    output_format="polars",
    max_concurrent_chunks=15,
    chunk_size=1000,
    inner_request_size=200,
    rpc="https://eth.merkle.io",
)

print(f"\nType of logs_df: {type(logs_df)}")
print(f"\nColumns in logs_df: {logs_df.columns}")

# Create ABI DataFrame
abi_df = pl.DataFrame(
    {
        "topic0": [
            logs_df["topic0"][0]
        ],  # Use first topic0 as they should all be the same
        "full_signature": [TRANSFER_EVENT],
    }
)

# Decode the events
decoded_df = decode_logs(logs_df, abi_df)
print(f"\nDecoded {len(decoded_df)} Transfer events. Sample event:")
print(decoded_df.select(["event_values", "event_keys", "event_json"]).head(1))
