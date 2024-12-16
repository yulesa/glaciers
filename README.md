# Glacier

Fast Ethereum event log decoder in Rust with Python bindings.

## Features
- Decode Ethereum event logs using ABI definitions
- Process logs in parallel using Polars DataFrames
- Python bindings for easy integration

## Installation

### Using uv (Recommended)
```bash
git clone https://github.com/yulesa/glaciers

# Install dependencies
uv sync

# Build and install the package
uv run maturin develop --uv
```

### Running Tests
```bash
# Run end-to-end test
uv run python test_e2e.py
```

## Usage Example
```python
import polars as pl
from glacier import decode_logs

# Create logs DataFrame with raw event logs
logs_df = pl.DataFrame({
    "topic0": [bytes.fromhex("ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")],
    "topic1": [bytes.fromhex("000000000000000000000000a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48")],
    "topic2": [bytes.fromhex("0000000000000000000000007a250d5630b4cf539739df2c5dacb4c659f2488d")],
    "topic3": [bytes.fromhex("0000000000000000000000000000000000000000000000000000000000000000")],
    "data": [bytes.fromhex("0000000000000000000000000000000000000000000000000000000000000064")],
})

# Create ABI DataFrame with event signature
abi_df = pl.DataFrame({
    "topic0": [bytes.fromhex("ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")],
    "full_signature": ["Transfer(address indexed from, address indexed to, uint256 value)"],
})

# Decode the events
decoded_df = decode_logs(logs_df, abi_df)
print(decoded_df)
```

