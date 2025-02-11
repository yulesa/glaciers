import polars as pl
from ._dataframe_utils import DataFrameType, to_polars, to_prefered_type
from . import _glaciers_python

async def async_decode_df_using_single_contract(
    decoder_type: str,
    df: DataFrameType,
    contract_address: str,
) -> DataFrameType:
    """
    Asynchronously decode blockchain data for a single contract address, by downloading the ABI from Sourcify.
    This is a shortcuting function that automatically downloads the ABI from Sourcify, reads it and decodes the DataFrame.
    Nevertheless, we recommend following the normal flow and creating the ABI DB first.

    Args:
        decoder_type (str): Type of decoder to use. Must be either "log" or "trace".
        df (DataFrameType): DataFrame (polars or pandas) containing the raw blockchain data.
        contract_address (str): Ethereum contract address to use for decoding.

    Returns:
        DataFrameType: Decoded DataFrame (polars or pandas according to the config) with the results.

    Example:
        ```python
        decoded_df = await async_decode_df_using_single_contract(
            "log",
            raw_logs_df,
            "0x1234..."
        )
        ```
    """
    valid_decoder_types = ["log", "trace"]
    if decoder_type not in valid_decoder_types:
        raise ValueError(f"Decoder type must be one of {valid_decoder_types}")
    
    df_pl = to_polars(df)
    result_pl: pl.DataFrame = await _glaciers_python.decode_df_using_single_contract(decoder_type, df_pl, contract_address)
    return to_prefered_type(result_pl)

def decode_df_using_single_contract(
    decoder_type: str,
    df: DataFrameType,
    contract_address: str,
) -> DataFrameType:
    """
    Decode blockchain data for a single contract address, by downloading the ABI from Sourcify.
    This is a shortcuting function that automatically downloads the ABI from Sourcify, reads it and decodes the DataFrame.
    Nevertheless, we recommend following the normal flow and creating the ABI DB first.

    This is a synchronous wrapper around async_decode_df_using_single_contract.

    Args:
        decoder_type (str): Type of decoder to use. Must be either "log" or "trace".
        df (DataFrameType): DataFrame (polars or pandas) containing the raw blockchain data.
        contract_address (str): Ethereum contract address to use for decoding.

    Returns:
        DataFrameType: Decoded DataFrame (polars or pandas according to the config) with the results.

    Example:
        ```python
        decoded_df = decode_df_using_single_contract(
            "log",
            raw_logs_df,
            "0x1234..."
        )
        ```
    """
    import asyncio
    coroutine = async_decode_df_using_single_contract(decoder_type, df, contract_address)
    
    try:
        import concurrent.futures
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(loop.run_until_complete, coroutine)
            result = future.result()
    except RuntimeError:
        result = asyncio.run(coroutine)

    return result