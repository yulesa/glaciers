import polars as pl

async def async_decode_log_df_with_abi_df(
    log_df: pl.DataFrame,
    abi_df: pl.DataFrame,
) -> pl.DataFrame:
    from . import _glaciers_python
    result: pl.DataFrame = await _glaciers_python.decode_log_df_with_abi_df(log_df, abi_df)
    return result

def decode_log_df_with_abi_df(
    log_df: pl.DataFrame,
    abi_df: pl.DataFrame,
) -> pl.DataFrame:

    import asyncio
    coroutine = async_decode_log_df_with_abi_df(log_df, abi_df)

    try:
        import concurrent.futures

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(loop.run_until_complete, coroutine)  # type: ignore
            result: T = future.result()  # type: ignore
    except RuntimeError:
        result = asyncio.run(coroutine)

    return result