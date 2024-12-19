import polars as pl

async def async_decode_log_df(
    logs_df: pl.DataFrame,
    topic0_path: str,
) -> pl.DataFrame:
    from . import _glaciers_rust
    result: pl.DataFrame = await _glaciers_rust.decode_log_df(logs_df, topic0_path)
    return result

def decode_log_df(
    logs_df: pl.DataFrame,
    topic0_path: str,
) -> pl.DataFrame:

    import asyncio
    coroutine = async_decode_log_df(logs_df, topic0_path)

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