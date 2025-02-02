import polars as pl
import toml
from glaciers import get_config
from ._dataframe_utils import DataFrameType, to_polars, to_prefered_type

def unnest_event(
        decoded_log_df: DataFrameType,
        event_name: str = None,
        full_signature: str = None,
        event_address: str = None,
        topic0: str = None,
    ) -> DataFrameType:

    decoded_log_df = to_polars(decoded_log_df)
    
    filtered_df = decoded_log_df
    if event_name is not None:
        filtered_df = filtered_df.filter(pl.col("name").str.to_lowercase() == event_name.lower())
    if full_signature is not None:
        filtered_df = filtered_df.filter(pl.col("full_signature").str.to_lowercase() == full_signature.lower())
    if event_address is not None:
        address_col = toml.loads(get_config())["decoder"]["schema"]["alias"]["address"]
        col_types = filtered_df.select([pl.col(address_col)]).dtypes
        if col_types[0] == pl.String:
            filtered_df = filtered_df.filter(pl.col(address_col).str.to_lowercase().str.replace("0x", "") == event_address.lower().replace("0x", ""))
        elif col_types[0] == pl.Binary:
            filtered_df = filtered_df.filter(pl.col(address_col).bin.encode("hex").str.to_lowercase().str.replace("0x", "") == event_address.lower().replace("0x", ""))
        else:
            raise ValueError(f"Invalid column type for address: {col_types[0]}")
    if topic0 is not None:
        topic0_col = toml.loads(get_config())["decoder"]["schema"]["alias"]["topic0"]
        col_types = filtered_df.select([pl.col(topic0_col)]).dtypes
        if col_types[0] == pl.String:
            filtered_df = filtered_df.filter(pl.col(topic0_col).str.to_lowercase().str.replace("0x", "") == topic0.lower().replace("0x", ""))
        elif col_types[0] == pl.Binary:
            filtered_df = filtered_df.filter(pl.col(topic0_col).bin.encode("hex").str.to_lowercase().str.replace("0x", "") == topic0.lower().replace("0x", ""))
        else:
            raise ValueError(f"Invalid column type for topic0: {col_types[0]}")
    
    unique_event = filtered_df.select(pl.col("full_signature")).unique()
    if unique_event.height > 1:
        signatures = unique_event["full_signature"].to_list()
        raise ValueError(f"Event signature is not unique. Signatures: {signatures}")
    if filtered_df.height == 0:
        raise ValueError("No event found after filtering with the given parameters")
    
    else:
        first_row = filtered_df.select(pl.col("event_json").str.json_decode()).row(1)[0]
        num_fields = len(first_row)
        value_types = []
        field_names = []
        for i in range(num_fields):
            value_types.append(first_row[i]["value_type"])
            field_names.append(first_row[i]["name"])

        unnesting_hex_string_encoding = toml.loads(get_config())["glaciers"]["unnesting_hex_string_encoding"]

        for (i, type) in enumerate(value_types):
            if type == "bool":
                filtered_df = filtered_df.with_columns(pl.col("event_values").str.json_decode().list.get(i).cast(pl.Boolean).alias(f"{field_names[i]}"))
            elif "int" in type:
                filtered_df = filtered_df.with_columns(pl.col("event_values").str.json_decode().list.get(i).cast(pl.Float64).alias(f"{field_names[i]}"))
            elif "bytes" in type:
                if unnesting_hex_string_encoding:
                    filtered_df = filtered_df.with_columns(pl.col("event_values").str.json_decode().list.get(i).cast(pl.String).alias(f"{field_names[i]}"))
                else:
                    filtered_df = filtered_df.with_columns(pl.col("event_values").str.json_decode().list.get(i).cast(pl.Binary).alias(f"{field_names[i]}"))
            elif type == "address":
                if unnesting_hex_string_encoding:
                    filtered_df = filtered_df.with_columns(pl.col("event_values").str.json_decode().list.get(i).cast(pl.String).alias(f"{field_names[i]}"))
                else:
                    filtered_df = filtered_df.with_columns(pl.col("event_values").str.json_decode().list.get(i).cast(pl.Binary).alias(f"{field_names[i]}"))
            elif type == "string":
                filtered_df = filtered_df.with_columns(pl.col("event_values").str.json_decode().list.get(i).cast(pl.String).alias(f"{field_names[i]}"))

        return to_prefered_type(filtered_df)




    