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
    """
    Unnest an event columns, by filtering a DataFrame with the given parameters and then unnesting the event.

    Args:
        decoded_log_df (DataFrameType): The DataFrame (polars or pandas) to unnest.
        event_name (str, optional): The name of the event to filter the DataFrame.
        full_signature (str, optional): The full signature of the event to filter the DataFrame.
        event_address (str, optional): The address of the event to filter the DataFrame. event_address must be a hex string.
        topic0 (str, optional): The topic0 of the event to filter the DataFrame. topic0 must be a hex string.

    Returns:
        DataFrameType: The unnested DataFrame.

    Example:
        ```python
        unnest_event(
            decoded_log_df,
            event_name="Transfer",
            event_address="0x1234567890123456789012345678901234567890",
        ```
    """

    decoded_log_df = to_polars(decoded_log_df)
    
    filtered_df = decoded_log_df
    if event_name is not None:
        filtered_df = filtered_df.filter(pl.col("name").str.to_lowercase() == event_name.lower())
    if full_signature is not None:
        filtered_df = filtered_df.filter(pl.col("full_signature").str.to_lowercase() == full_signature.lower())
    if event_address is not None:
        address_col = toml.loads(get_config())["log_decoder"]["log_schema"]["log_alias"]["address"]
        col_types = filtered_df.select([pl.col(address_col)]).dtypes
        if col_types[0] == pl.String:
            filtered_df = filtered_df.filter(pl.col(address_col).str.to_lowercase().str.replace("0x", "") == event_address.lower().replace("0x", ""))
        elif col_types[0] == pl.Binary:
            filtered_df = filtered_df.filter(pl.col(address_col).bin.encode("hex").str.to_lowercase().str.replace("0x", "") == event_address.lower().replace("0x", ""))
        else:
            raise ValueError(f"Invalid column type for address: {col_types[0]}")
    if topic0 is not None:
        topic0_col = toml.loads(get_config())["log_decoder"]["log_schema"]["log_alias"]["topic0"]
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
    elif filtered_df.height == 0:
        raise ValueError("No event found after filtering with the given parameters")
    else:
        first_row = filtered_df.select(pl.col("event_json").str.json_decode()).row(0)[0]
        num_fields = len(first_row)
        value_types = []
        field_names = []
        for i in range(num_fields):
            value_types.append(first_row[i]["value_type"])
            if first_row[i]["name"] != "":
                field_names.append(first_row[i]["name"])
            else:
                field_names.append(f"field_{i}")

        unnesting_hex_string_encoding = toml.loads(get_config())["glaciers"]["unnesting_hex_string_encoding"]

        for (i, type) in enumerate(value_types):
            if type == "bool":
                filtered_df = filtered_df.with_columns(pl.col("event_values").str.json_decode().list.get(i).replace_strict({"false":False, "true":True}).cast(pl.Boolean).alias(f"{field_names[i]}"))
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


def unnest_trace(  
        decoded_trace_df: DataFrameType,
        function_name: str = None,
        action_to: str = None,
        selector: str = None,
        full_signature: str = None,

    ) -> DataFrameType:
    """
    Unnest a trace columns, by filtering a DataFrame with the given parameters and then unnesting the trace.

    Args:
        decoded_trace_df (DataFrameType): The DataFrame (polars or pandas) to unnest.
        function_name (str, optional): The name of the function to filter the DataFrame.
        action_to (str, optional): The action_to of the function to filter the DataFrame.
        selector (str, optional): The selector of the function to filter the DataFrame. selector must be a hex string.
        full_signature (str, optional): The full signature of the function to filter the DataFrame.

    Returns:
        DataFrameType: The unnested DataFrame.

    Example:
        ```python
        unnest_traces(
            decoded_trace_df,
            function_name="transfer",
            action_to="0x1234567890123456789012345678901234567890"
        )   
    """

    decoded_trace_df = to_polars(decoded_trace_df)
    
    filtered_df = decoded_trace_df
    if function_name is not None:
        filtered_df = filtered_df.filter(pl.col("name").str.to_lowercase() == function_name.lower())
    if full_signature is not None:
        filtered_df = filtered_df.filter(pl.col("full_signature").str.to_lowercase() == full_signature.lower())
    if action_to is not None:
        action_to_col = toml.loads(get_config())["trace_decoder"]["trace_schema"]["trace_alias"]["action_to"]
        col_types = filtered_df.select([pl.col(action_to_col)]).dtypes
        if col_types[0] == pl.String:
            filtered_df = filtered_df.filter(pl.col(action_to_col).str.to_lowercase().str.replace("0x", "") == action_to.lower().replace("0x", ""))
        elif col_types[0] == pl.Binary:
            filtered_df = filtered_df.filter(pl.col(action_to_col).bin.encode("hex").str.to_lowercase().str.replace("0x", "") == action_to.lower().replace("0x", ""))
        else:
            raise ValueError(f"Invalid column type for action_to: {col_types[0]}")
    if selector is not None:
        selector_col = toml.loads(get_config())["trace_decoder"]["trace_schema"]["trace_alias"]["selector"]
        col_types = filtered_df.select([pl.col(selector_col)]).dtypes
        if col_types[0] == pl.String:
            filtered_df = filtered_df.filter(pl.col(selector_col).str.to_lowercase().str.replace("0x", "") == selector.lower().replace("0x", ""))
        elif col_types[0] == pl.Binary:
            filtered_df = filtered_df.filter(pl.col(selector_col).bin.encode("hex").str.to_lowercase().str.replace("0x", "") == selector.lower().replace("0x", ""))
        else:
            raise ValueError(f"Invalid column type for selector: {col_types[0]}")
    
    unique_trace = filtered_df.select(pl.col("full_signature")).unique()
    if unique_trace.height > 1:
        signatures = unique_trace["full_signature"].to_list()
        raise ValueError(f"Trace signature is not unique. Signatures: {signatures}")
    elif filtered_df.height == 0:
        raise ValueError("No trace found after filtering with the given parameters")
    else:
        unnesting_hex_string_encoding = toml.loads(get_config())["glaciers"]["unnesting_hex_string_encoding"]

        input_first_row = filtered_df.select(pl.col("input_json").str.json_decode()).row(0)[0]
        input_num_fields = len(input_first_row)
        input_value_types = []
        input_field_names = []
        for i in range(input_num_fields):
            input_value_types.append(input_first_row[i]["value_type"])
            if input_first_row[i]["name"] != "":
                input_field_names.append(input_first_row[i]["name"])
            else:
                input_field_names.append(f"input_{i}")
        
        for (i, type) in enumerate(input_value_types):
            if type == "bool":
                filtered_df = filtered_df.with_columns(pl.col("input_values").str.json_decode().list.get(i).replace_strict({"false":False, "true":True}).cast(pl.Boolean).alias(f"{input_field_names[i]}"))
            elif "int" in type:
                filtered_df = filtered_df.with_columns(pl.col("input_values").str.json_decode().list.get(i).cast(pl.Float64).alias(f"{input_field_names[i]}"))
            elif "bytes" in type:
                if unnesting_hex_string_encoding:
                    filtered_df = filtered_df.with_columns(pl.col("input_values").str.json_decode().list.get(i).cast(pl.String).alias(f"{input_field_names[i]}"))
                else:
                    filtered_df = filtered_df.with_columns(pl.col("input_values").str.json_decode().list.get(i).cast(pl.Binary).alias(f"{input_field_names[i]}"))
            elif type == "address": 
                if unnesting_hex_string_encoding:
                    filtered_df = filtered_df.with_columns(pl.col("input_values").str.json_decode().list.get(i).cast(pl.String).alias(f"{input_field_names[i]}"))
                else:
                    filtered_df = filtered_df.with_columns(pl.col("input_values").str.json_decode().list.get(i).cast(pl.Binary).alias(f"{input_field_names[i]}"))
            elif type == "string":
                filtered_df = filtered_df.with_columns(pl.col("input_values").str.json_decode().list.get(i).cast(pl.String).alias(f"{input_field_names[i]}"))

        output_first_row = filtered_df.select(pl.col("output_json").str.json_decode()).row(0)[0]
        output_num_fields = len(output_first_row)
        output_value_types = []
        output_field_names = []
        for i in range(output_num_fields):
            output_value_types.append(output_first_row[i]["value_type"])
            if output_first_row[i]["name"] != "":
                output_field_names.append(output_first_row[i]["name"])
            else:
                output_field_names.append(f"output_{i}")
        
        for (i, type) in enumerate(output_value_types):
            if type == "bool":
                filtered_df = filtered_df.with_columns(pl.col("output_values").str.json_decode().list.get(i).replace_strict({"false":False, "true":True}).cast(pl.Boolean).alias(f"{output_field_names[i]}"))
            elif "int" in type:
                filtered_df = filtered_df.with_columns(pl.col("output_values").str.json_decode().list.get(i).cast(pl.Float64).alias(f"{output_field_names[i]}"))
            elif "bytes" in type:
                if unnesting_hex_string_encoding:
                    filtered_df = filtered_df.with_columns(pl.col("output_values").str.json_decode().list.get(i).cast(pl.String).alias(f"{output_field_names[i]}"))
                else:
                    filtered_df = filtered_df.with_columns(pl.col("output_values").str.json_decode().list.get(i).cast(pl.Binary).alias(f"{output_field_names[i]}"))
            elif type == "address":
                if unnesting_hex_string_encoding:
                    filtered_df = filtered_df.with_columns(pl.col("output_values").str.json_decode().list.get(i).cast(pl.String).alias(f"{output_field_names[i]}"))
                else:
                    filtered_df = filtered_df.with_columns(pl.col("output_values").str.json_decode().list.get(i).cast(pl.Binary).alias(f"{output_field_names[i]}"))
            elif type == "string":
                filtered_df = filtered_df.with_columns(pl.col("output_values").str.json_decode().list.get(i).cast(pl.String).alias(f"{output_field_names[i]}"))
    
        return to_prefered_type(filtered_df)