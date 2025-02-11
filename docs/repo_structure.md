# Glaciers Repository Structure

This document is subject to change as Glaciers is actively being improved. If you find any inconsistencies, please raise an issue in the repository. Your contribution is highly valuable.

The `/crates` directory in the `yulesa/glaciers` repository contains all the Rust code for the project, It's organized into three main subdirectories: `/cli`, `/glaciers` and `/python`, one for each crate. Each subdirectory houses all the Rust source code and resources for the different crates of the Glaciers project.

Two folders with sample data are also present in the root of the repository: `/ABIs` and `/data`. The first one contains two small ABI DB files, one for events and one for functions, and an abi_database folder with some ABI files for testing. The second one contains some raw logs and traces files used as examples/testing files. In the root, there is also `glaciers_config_edit_example.toml`, an example of a configuration file for Glaciers, that comes pre-loaded with all the default configurations for Glaciers. You can use it to create your own configuration file.

The root also contains the `/glacier_analytics_example` folder, which contains a Jupyter notebook with one example of how to use Glaciers to decode and analyze some data.

The `/docs` directory contains some aditional documentation for the project, such as this file.



## `crates/cli`

This directory contains the source code for the Glaciers command-line interface (CLI) tool.

- **`src/main.rs`**: The entry point for the CLI tool. It uses the `clap` crate to parse command-line arguments and the `tokio` crate to run asynchronous tasks.



## `crates/glaciers`

This crate contains the core functionality of the Glaciers project. It includes modules for handling ABIs, decoding Ethereum logs and traces, as well as utilities for working with the decoded data.

- **`src/lib.rs`**: This module is the entry point for the Glaciers crate. It list all the modules of the crate.

- **`src/abi_reader.rs`**: This module contains the logic for reading and processing ABI files. It provides functionality for maintaining an ABI database, reading new ABIs from a folder or file, parsing the ABI JSON, extracting function and event signatures from the ABI, and converting ABI data into a structured DataFrame format.

- **`src/decoder.rs`**: This module contains the high level processing and decoding of blockchain data. It provides functionality for decoding a folder of logs/traces, a single log/trace file, a DataFrame of logs/traces using an ABI database file path or a pre-loaded ABI DataFrame.

- **`src/log_decoder.rs`**: This module contains the specific decoding logic for Ethereum logs. It provides functionality for decoding logs into a structured format, including extracting event values and parameter names.

- **`src/trace_decoder.rs`**: This module contains the specific decoding logic for Ethereum traces. It provides functionality for decoding traces into a structured format, including extracting input and output parameters.

- **`src/matcher.rs`**: This module contains the logic for matching decoded logs and traces to the appropriate ABI items in the database.

- **`src/configger.rs`**: This module contains the logic for managing the configuration of the Glaciers project. It has 3 main components: it defines the structs for all the configuration fields, it provides the static GLACIERS_CONFIG, which is the default configuration for Glaciers, and it provides the functions to get and set the configuration fields.

- **`src/utils.rs`**: This module provides utility functions that are not part of the main functionality of the Glaciers. It provides functions to convert binary columns to hex string columns and vice versa, and to read and write DataFrames to files.

- **`src/miscellaneous.rs`**: This module contains miscellaneous functions that are not part of the main functionality of the Glaciers. It stores a function to decode a DataFrame with only a single contract address, by downloading the ABI from Sourcify.



## `crates/python`

This crate contains the Python bindings for the Glaciers project and a Python module that is used to call the Rust functions from pyo3.

- **`rust_src/lib.rs`**: This is a Rust library that provides Python bindings using PyO3 (a framework for creating Python extensions in Rust). The file defines a Python module named _glaciers_python that exposes various Python functions for the Glaciers project.

- **`python/glaciers`**: This folder contains the Python module that is used to call the Rust functions from pyo3.
    - **`__init__.py`**: This file is required to make the folder a Python package.
    - **`_abi_reader.py`**: This file contains the Python bindings for the `abi_reader` module.
    - **`_decode_df_using_single_contract.py`, `_decode_df_with_abi_df.py`, `_decode_df.py`, `_decode_file.py`, `_decode_folder.py`**: These files contain the Python bindings for the `decoder` module.
    - **`_dataframe_utils.py`**: This file has utility functions for handling both Pandas and Polars DataFrames.
    - **`_unnest.py`**: This file contains a single function to unnest a DataFrame with a single event.

- **`tests`**: This folder contains the tests for the Python module.
- **`e2e_example.py`**: This file contains an example of how to use the Glaciers Python module, including all its functionalities.



## Documentation

The `/docs` directory contains comprehensive documentation:

- **`repo_structure.md`**: This file, which contains the structure of the repository.
- **`installation.md`**: Unconventional installation guide.
- **`decoding_key_concepts.md`**: Key concepts for decoding, explaing how Glaciers decodes data.
- **`roadmap.md`**: The public roadmap for the project.