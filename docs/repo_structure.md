# Glaciers Repository Structure

This document is subject to change as Glaciers is actively being improved. If you find any inconsistencies, please raise an issue in the repository. Your contributions are highly valuable.

## Overview

The `/crates` directory in the `yulesa/glaciers` repository contains all the Rust code for the project, organized into three main subdirectories: `/cli`, `/glaciers`, and `/python`, each corresponding to a separate crate. Each subdirectory houses all the Rust source code and resources for its respective crate within the Glaciers project.

Additionally, the repository contains the following key folders and files:
- **`/ABIs`**: Contains two small ABI database files (one for events and one for functions) and an `abi_database` folder with ABI files for testing.
- **`/data`**: Holds raw logs and trace files used for examples and testing.
- **`glaciers_config_edit_example.toml`**: An example configuration file pre-loaded with default settings for Glaciers, which can be used as a template.
- **`/glacier_analytics_example`**: Contains a Jupyter notebook demonstrating how to use Glaciers for decoding and analyzing data.
- **`/docs`**: Stores additional project documentation, including this file.

---

## `crates/cli`

This directory contains the source code for the Glaciers command-line interface (CLI) tool.

- **`src/main.rs`**: The entry point for the CLI tool, utilizing the `clap` crate for command-line argument parsing and the `tokio` crate for asynchronous task execution.

---

## `crates/glaciers`

This crate contains the core functionality of the Glaciers project, including modules for handling ABIs, decoding Ethereum logs and traces, and utilities for working with decoded data.

- **`src/lib.rs`**: The main entry point, listing all the modules of the crate.
- **`src/abi_reader.rs`**: Handles reading and processing ABI files, maintaining an ABI database, and extracting function and event signatures.
- **`src/decoder.rs`**: Provides high-level processing for decoding blockchain data, supporting both individual files and entire folders of logs/traces.
- **`src/log_decoder.rs`**: This module contains the specific decoding logic for decoding Ethereum logs, extracting event values and parameter names.
- **`src/trace_decoder.rs`**: This module contains the specific decoding logic for decoding Ethereum traces, extracting input and output parameters.
- **`src/matcher.rs`**: Matches decoded logs and traces to the appropriate ABI items in the database.
- **`src/configger.rs`**: Manages configuration settings, defining structures, default configurations, and functions to modify settings.
- **`src/utils.rs`**: Provides utility functions  that are not part of the main functionality of the Glaciers, such as converting binary columns to hex strings and reading/writing DataFrames.
- **`src/miscellaneous.rs`**: Includes additional functions, not part of the main functionality of the Glaciers. It stores a single function to decode a DataFrame with only one contract address, by downloading the ABI from Sourcify.

---

## `crates/python`

This crate contains the Python bindings for Glaciers and a Python module that interfaces with Rust functions via PyO3.

- **`rust_src/lib.rs`**: Defines the Python bindings using PyO3, exposing Rust functions to Python.
- **`python/glaciers`**: The Python module that interacts with Rust.
  - **`__init__.py`**: Marks the directory as a Python package, and list the exposed functions to the Python module.
  - **`_abi_reader.py`**: Python bindings for the `abi_reader` module.
  - **`_decode_df_using_single_contract.py`, `_decode_df_with_abi_df.py`, `_decode_df.py`, `_decode_file.py`, `_decode_folder.py`**: Bindings for the `decoder` module.
  - **`_dataframe_utils.py`**: Utility functions for handling Pandas and Polars DataFrames.
  - **`_unnest.py`**: Contains a function for flattening nested columns after filtering to a single event.
- **`tests`**: Includes the tests for the Python module.
- **`e2e_example.py`**: Provides an end-to-end example of using the Glaciers Python module.

---

## Documentation

The `/docs` directory contains comprehensive project documentation:

- **`repo_structure.md`**: This file, describing the repository structure.
- **`installation.md`**: Unconventional installations guide.
- **`decoding_key_concepts.md`**: Explains key concepts behind Glaciers' decoding process.
- **`roadmap.md`**: Outlines the project's development roadmap.