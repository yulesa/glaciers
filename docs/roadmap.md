# Roadmap

Our goal is to develop Glaciers into a robust and efficient solution for decoding multiple sources of EVM data. This roadmap outlines features that have been requested or envisioned, presented in no particular order. It is a living document, open to additions, details, or modifications by anyone.

## Features

- [x] Create a CLI for Glaciers.
- [x] Create a config file and allow users to modify Glaciers' execution parameters.
- [x] Add support for additional input/output source types beyond Parquet, such as CSV.
- [x] Add support for Pandas DataFrames.
- [x] Add an extra algo for contract match before hash match.
- [x] Add single-contract log decoding functions.
- [ ] Package Glaciers as a Polars plugin, enabling Python commands like `df.glaciers.decode_events()`.
- [x] Add generalized "function call" decoding to support algorithmic trace decoding.
- [x] Create an unnest function.
- [ ] Document the repository structure to facilitate contributions.
- [x] Develop example files to guide new users in using Glaciers.

## What did you get done this week:

### 2025-01-31
- Created a simple CLI for Glaciers.
- Created a larger ABI DB using Sourcify and make it public in a [separate repo](https://github.com/yulesa/sourcify_abis).
- Wrote a blog post about decoding all Ethereum raw logs with Glaciers, using the larger ABI DB.
- Debugged and fixed a bug in the unnest function. Fixed a bug when the logs folder has directories.

### 2025-01-24
- Added support for Pandas DataFrames.
- Added a single-contract log decoding function.
- Added a unnest function.

### 2025-01-17
- Merged PR supporting CSV files.
- Finished the analytics example notebook.
- Added a new matching algorithm: `topic0_address`, and updated the provided ABI db.
- Had discussions with some EVM indexers interested in using Glaciers.

### 2025-01-10
- Added support for CSV files, PR in review.
- Debugged and fixed event_values. Fixed Byte values formating as hex strings.
- Started an example notebook for the project.

### 2025-01-03
- Added a config file and allow users to modify Glaciers' execution parameters.
- Added new and clearer decoding functions.
- Wrote a blog post about the project.

### 2024-12-27
 - Refactor the ABI Reader, providing functions to aggregate multiple ABI files in a folder, a single ABI, or even a manually inputted ABI, into a file or DF.
 - Debuged matched events, but decode fails.
 - Created the decoding key concepts documentation.

### 2024-12-20
- Shared Glaciers in the Data Tools channel.
- Created Python bindings and deployed them to PyPI.
- Published Glaciers on crates.io.
- Started a roadmap.

### 2024-12-13
- Named the project "Glaciers," replacing "batch decoder."
- Completed the MVP.
- Created a `README.md` document.
- Made additional preparations for sharing Glaciers in the Data Tools channel.

