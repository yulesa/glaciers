# Roadmap

Our goal is to develop Glaciers into a robust and efficient solution for decoding multiple sources of EVM data. This roadmap outlines features that have been requested or envisioned, presented in no particular order. It is a living document, open to additions, details, or modifications by anyone.

## Features

- [ ] Create a CLI for Glaciers.
- [ ] Create a config file and allow users to modify Glaciers' execution parameters.
- [ ] Add support for additional input/output source types beyond Parquet, such as CSV, JSON, and Pandas DataFrames.
- [ ] Add an extra algo for contract match before hash match, and param for only matched contracts.
- [ ] Add single-contract log decoding functions.
- [ ] Add single-contract function call (traces) decoding functions.
- [ ] Package Glaciers as a Polars plugin, enabling Python commands like `df.glaciers.decode_events()`.
- [ ] Add generalized function call decoding to support algorithmic trace decoding.
- [ ] Create an unnest function.
- [ ] Document the repository structure to facilitate contributions.
- [ ] Develop example files to guide new users in using Glaciers.

## What did you get done this week:

### 2024-12-27
 - Refactor the ABI Reader, providing functions to aggregate multiple ABI files in a folder, a single ABI, or even a manually inputted ABI, into a file or DF.
 - Debuged matched events, but decode fails.
 - Created the decoding key concept documentation.

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

