## Rust Installation
Currently, Glaciers requires manual compilation and execution. Methods requires having rust installed. See [rustup](https://rustup.rs/) for instructions. To get started:

1. Clone the repository:
```bash
git clone https://github.com/yulesa/glaciers
# Navigate to the project folder
cd glaciers
```
2. Compile and run the project:

```bash
cargo run
```



## Python Installation

### Using uv, from source (Recommended)

This method requires having uv (Python package and project manager) installed. See [uv](https://docs.astral.sh/uv/getting-started/installation/) for instructions.

1. Clone the repository:
```bash
git clone https://github.com/yulesa/glaciers
# Navigate to the project python module folder
cd glaciers/crates/python
```
2. Install dependencies in .venv
```bash
uv sync
```
3.  Build the glaciers python module and install the package

```bash
uv run maturin develop --uv

```
4.  Run the end-to-end example

```bash
uv run python e2e_example.py
```

### Using pip, from source

1. Install [maturin](https://www.maturin.rs/), a tool to embed Rust libraries into a Python module. 
```bash
pip install maturin
```
2. Clone the repository:
```bash
git clone https://github.com/yulesa/glaciers
# Navigate to the project python module folder
cd glaciers/crates/python
```
3.  Build the Rust python module and install the package

```bash
maturin develop
```
4.  Run the end-to-end example

```bash
python e2e_example.py
```