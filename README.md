# Virtualization of GOES data using VirtualiZarr

## Usage

- `create.py` --- Creates a Kerchunk index for a particular GOES-17 day-band combination. Change the target year, day, and band at the bottom of the file.
- `read.py` --- Test and benchmark the reading of a single Kerchunk index file.

Other scripts are scratch/testing code.

## Setup

This uses the [`pixi` package manager](https://pixi.sh/latest/).
To get a shell with all dependencies automatically installed and loaded, run `pixi shell`.

Alternatively, you can run individual scripts with `pixi run python <script.py>`.

If you want to install dependencies manually, refer to the `pyproject.toml` file.
