#!/usr/bin/env python

from pathlib import Path
import shutil

import pandas as pd

parq = Path("combined.parq")
target = Path("combined_clean.parq")
target.mkdir(exist_ok=True)

# First, copy .zmetadata
shutil.copy(parq / ".zmetadata", target / ".zmetadata")

# Next, copy cleaned versions of each array
parqvars = parq.glob("*/")

for v in parqvars:
    print(f"{v=}")
    outdir = target / v.name
    outdir.mkdir()
    refdf = pd.read_parquet(v)
    ref_notna = refdf.loc[pd.notna(refdf.path)]
    ref_notna.to_parquet(target / v.name / "refs.0.parq")

# Now, try reading
import xarray as xr

dat = xr.open_dataset("combined_clean.parq", engine="kerchunk")
