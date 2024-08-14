import json
from pathlib import Path

from kerchunk import hdf, df
import fsspec.implementations.reference
from fsspec.implementations.reference import LazyReferenceMapper

print(fsspec.__version__)

# Replace with file name
fname = "/css/geostationary/BackStage/GOES-17-ABI-L1B-FULLD/2022/001/00/OR_ABI-L1b-RadF-M6C01_G17_s20220010000320_e20220010009386_c20220010009424.nc"

h5chunks = hdf.SingleHdf5ToZarr(fname)
refs = h5chunks.translate()

# Write to JSON
with open("test.json", "w") as f:
    f.write(json.dumps(refs, indent=2))

# Write to parquet
fs = fsspec.filesystem("file")
out = LazyReferenceMapper.create(record_size=10_000, root="test.parq", fs=fs)
out.flush()
df.refs_to_dataframe(refs, "test.parq")

# Clean up parquet zmetadata
with open("test.parq/.zmetadata", "r") as f:
    zm = json.load(f)

# Create a copy of the original metadata
with open("test.parq/.zmetadata.orig", "w") as f:
    f.write(json.dumps(zm, indent=2))

# Remove keys that don't have any array information
varnames = {k.split("/")[0] for k in zm["metadata"].keys() if "/" in k}
haskeys = {x.name for x in Path("test.parq").glob("*/")}
keepvars = varnames.difference(haskeys)
zm2 = zm.copy()
zm2_meta = zm2["metadata"]
for key in list(zm2_meta.keys()):
    for v in keepvars:
        if v in key:
            zm2_meta.pop(key)

with open("test.parq/.zmetadata", "w") as f:
    f.write(json.dumps(zm2))

# Test the read
import xarray as xr

tjson = xr.open_dataset("test.json", engine="kerchunk")
tjson.Rad.mean().values

tparq = xr.open_dataset("test.parq", engine="kerchunk")
tparq.Rad.mean().values
