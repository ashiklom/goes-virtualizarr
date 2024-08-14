from kerchunk import hdf, df
import fsspec.implementations.reference
from fsspec.implementations.reference import LazyReferenceMapper
import json

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

# Test the read

import xarray as xr

tjson = xr.open_dataset("test.json", engine="kerchunk")
tparq = xr.open_dataset("test.parq", engine="kerchunk")

tjson.Rad.mean().values
tparq.Rad.mean().values
