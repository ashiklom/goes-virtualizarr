import kerchunk.hdf
import json

fname = "/css/geostationary/BackStage/GOES-17-ABI-L1B-FULLD/2022/001/00/OR_ABI-L1b-RadF-M6C01_G17_s20220010000320_e20220010009386_c20220010009424.nc"

h5chunks = kerchunk.hdf.SingleHdf5ToZarr(fname)
refs = h5chunks.translate()

with open("test.json", "w") as f:
    f.write(json.dumps(refs, indent=2))

import xarray as xr

dat = xr.open_dataset("test.json", engine="kerchunk")

d2 = xr.open_dataset("combined.parq/", engine="kerchunk")
d2.Rad.mean().values
