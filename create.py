import glob
import re
import datetime

import xarray as xr
import numpy as np
from virtualizarr import open_virtual_dataset

# # All of these throw errors on open_virtual_dataset
# dropvars = [
#     "algorithm_product_version_container",
#     "processing_parm_version_container",
#     "goes_imager_projection",
#     "algorithm_dynamic_input_data_container",
#     "geospatial_lat_lon_extent",
# ]

# As a test, only grab radiance --- drop everything else
ds = xr.open_dataset("data/OR_ABI-L1b-RadF-M6C01_G17_s20230010000317_e20230010009384_c20230010009432.nc")
dropvars = set(ds.keys()).difference({"Rad"})

files = glob.glob("data/*.nc")
virtual_datasets = [
    open_virtual_dataset(fname, drop_variables=dropvars, indexes={})
    for fname in files
]

# Note: A lot of variables here are *not* dropped. Is this a bug?
len(virtual_datasets[0].variables)

# Extract times for concatenation (time is in the filename, but is not a dimension in the data)
def parse_date(path):
    match = re.search(r'_s(\d{11})', path)
    if not match:
        raise ValueError(f"Cannot get date from path {path}")
    dstring = match.groups()[0]
    dt = datetime.datetime.strptime(dstring, "%Y%j%H%M")
    return np.datetime64(dt)

dates = xr.DataArray([parse_date(f) for f in files])
virtual_ds = xr.combine_nested(
    virtual_datasets,
    concat_dim={"time": dates},
    coords = "minimal",
    compat = "override"
)

virtual_ds.virtualize.to_kerchunk("combined.parq", format="parquet")
