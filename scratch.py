import glob
from h5py import h5
from virtualizarr import ManifestArray, open_virtual_dataset, ChunkManifest

import xarray as xr

# Can I read it?
# import h5py
# hf = h5py.File("data/OR_ABI-L1b-RadF-M6C01_G17_s20230010000317_e20230010009384_c20230010009432.nc", "r")
# hd = hf["Rad"]
# hd.chunks

import kerchunk.hdf

fname = "data/OR_ABI-L1b-RadF-M6C01_G17_s20230010000317_e20230010009384_c20230010009432.nc"
h5chunks = kerchunk.hdf.SingleHdf5ToZarr(fname)
h5_json = h5chunks.translate()

import virtualizarr.kerchunk
from virtualizarr.kerchunk import read_kerchunk_references_from_file
from virtualizarr.xarray import virtual_vars_from_kerchunk_refs, variable_from_kerchunk_refs


rf = read_kerchunk_references_from_file(fname, None)
var_names = virtualizarr.kerchunk.find_var_names(rf)
for v in var_names:
    try:
        variable_from_kerchunk_refs(rf, v, ManifestArray)
    except Exception as _:
        print(f"Failed on variable {v}")
vv = virtual_vars_from_kerchunk_refs(rf)

dropvars = [
    "algorithm_product_version_container",
    "processing_parm_version_container",
    "goes_imager_projection",
    "algorithm_dynamic_input_data_container",
    "geospatial_lat_lon_extent",
]
vd = open_virtual_dataset(fname, drop_variables=dropvars, indexes={})

# dat = xr.open_dataset("data/OR_ABI-L1b-RadF-M6C01_G17_s20230010010317_e20230010019384_c20230010019434.nc")
# dat.chunks
# dat.chunksizes

virtual_datasets = [
    open_virtual_dataset(fname)
    for fname in glob.glob("data/*.nc")
]

