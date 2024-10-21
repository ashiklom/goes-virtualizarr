#!/usr/bin/env python
from multiprocessing import Pool
from functools import partial

import cftime
from virtualizarr import open_virtual_dataset
import xarray as xr
import numpy as np

def get_dt(ds):
    val = ds.t.values

    # Round to the nearest minute
    if val.dtype == "<M8[ns]":
        val_rnd = np.array(val, dtype="datetime64[m]")
        return np.array(val_rnd, dtype="datetime64[ns]")

    val_rnd = (val // (60)) * (60)
    cfdt = cftime.num2date(val_rnd, ds.t.units)
    return np.array(cfdt, dtype="datetime64[ns]")

def open_ds(fname, dropvars):
    try:
        return open_virtual_dataset(
            str(fname),
            drop_variables=dropvars,
            loadable_variables=["t"],
            indexes={}
        )
    except OSError:
        print(f"Error on filename: {fname}")
        return None

def virtualize_day_band(gday_path, band, **kwargs):
    gday_files = sorted(gday_path.glob(f"**/*M6C{band:02d}*.nc"))
    return virtualize_day_band_list(gday_files, **kwargs)

def virtualize_day_band_list(gday_files, nworkers=10):

    # Figure out which variables to *drop* by loading all variables from first
    # file and dropping everything except radiance.
    d0 = open_virtual_dataset(str(gday_files[0]), indexes={})
    dropvars = set(d0.keys()).difference({"Rad", "t"})

    # # All of these throw errors on open_virtual_dataset
    # dropvars = [
    #     "algorithm_product_version_container",
    #     "processing_parm_version_container",
    #     "goes_imager_projection",
    #     "algorithm_dynamic_input_data_container",
    #     "geospatial_lat_lon_extent",
    # ]

    open_ds_local = partial(open_ds, dropvars=dropvars)
    with Pool(nworkers) as pool:
        virtual_datasets = pool.map(open_ds_local, gday_files)
    # virtual_datasets = [open_ds(fname, dropvars) for fname in gday_files]
    virtual_datasets = [vd for vd in virtual_datasets if vd is not None]

    dtimes = xr.DataArray(
        [get_dt(ds) for ds in virtual_datasets],
        dims="time"
    )
    virtual_ds = xr.combine_nested(
        virtual_datasets,
        concat_dim="time",
        coords="minimal",
        compat="override"
    )
    virtual_ds = virtual_ds.assign_coords({"time": dtimes})
    return virtual_ds

