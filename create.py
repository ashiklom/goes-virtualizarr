from pathlib import Path

import xarray as xr
from virtualizarr import open_virtual_dataset
import cftime
import numpy as np

# # All of these throw errors on open_virtual_dataset
# dropvars = [
#     "algorithm_product_version_container",
#     "processing_parm_version_container",
#     "goes_imager_projection",
#     "algorithm_dynamic_input_data_container",
#     "geospatial_lat_lon_extent",
# ]

def get_dt(ds):
    val = ds.t.values
    # Round to the nearest minute
    val_rnd = (val // (60)) * (60)
    return cftime.num2date(val_rnd, ds.t.units)

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

def virtualize_day_band(gday_path, band):
    gday_files = sorted(gday_path.glob(f"**/*M6C{band:02d}*.nc"))

    # Figure out which variables to *drop* by loading all variables and dropping
    # everything except radiance.
    d0 = xr.open_dataset(gday_files[1])
    dropvars = set(d0.keys()).difference({"Rad"})

    virtual_datasets = [open_ds(fname, dropvars) for fname in gday_files]
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

# Try virtualizing one complete day for one bands
basepath = Path("/css/geostationary/BackStage/")
g16path = basepath / "GOES-16-ABI-L1B-FULLD"
g17path = basepath / "GOES-17-ABI-L1B-FULLD"

# List all the years
g17_years = sorted(g17path.glob("????"))
# List the days for the last available year
g17_days = sorted(g17_years[-1].glob("*"))

# First, let's try parsing all the files for 1 day and 1 band.
gday = g17_days[0]
gday_files = sorted(gday.glob("**/*M6C01*.nc"))
gday_files = gday_files[0:5]

gd1 = virtualize_day_band(g17_days[0], 1)
dt_np = np.array(gd1.time.values, dtype="datetime64[ns]")
gd1["time"] = xr.DataArray(dt_np, name="time", dims="time")
gd1.virtualize.to_kerchunk("2022-001-combined.json", format="json")
