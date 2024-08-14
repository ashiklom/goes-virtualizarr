from pathlib import Path

import xarray as xr
from virtualizarr import open_virtual_dataset
import cftime
import numpy as np

def get_dt(ds):
    val = ds.t.values
    # Round to the nearest minute
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

def virtualize_day_band(gday_path, band):
    gday_files = sorted(gday_path.glob(f"**/*M6C{band:02d}*.nc"))

    # Figure out which variables to *drop* by loading all variables from first
    # file and dropping everything except radiance.
    d0 = xr.open_dataset(gday_files[0])
    dropvars = set(d0.keys()).difference({"Rad"})

    # # All of these throw errors on open_virtual_dataset
    # dropvars = [
    #     "algorithm_product_version_container",
    #     "processing_parm_version_container",
    #     "goes_imager_projection",
    #     "algorithm_dynamic_input_data_container",
    #     "geospatial_lat_lon_extent",
    # ]

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

if __name__ == "__main__":
    # Try virtualizing one complete day for one bands
    basepath = Path("/css/geostationary/BackStage/")
    g16path = basepath / "GOES-16-ABI-L1B-FULLD"
    g17path = basepath / "GOES-17-ABI-L1B-FULLD"

    # List all the years
    # g17_years = sorted(g17path.glob("????"))
    # List the days for the last available year
    # g17_days = sorted(g17_years[-1].glob("*"))

    year = 2022
    day = 1
    band = 1

    print(f"Parsing files for {year=}, {day=}, {band=}")

    gday = g17path / str(year) / f"{day:03d}"
    if not gday.exists():
        raise ValueError(f"{gday} not found.")

    # First, let's try parsing all the files for 1 day and 1 band.
    gd1 = virtualize_day_band(gday, 1)
    outfile = Path("combined") / g17path.name / f"{year}-{day:03d}-B{band:02d}.json"
    outfile.parent.mkdir(exist_ok=True, parents=True)
    gd1.virtualize.to_kerchunk(outfile, format="json")
