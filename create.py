from pathlib import Path
import argparse
import os
import xarray as xr
from virtualizarr import open_virtual_dataset
import cftime
import numpy as np
from concurrent.futures import ThreadPoolExecutor
import concurrent

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

    except Exception as e:
        print(f"Error on filename: {str(fname)}, Error: {str(e)}")
        return None


def virtualize_day_band(gday_path, band, nworkers=10):
    gday_files = sorted(gday_path.glob(f"**/*M6C{band:02d}*.nc"))
    # Figure out which variables to *drop* by loading all variables from first
    # file and dropping everything except radiance.
    d0 = open_virtual_dataset(str(gday_files[0]), indexes={})
    dropvars = set(d0.keys()).difference({"Rad", "t"})

    virtual_datasets = []

    with ThreadPoolExecutor() as executor:
        future_to_file = {executor.submit(open_ds, fname, dropvars): fname for fname in gday_files}
        for future in concurrent.futures.as_completed(future_to_file):
            try:
                ds = future.result()
                if ds is not None:
                    virtual_datasets.append(ds)
            except Exception as e:
                print(f"Error processing file {future_to_file[future]}: {e}")
        print(f'finished opening {len(virtual_datasets)} out of {len(gday_files)}')

    #virtual_datasets = [open_ds(fname, dropvars) for fname in gday_files]
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
    parser = argparse.ArgumentParser(
            prog = "create.py",
            description = "Create parquet references from remote (S3) GOES files"
    )
    
    parser.add_argument("-b", "--band", type=int)
    parser.add_argument("-d", "--day", type=int)
    parser.add_argument("-y", "--year", type=int, default=2021)
    parser.add_argument("-n", "--name", type=int, default=17)
    parser.add_argument("-w", "--nworkers", type=int, default=36)
    
    argv = parser.parse_args()
    
    year = argv.year
    day = argv.day
    band = int(argv.band)
    nworkers = argv.nworkers
    
    basepath = Path("/css/geostationary/BackStage/")
    
    if argv.name == 17:
        #name = "GOES-17-ABI-L1B-FULLD"
        path = basepath / "GOES-17-ABI-L1B-FULLD"
    elif argv.name == 16:
        #name = "GOES-16-ABI-L1B-FULLD"
        path = basepath / "GOES-16-ABI-L1B-FULLD"
    else:
        raise Exception


    # Try virtualizing one complete day for one bands
    #basepath = Path("/css/geostationary/BackStage/")
    #g16path = basepath / "GOES-16-ABI-L1B-FULLD"
    #g17path = basepath / "GOES-17-ABI-L1B-FULLD"

    # List all the years
    # g17_years = sorted(g17path.glob("????"))
    # List the days for the last available year
    # g17_days = sorted(g17_years[-1].glob("*"))

    print(f"Parsing files for {year=}, {day=}, {band=}")

    gday = path / str(year)/ f"{day:03d}"
    
    if not gday.exists():
        raise ValueError(f"{gday} not found.")
   
    # First, let's try parsing all the files for 1 day and 1 band.
    gd1 = virtualize_day_band(gday, band)
    outfile = Path("combined") / path.name / f"{year}-{day:03d}-B{band:02d}.parq"
    outfile.parent.mkdir(exist_ok=True, parents=True)
    gd1.virtualize.to_kerchunk(outfile, format="parquet")
