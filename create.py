#!/usr/bin/env python
from pathlib import Path
import argparse

from utils import virtualize_day_band

if __name__ == "__main__":
    # Try virtualizing one complete day for one bands
    basepath = Path("/data/geostationary/")
    g17path = basepath / "GOES-17-ABI-L1b-Radf"

    # List all the years
    # g17_years = sorted(g17path.glob("????"))
    # List the days for the last available year
    # g17_days = sorted(g17_years[-1].glob("*"))

    parser = argparse.ArgumentParser(
            prog = "create.py",
            description = "Create parquet references from local GOES files"
    )
    parser.add_argument("-b", "--band", type=int)
    parser.add_argument("-d", "--day", type=int)
    parser.add_argument("-y", "--year", type=int, default=2021)

    argv = parser.parse_args()
    # argv = parser.parse_args(["-b", "6", "-d", "200"])

    year = argv.year # 2021
    day = argv.day   # 200
    band = argv.band # 1

    print(f"Parsing files for {year=}, {day=}, {band=}")

    gday = g17path / str(year) / f"{day:03d}"
    if not gday.exists():
        raise ValueError(f"{gday} not found.")

    # First, let's try parsing all the files for 1 day and 1 band.
    gd1 = virtualize_day_band(gday, band)
    # %time gd1 = virtualize_day_band(gday, band)
    outfile = Path("combined") / g17path.name / f"{year}-{day:03d}-B{band:02d}.parq"
    outfile.parent.mkdir(exist_ok=True, parents=True)
    gd1.virtualize.to_kerchunk(outfile, format="parquet")
