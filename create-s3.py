#!/usr/bin/env python
from pathlib import Path
import subprocess
import re

import argparse

from utils import virtualize_day_band_list

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
            prog = "create.py",
            description = "Create parquet references from remote (S3) GOES files"
    )
    parser.add_argument("-b", "--band", type=int)
    parser.add_argument("-d", "--day", type=int)
    parser.add_argument("-y", "--year", type=int, default=2021)

    argv = parser.parse_args()
    # argv = parser.parse_args(["-b", "1", "-d", "199"])

    # Try virtualizing one complete day for one bands
    year = argv.year
    day = argv.day
    band = argv.band

    print(f"Parsing files for {year=}, {day=}, {band=}")

    basepath = f"s3://noaa-goes17/ABI-L1b-RadF/{year}/{day}"
    flist_raw = subprocess.run(
        ["aws", "s3", "ls", "--recursive", basepath],
        capture_output=True,
        text=True
    )
    flist_split = flist_raw.stdout.split("\n")
    flist = [
        m.group() for s in flist_split
        if (m := re.search("ABI-L1b-RadF.*\\.nc$", s))
    ]
    gday_files = sorted(f"s3://noaa-goes17/{f}" for f in flist)

    # First, let's try parsing all the files for 1 day and 1 band.
    gd1 = virtualize_day_band_list(gday_files[0:5])
    # %time gd1 = virtualize_day_band_list(gday_files[0:5])
    outfile = (Path("combined") /
               "GOES-17-ABI-L1b-Radf" /
               f"{year}-{day:03d}-B{band:02d}.parq")
    outfile.parent.mkdir(exist_ok=True, parents=True)
    gd1.virtualize.to_kerchunk(outfile, format="parquet")
