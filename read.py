import xarray as xr

combined_ds = xr.open_dataset("combined.parq", engine="kerchunk")
# combined_ds = xr.open_dataset("combined.json", engine="kerchunk")

print(combined_ds.Rad.mean().values)
