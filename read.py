import xarray as xr

combined_ds = xr.open_dataset('combined.parq', engine='kerchunk')

combined_ds.isel(x = 5000, y = 5000)['Rad'].values
