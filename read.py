import xarray as xr
import time

def ttime(f):
    ts = time.time()
    result = f()
    te = time.time()
    print(f"Time: {te - ts:.02f} sec")
    return result

# combined_ds = xr.open_dataset("combined.parq", engine="kerchunk")
print("Opening file")
combined_ds = ttime(lambda: xr.open_dataset(
    "combined/GOES-17-ABI-L1B-FULLD/2022-001-B01.json",
    engine="kerchunk"
))

print("Calculating global mean")
result = ttime(lambda: combined_ds.Rad.mean().values)

print(f"Global mean: {result}")
