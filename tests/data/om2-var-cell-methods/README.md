Generated using the following:


```python
import xarray as xr
for fname_in, fname_out in [('/g/data/cj50/access-om2/raw-output/access-om2-01/01deg_jra55v140_iaf_cycle3/output656/ocean/ocean-3d-v-1-monthly-mean-ym_2000_01.nc', '~/ocean-3d-v-1-monthly-mean-ym_2000_01.nc'),
 ('/g/data/cj50/access-om2/raw-output/access-om2-01/01deg_jra55v140_iaf_cycle3/output656/ocean/ocean-3d-v-1-monthly-pow02-ym_2000_01.nc','~/ocean-3d-v-1-monthly-pow02-ym_2000_01.nc')]:
    ds = xr.open_dataset(fname_in, decode_timedelta=False)
    
    for dim in ds.dims:
        try:
            ds = ds.isel(**{dim: [0, 1], "drop": False})
        except IndexError:
            ds = ds.isel(**{dim: [0], "drop": False})

    print(f"Finished for {fname_out}")
        
    ds.to_netcdf(fname_out)
```

following https://gist.github.com/julia-neme/8f3db08c72760205ed9aa0ff77470ff8 & https://forum.access-hive.org.au/t/intake-bug/5398
