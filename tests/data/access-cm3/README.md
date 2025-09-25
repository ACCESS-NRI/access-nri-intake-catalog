Generated with the following, and then manually pruned to reduce wasted file size
```python
from pathlib import Path
import xarray as xr


BASE_PATH = Path("/g/data/zv30/non-cmip/ACCESS-CM3/cm3-run-11-08-2025-25km-beta-om3-new-um-params/archive/1981")

OUTPUT_BASE_PATH = Path("/home/189/ct1163/access-nri-intake-catalog/tests/data/access-cm3")

inputs = [path for path in BASE_PATH.glob("**/*.nc")]
outputs = [(Path(OUTPUT_BASE_PATH) / Path(*output.parts[-3:])) for output in outputs]

for input_file, output_file in zip(inputs, outputs):
    input_file = str(input_file)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file = str(output_file)
    ds = xr.open_dataset(input_file, decode_timedelta=False)
    
    for dim in ds.dims:
        ds = ds.isel(**{dim : [0], "drop" : False})

    print(f"Finished for {output_file}")
        
    ds.to_netcdf(output_file)
```
