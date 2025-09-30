Generated with the following, and then manually pruned to reduce wasted file size
```python
from pathlib import Path
import xarray as xr


BASE_PATH = Path("/g/data/e14/afp599/access-esm/fs38_processed")

OUTPUT_BASE_PATH = Path("/home/189/ct1163/access-nri-intake-catalog/tests/data/cmip6")

inputs = [path for path in BASE_PATH.glob("*.nc")]
outputs = [(Path(OUTPUT_BASE_PATH) / inp.name) for inp in inputs]


for input_file, output_file in zip(inputs, outputs):
    input_file = str(input_file)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file = str(output_file)
    ds = xr.open_dataset(input_file, decode_timedelta=False)
    
    for dim in ds.dims:
        try:
            ds = ds.isel(**{dim: [0, 1], "drop": False})
        except IndexError:
            ds = ds.isel(**{dim: [0], "drop": False})

    print(f"Finished for {output_file}")
        
    ds.to_netcdf(output_file)
```
