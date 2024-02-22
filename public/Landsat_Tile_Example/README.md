
# Fused Multi-Step Job

## Get started
```python
# Import UDFs
from udf_Landsat_Tile_Example import Landsat_Tile_Example

# Instantiate individual jobs
job_Landsat_Tile_Example = Landsat_Tile_Example(bbox, time_of_interest='2023-09-01/2023-10-30', red_band='red', nir_band='nir08', collection='landsat-c2-l2')

# Instantiate multi-step job
job = fused.job([job_Landsat_Tile_Example])

# Run locally
job.run_local(file_id=0, chunk_id=0)

# Run remotely
job.run_remote(output_table='output_table_name')
```
