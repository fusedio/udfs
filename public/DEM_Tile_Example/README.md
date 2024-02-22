
# Fused Multi-Step Job

## Get started
```python
# Import UDFs
from udf_DEM_Tile_Example import DEM_Tile_Example

# Instantiate individual jobs
job_DEM_Tile_Example = DEM_Tile_Example(bbox, provider='AWS')

# Instantiate multi-step job
job = fused.job([job_DEM_Tile_Example])

# Run locally
job.run_local(file_id=0, chunk_id=0)

# Run remotely
job.run_remote(output_table='output_table_name')
```
