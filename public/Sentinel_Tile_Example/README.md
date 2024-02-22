
# Fused Multi-Step Job

## Get started
```python
# Import UDFs
from udf_Sentinel_Tile_Example import Sentinel_Tile_Example

# Instantiate individual jobs
job_Sentinel_Tile_Example = Sentinel_Tile_Example(bbox, provider='AWS', time_of_interest='2023-11-01/2023-12-30')

# Instantiate multi-step job
job = fused.job([job_Sentinel_Tile_Example])

# Run locally
job.run_local(file_id=0, chunk_id=0)

# Run remotely
job.run_remote(output_table='output_table_name')
```
