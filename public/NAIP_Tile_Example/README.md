
# Fused Multi-Step Job

## Get started
```python
# Import UDFs
from udf_NAIP_Tile_Example import NAIP_Tile_Example

# Instantiate individual jobs
job_NAIP_Tile_Example = NAIP_Tile_Example(bbox, var='NDVI', chip_len='256', buffer_degree=0.0)

# Instantiate multi-step job
job = fused.job([job_NAIP_Tile_Example])

# Run locally
job.run_local(file_id=0, chunk_id=0)

# Run remotely
job.run_remote(output_table='output_table_name')
```
