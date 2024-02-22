
# Fused Multi-Step Job

## Get started
```python
# Import UDFs
from udf_LULC_Tile_Example import LULC_Tile_Example

# Instantiate individual jobs
job_LULC_Tile_Example = LULC_Tile_Example(bbox, year='2022')

# Instantiate multi-step job
job = fused.job([job_LULC_Tile_Example])

# Run locally
job.run_local(file_id=0, chunk_id=0)

# Run remotely
job.run_remote(output_table='output_table_name')
```
