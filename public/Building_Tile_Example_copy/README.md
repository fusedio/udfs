
# Fused Multi-Step Job

## Get started
```python
# Import UDFs
from udf_Building_Tile_Example_copy import Building_Tile_Example_copy

# Instantiate individual jobs
job_Building_Tile_Example_copy = Building_Tile_Example_copy(bbox, table_path='s3://fused-asset/infra/building_msft_us')

# Instantiate multi-step job
job = fused.job([job_Building_Tile_Example_copy])

# Run locally
job.run_local(file_id=0, chunk_id=0)

# Run remotely
job.run_remote(output_table='output_table_name')
```
