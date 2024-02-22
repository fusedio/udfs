
# Fused Multi-Step Job

## Get started
```python
# Import UDFs
from udf_CDLs_Tile_Example import CDLs_Tile_Example

# Instantiate individual jobs
job_CDLs_Tile_Example = CDLs_Tile_Example(bbox, year='2022', crop_type='', chip_len=256)

# Instantiate multi-step job
job = fused.job([job_CDLs_Tile_Example])

# Run locally
job.run_local(file_id=0, chunk_id=0)

# Run remotely
job.run_remote(output_table='output_table_name')
```
