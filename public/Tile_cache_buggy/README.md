
# Fused Multi-Step Job

## Get started
```python
# Import UDFs
from udf_Tile_cache_buggy import Tile_cache_buggy

# Instantiate individual jobs
job_Tile_cache_buggy = Tile_cache_buggy(bbox=None, collections=['cop-dem-glo-30'], date_range='2021', n_mosaic=1, overview_level=0)

# Instantiate multi-step job
job = fused.job([job_Tile_cache_buggy])

# Run locally
job.run_local(file_id=0, chunk_id=0)

# Run remotely
job.run_remote(output_table='output_table_name')
```
