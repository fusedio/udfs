
# Fused Multi-Step Job

## Get started
```python
# Import UDFs
from udf_HLS_Tile_Example import HLS_Tile_Example

# Instantiate individual jobs
job_HLS_Tile_Example = HLS_Tile_Example(bbox, collection_id='HLSS30.v2.0', band='B8A', date_range='2023-11/2024-01', max_cloud_cover=25, n_mosaic=5, min_max=(0, 3000), username='your_username', password='your_password', env='earthdata')

# Instantiate multi-step job
job = fused.job([job_HLS_Tile_Example])

# Run locally
job.run_local(file_id=0, chunk_id=0)

# Run remotely
job.run_remote(output_table='output_table_name')
```
