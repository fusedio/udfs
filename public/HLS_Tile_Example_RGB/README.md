
# Fused Multi-Step Job

## Get started
```python
# Import UDFs
from udf_HLS_Tile_Example_RGB import HLS_Tile_Example_RGB

# Instantiate individual jobs
job_HLS_Tile_Example_RGB = HLS_Tile_Example_RGB(bbox, collection_id='HLSS30.v2.0', bands=['B04', 'B03', 'B02'], date_range='2023-11/2024-01', max_cloud_cover=25, n_mosaic=5, username='your_username', password='your_password')

# Instantiate multi-step job
job = fused.job([job_HLS_Tile_Example_RGB])

# Run locally
job.run_local(file_id=0, chunk_id=0)

# Run remotely
job.run_remote(output_table='output_table_name')
```
