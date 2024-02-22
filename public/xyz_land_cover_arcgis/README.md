
# Fused Multi-Step Job

## Get started
```python
# Import UDFs
from udf_xyz_land_cover_arcgis import xyz_land_cover_arcgis

# Instantiate individual jobs
job_xyz_land_cover_arcgis = xyz_land_cover_arcgis(bbox, thresh=10)

# Instantiate multi-step job
job = fused.job([job_xyz_land_cover_arcgis])

# Run locally
job.run_local(file_id=0, chunk_id=0)

# Run remotely
job.run_remote(output_table='output_table_name')
```
