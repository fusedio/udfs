
# Fused Multi-Step Job

## Get started
```python
# Import UDFs
from udf_geos_18_minute_team import geos_18_minute_team

# Instantiate individual jobs
job_geos_18_minute_team = geos_18_minute_team(i=10, product_name='ABI-L2-CMIPF', bucket_name='noaa-goes18', datestr='2024-01-31', offset='0', band=8, x_res=4000, y_res=4000)

# Instantiate multi-step job
job = fused.job([job_geos_18_minute_team])

# Run locally
job.run_local(file_id=0, chunk_id=0)

# Run remotely
job.run_remote(output_table='output_table_name')
```
