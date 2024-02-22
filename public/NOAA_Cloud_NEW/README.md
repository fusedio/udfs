
# Fused Multi-Step Job

## Get started
```python
# Import UDFs
from udf_NOAA_Cloud_NEW import NOAA_Cloud_NEW

# Instantiate individual jobs
job_NOAA_Cloud_NEW = NOAA_Cloud_NEW(bbox=None, bounds='-180,-10,-65,70', res=100, yr=2024, month='02', day='19', hr='13')

# Instantiate multi-step job
job = fused.job([job_NOAA_Cloud_NEW])

# Run locally
job.run_local(file_id=0, chunk_id=0)

# Run remotely
job.run_remote(output_table='output_table_name')
```
