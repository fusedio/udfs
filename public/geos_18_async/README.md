
# Fused Multi-Step Job

## Get started
```python
# Import UDFs
from udf_geos_18_async import geos_18_async

# Instantiate individual jobs
job_geos_18_async = geos_18_async(datestr='2024-02-05', start_i=0, end_i=6, band=8, product_name='ABI-L2-CMIPF')

# Instantiate multi-step job
job = fused.job([job_geos_18_async])

# Run locally
job.run_local(file_id=0, chunk_id=0)

# Run remotely
job.run_remote(output_table='output_table_name')
```
