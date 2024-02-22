
# Fused Multi-Step Job

## Get started
```python
# Import UDFs
from udf_natural_earth_data import natural_earth_data

# Instantiate individual jobs
job_natural_earth_data = natural_earth_data(bbox=None, country_name='')

# Instantiate multi-step job
job = fused.job([job_natural_earth_data])

# Run locally
job.run_local(file_id=0, chunk_id=0)

# Run remotely
job.run_remote(output_table='output_table_name')
```
