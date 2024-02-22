
# Fused Multi-Step Job

## Get started
```python
# Import UDFs
from udf_violet_krill import violet_krill

# Instantiate individual jobs
job_violet_krill = violet_krill(bbox=None, n=10)

# Instantiate multi-step job
job = fused.job([job_violet_krill])

# Run locally
job.run_local(file_id=0, chunk_id=0)

# Run remotely
job.run_remote(output_table='output_table_name')
```
