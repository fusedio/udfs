
# Fused Multi-Step Job

## Get started
```python
# Import UDFs
from udf_Get_Isochrone import Get_Isochrone

# Instantiate individual jobs
job_Get_Isochrone = Get_Isochrone(lng, lat=40.776732, costing='auto', time_steps=[1, 5, 10, 15, 20, 25, 30])

# Instantiate multi-step job
job = fused.job([job_Get_Isochrone])

# Run locally
job.run_local(file_id=0, chunk_id=0)

# Run remotely
job.run_remote(output_table='output_table_name')
```
