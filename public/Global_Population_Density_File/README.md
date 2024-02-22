
# Fused Multi-Step Job

## Get started
```python
# Import UDFs
from udf_Global_Population_Density_File import Global_Population_Density_File

# Instantiate individual jobs
job_Global_Population_Density_File = Global_Population_Density_File(lat, lng=30, dataset='general', version='1.5.4')

# Instantiate multi-step job
job = fused.job([job_Global_Population_Density_File])

# Run locally
job.run_local(file_id=0, chunk_id=0)

# Run remotely
job.run_remote(output_table='output_table_name')
```
