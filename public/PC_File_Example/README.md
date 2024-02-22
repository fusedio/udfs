
# Fused Multi-Step Job

## Get started
```python
# Import UDFs
from udf_PC_File_Example import PC_File_Example

# Instantiate individual jobs
job_PC_File_Example = PC_File_Example()

# Instantiate multi-step job
job = fused.job([job_PC_File_Example])

# Run locally
job.run_local(file_id=0, chunk_id=0)

# Run remotely
job.run_remote(output_table='output_table_name')
```
