
# Fused Multi-Step Job

## Get started
```python
# Import UDFs
from udf_DC_File_Example import DC_File_Example

# Instantiate individual jobs
job_DC_File_Example = DC_File_Example(url='https://www2.census.gov/geo/tiger/TIGER_RD18/STATE/11_DISTRICT_OF_COLUMBIA/11/tl_rd22_11_bg.zip')

# Instantiate multi-step job
job = fused.job([job_DC_File_Example])

# Run locally
job.run_local(file_id=0, chunk_id=0)

# Run remotely
job.run_remote(output_table='output_table_name')
```
