
# Fused Multi-Step Job

## Get started
```python
# Import UDFs
from udf_Daymet_Single import Daymet_Single

# Instantiate individual jobs
job_Daymet_Single = Daymet_Single(latlngs_json, start_year=2021, end_year=2023)

# Instantiate multi-step job
job = fused.job([job_Daymet_Single])

# Run locally
job.run_local(file_id=0, chunk_id=0)

# Run remotely
job.run_remote(output_table='output_table_name')
```
