
# Fused Multi-Step Job

## Get started
```python
# Import UDFs
from udf_SAR_Umbra_File_Example import SAR_Umbra_File_Example

# Instantiate individual jobs
job_SAR_Umbra_File_Example = SAR_Umbra_File_Example(raster_url='https://www.historicalcharts.noaa.gov/jpgs/NYH.jpg', overview_level=1, do_tranform=True)

# Instantiate multi-step job
job = fused.job([job_SAR_Umbra_File_Example])

# Run locally
job.run_local(file_id=0, chunk_id=0)

# Run remotely
job.run_remote(output_table='output_table_name')
```
