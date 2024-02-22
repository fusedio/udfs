
# Fused Multi-Step Job

## Get started
```python
# Import UDFs
from udf_Charm_Demo import Charm_Demo

# Instantiate individual jobs
job_Charm_Demo = Charm_Demo(short='CORN, GRAIN - ACRES HARVESTED', resource='Corn', price=60, density_min=10)

# Instantiate multi-step job
job = fused.job([job_Charm_Demo])

# Run locally
job.run_local(file_id=0, chunk_id=0)

# Run remotely
job.run_remote(output_table='output_table_name')
```
