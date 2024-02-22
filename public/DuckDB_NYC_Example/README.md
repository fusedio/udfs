
# Fused Multi-Step Job

## Get started
```python
# Import UDFs
from udf_DuckDB_NYC_Example import DuckDB_NYC_Example

# Instantiate individual jobs
job_DuckDB_NYC_Example = DuckDB_NYC_Example(bbox=None, agg_factor=3, min_count=5)

# Instantiate multi-step job
job = fused.job([job_DuckDB_NYC_Example])

# Run locally
job.run_local(file_id=0, chunk_id=0)

# Run remotely
job.run_remote(output_table='output_table_name')
```
