<!--fused:preview-->
<p align="center"><img src="https://fused-magic.s3.us-west-2.amazonaws.com/thumbnails/udfs-staging/Duckdb_NYC311_Example.png" width="600" alt="UDF preview image"></p>

<!--fused:tags-->
Tags: `file` `vector` `aws` `tutorial`

<!--fused:readme-->
## Overview

This UDF shows how to open NYC yellow taxi trip dataset using DuckDB.

## External links

- [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

## Run this in any Jupyter Notebook

```python
import fused

udf = fused.load("https://github.com/fusedio/udfs/tree/main/public/DuckDB_NYC_Example")
gdf = fused.run(udf=udf)
gdf
```
