<!--fused:preview-->
<p align="center"><img src="https://fused-magic.s3.us-west-2.amazonaws.com/thumbnails/udfs-staging/Duckdb_H3_Example.png" width="600" alt="UDF preview image"></p>

<!--fused:tags-->
Tags: `aggregation` `parquet` `duckdb` `h3-vector`

<!--fused:readme-->
## Overview

This UDF shows how to open NYC yellow taxi trip dataset using DuckDB and aggregate the pickups using [H3-DuckDB](https://github.com/isaacbrodsky/h3-duckdb).

## External links

- [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

## Run this in any Jupyter Notebook

```python
import fused

udf = fused.load("https://github.com/fusedio/udfs/tree/main/public/DuckDB_H3_Example")
gdf = fused.run(udf=udf)
gdf
```
