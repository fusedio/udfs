<!--fused:preview-->
<p align="center"><img src="https://fused-magic.s3.us-west-2.amazonaws.com/thumbnails/udfs-staging/DuckDB_H3_Example_Tile.png" width="600" alt="UDF preview image"></p>

<!--fused:readme-->
## Overview

This UDF shows how to open NYC yellow taxi trip dataset using DuckDB and aggregate the pickups using [H3-DuckDB](https://github.com/isaacbrodsky/h3-duckdb) as tiles.

## External links

- [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

## Run this in any Jupyter Notebook

```python
import fused

udf = fused.load("https://github.com/fusedio/udfs/tree/main/public/DuckDB_H3_Example_Tile")
gdf = fused.run(udf=udf, x=2412, y=3078, z=13)
gdf
```
