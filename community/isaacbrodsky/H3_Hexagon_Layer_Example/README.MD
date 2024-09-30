<!--fused:preview-->
<p align="center"><img src="https://fused-magic.s3.us-west-2.amazonaws.com/thumbnails/udfs-staging/H3_Hexagon_Layer_Example.png" width="600" alt="UDF preview image"></p>

<!--fused:tags-->
Tags:  `Aggregation` `H3-polygon` `NYC Open Data` `parquet`

<!--fused:readme-->
## Overview

This UDF shows how to open NYC yellow taxi trip dataset using DuckDB and aggregate the pickups using [H3-DuckDB](https://github.com/isaacbrodsky/h3-duckdb). Results are visualized as [hexagons](https://deck.gl/docs/api-reference/geo-layers/h3-hexagon-layer).

## External links

- [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

## Run this in any Jupyter Notebook

```python
import fused

udf = fused.load("https://github.com/fusedio/udfs/tree/main/public/H3_Hexagon_Layer_Example")
gdf = fused.run(udf=udf)
gdf
```
