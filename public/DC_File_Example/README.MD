<!--fused:preview-->
<p align="center"><img src="https://fused-magic.s3.us-west-2.amazonaws.com/thumbnails/udfs-staging/DC_File_Example.png" width="600" alt="UDF preview image"></p>

<!--fused:readme-->
# Overview

This UDF is an example of how to load data from Census [TIGER/Line Shapefiles](https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html).

# External links

- [source](https://www2.census.gov/geo/tiger/TIGER_RD18/STATE/11_DISTRICT_OF_COLUMBIA/11/)

## Run this in any Jupyter Notebook

```python
import fused

udf_url = "https://github.com/fusedio/udfs/tree/main/public/DC_File_Example"
udf = fused.load(udf_url)
gdf = udf.run_local()
gdf
```
