<!--fused:preview-->
<p align="center"><img src="https://fused-magic.s3.us-west-2.amazonaws.com/thumbnails/udfs-staging/NYC_Taxi_Arc_Layer_Example.png" width="600" alt="UDF preview image"></p>

<!--fused:tags-->
Tags: `file` `vector` `arc` `taxi`

<!--fused:readme-->
## Overview

This UDF shows how to open NYC yellow taxi trip dataset and visualize trips as [arcs](https://deck.gl/docs/api-reference/layers/arc-layer).

## External links

- [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

## Run this in any Jupyter Notebook

```python
import fused

udf = fused.load("https://github.com/fusedio/udfs/tree/main/public/NYC_Taxi_Arc_Layer_Example")
gdf = fused.run(udf=udf)
gdf
```
