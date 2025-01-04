<!--fused:pin=13-->
<!--fused:preview-->
<p align="center"><img src="https://fused-magic.s3.us-west-2.amazonaws.com/thumbnails/udfs-staging/alexgleith1.png" width="600" alt="UDF preview image"></p>

<!--fused:tags-->
Tags: `band-algebra` `aws` `msft-pc` `stac`

<!--fused:readme-->
# Geoparquet STAC Example Publishing Land Productivity Analysis

## Overview

This visualisation is from a Land Productivity analysis as part of the
[Land Degradation Neutrality](https://www.unccd.int/land-and-life/land-degradation-neutrality/overview)
component of the UN Sustainability Goal, 15.3.

The interesting part of this use case for Fused is that it all runs serverless, so there are
hundreds of data packages on S3, containing STAC Items pointing at Cloud Optimised GeoTIFF assets
and rather than running a STAC API service as an index, we've created a single Geoparquet
file using [STAC Geoparquet](https://stac-utils.github.io/stac-geoparquet), which is then accessed
using the [stac-rs](https://github.com/stac-utils/stac-rs) tool and
[odc-stac](https://odc-stac.readthedocs.io/en/latest/_api/odc.stac.load.html).

The end result is a scalable, dynamic visualisation with only Fused as a backend.

## External links

- [Leaflet visualisation](https://data.ldn.auspatious.com/testing/map.html?year=2023#10/17.2975/-88.3088)

## Run this in any Jupyter Notebook

```python
import fused
udf = fused.load("https://github.com/fusedio/udfs/tree/main/public/Geoparquet_STAC_Example")
arr = fused.run(udf=udf, x=4092, y=2241, z=15)
arr
```
