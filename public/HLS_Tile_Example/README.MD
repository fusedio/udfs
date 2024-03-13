<!--fused:preview-->
<p align="center"><img src="https://fused-magic.s3.us-west-2.amazonaws.com/thumbnails/udfs-staging/HLS_Tile_Example.png" width="600" alt="UDF preview image"></p>

<!--fused:tags-->
Tags: `30-meter` `aws` `nasa` `raster`

<!--fused:readme-->

## Overview
This UDF demonstrate how to access the NASA’s Harmonized Landsat Sentinel-2 (HLS) version 2 assets. Please visit [Earth Data Portal](https://urs.earthdata.nasa.gov) to register and manage your Earthdata Login account.

## External links

- [Data Discovery Example](https://nasa-openscapes.github.io/2021-Cloud-Hackathon/tutorials/02_Data_Discovery_CMR-STAC_API.html)
- [Name of link](https://example.com)

## Run this in any Jupyter Notebook

Please set `username` and `password` arguments. Create credentials at: https://urs.earthdata.nasa.gov to register and manage your Earthdata Login account.

```python
import fused

udf = fused.load("https://github.com/fusedio/udfs/tree/main/public/HLS_Tile_Example")
gdf = fused.run(udf=udf, x=5241, y=12667, z=15,  username="your_username", password="your_password")
gdf
```