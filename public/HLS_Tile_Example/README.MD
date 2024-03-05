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
import geopandas as gpd

udf_url = "https://github.com/fusedio/udfs/tree/main/public/HLS_Tile_Example"
udf = fused.load(udf_url)

bbox = gpd.GeoDataFrame.from_features(
    {"type": "FeatureCollection", "features": [{"id": "0", "type": "Feature", "properties": {"x": 5241, "y": 12667, "z": 15}, "geometry": {"type": "Polygon", "coordinates": [[[-124.1455078125, 41.77131167976406], [-124.1455078125, 41.73852846935915], [-124.189453125, 41.73852846935915], [-124.189453125, 41.77131167976406], [-124.1455078125, 41.77131167976406]]]}}]},
    crs="EPSG:4326"
)

gdf = udf.run_local(bbox=bbox, username="your_username", password="your_password")
gdf
```