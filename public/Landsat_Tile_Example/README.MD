<!--fused:preview-->
<p align="center"><img src="https://fused-magic.s3.us-west-2.amazonaws.com/thumbnails/udfs-staging/Landsat_Tile_Example.png" width="600" alt="UDF preview image"></p>

<!--fused:tags-->
Tags: `raster` `30-meter` `aws`

<!--fused:readme-->

## Overview

Landsat Collection 2 offers global Level-2 surface reflectance and temperature products. These are generated from Collection 2 Level-1 inputs meeting specific criteria, including a Solar Zenith Angle constraint of <76 degrees and required auxiliary data inputs.

## External links

- [Landsat Official Website](https://www.usgs.gov/landsat-missions/landsat-collection-2-level-2-science-products)

## Run this in any Jupyter Notebook

```python
import fused
import geopandas as gpd

udf_url = "https://github.com/fusedio/udfs/tree/main/public/LULC_Tile_Example"
udf = fused.load(udf_url)

bbox = gpd.GeoDataFrame.from_features(
    {"type": "FeatureCollection", "features": [{"id": "0", "type": "Feature", "properties": {"x": 5241, "y": 12667, "z": 15}, "geometry": {"type": "Polygon", "coordinates": [[[-124.1455078125, 41.77131167976406], [-124.1455078125, 41.73852846935915], [-124.189453125, 41.73852846935915], [-124.189453125, 41.77131167976406], [-124.1455078125, 41.77131167976406]]]}}]},
    crs="EPSG:4326"
)

arr = udf.run_local(bbox=bbox)
arr
```