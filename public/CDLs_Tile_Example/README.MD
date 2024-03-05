<!--fused:preview-->
<p align="center"><img src="https://fused-magic.s3.us-west-2.amazonaws.com/thumbnails/udfs-staging/CDLS_Tile_Example.png" width="600" alt="UDF preview image"></p>

<!--fused:tags-->
Tags: `raster` `aws` `bbox` `lulc` `30-meter`

<!--fused:readme-->
# Overview

This UDF shows how to open [Cropland Data Layer](https://www.nass.usda.gov/Research_and_Science/Cropland/SARS1a.php) (CDL) tiff files. The CDL is a categorical land cover dataset that provides information about the types of crops and land cover on agricultural lands in the United States. The CDL is produced by the United States Department of Agriculture (USDA) National Agricultural Statistics Service (NASS) on an annual basis.

# External links

- [Original Download Links](https://www.nass.usda.gov/Research_and_Science/Cropland/Release/index.php)

## Run this in any Jupyter Notebook

```python
import fused
import geopandas as gpd

udf_url = "https://github.com/fusedio/udfs/tree/main/public/CDLs_Tile_Example"
udf = fused.load(udf_url)

bbox = gpd.GeoDataFrame.from_features(
    {"type": "FeatureCollection", "features": [{"id": "0", "type": "Feature", "properties": {"x": 1270, "y": 3048, "z": 20}, "geometry": {"type": "Polygon", "coordinates": [[[-124.1455078125, 41.77131167976406], [-124.1455078125, 41.73852846935915], [-124.189453125, 41.73852846935915], [-124.189453125, 41.77131167976406], [-124.1455078125, 41.77131167976406]]]}}]}
)

arr = udf.run_local(bbox=bbox)
arr
```
