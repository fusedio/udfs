<!--fused:preview-->
<p align="center"><img src="https://fused-magic.s3.us-west-2.amazonaws.com/thumbnails/udfs-staging/NAIP_Tile_Example.png" width="600" alt="UDF preview image"></p>

<!--fused:tags-->
Tags: `high-res` `raster` `aws` `bbox`

<!--fused:readme-->
# Overview

NAIP aerial imagery, obtained seasonally in the U.S., aims to deliver digital ortho photography to the government and the public within a year of acquisition.

# External links

- [National Agriculture Imagery Program (NAIP)](https://naip-usdaonline.hub.arcgis.com/)

## Run this in any Jupyter Notebook

```python
import fused
import geopandas as gpd

udf_url = "https://github.com/fusedio/udfs/tree/main/public/NAIP_Tile_Example"
udf = fused.load(udf_url)

bbox = gpd.GeoDataFrame.from_features(
    {"type": "FeatureCollection", "features": [{"id": "0", "type": "Feature", "properties": {"x": 5241, "y": 12667, "z": 15}, "geometry": {"type": "Polygon", "coordinates": [[[-124.1455078125, 41.77131167976406], [-124.1455078125, 41.73852846935915], [-124.189453125, 41.73852846935915], [-124.189453125, 41.77131167976406], [-124.1455078125, 41.77131167976406]]]}}]},
    crs="EPSG:4326"
)

arr = udf.run_local(bbox=bbox)
arr
```