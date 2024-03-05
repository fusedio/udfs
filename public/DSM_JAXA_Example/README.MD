<!--fused:preview-->
<p align="center"><img src="https://fused-magic.s3.us-west-2.amazonaws.com/thumbnails/udfs-staging/DSM_JAXA_Example.png" width="600" alt="UDF preview image"></p>

<!--fused:tags-->
Tags: `raster` `elevation` `30-meter`

<!--fused:readme-->
## Overview

ALOS Global Digital Surface Model "ALOS World 3D - 30m (AW3D30)" captured by the PRISM optical sensor on the ALOS satellite, with a 30-meter horizontal resolution.

## External links
- [JAXA Digital Surface Model](https://data.earth.jaxa.jp/en/datasets/#/id/JAXA.EORC_ALOS.PRISM_AW3D30.v3.2_global)
- [STAC Collection File](https://s3.ap-northeast-1.wasabisys.com/je-pds/cog/v1/JAXA.EORC_ALOS.PRISM_AW3D30.v3.2_global/collection.json)

## Run this in any Jupyter Notebook

```python
import fused
import geopandas as gpd

udf_url = "https://github.com/fusedio/udfs/tree/main/public/DSM_JAXA_Example"
udf = fused.load(udf_url)

bbox = gpd.GeoDataFrame.from_features(
    {"type": "FeatureCollection", "features": [{"id": "0", "type": "Feature", "properties": {"x": 655, "y": 1583, "z": 12}, "geometry": {"type": "Polygon", "coordinates": [[[-124.1455078125, 41.77131167976406], [-124.1455078125, 41.73852846935915], [-124.189453125, 41.73852846935915], [-124.189453125, 41.77131167976406], [-124.1455078125, 41.77131167976406]]]}}]},
    crs="EPSG:4326"
)

arr = udf.run_local(bbox=bbox)
arr
```
