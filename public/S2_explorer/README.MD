<!--fused:preview-->
<p align="center"><img src="https://fused-magic.s3.us-west-2.amazonaws.com/thumbnails/udfs-staging/s2_explorer_udf.png" width="600" alt="UDF preview image"></p>

<!--fused:readme-->
## Overview

Three channel Sentinel 2 satellite imaging: pseudo RGB bands composed from the original 13 Sentinel 2 bands: the defaults are: pseudoRed = Glacier Index = (B3 – B4) / (B3 + B4), 
pseudoGreen= B8 (Near InfraRed), 
pseudoBlue = B11 (Short Wave InfraRed) 

## Run this in any Jupyter Notebook

```python
import fused
import geopandas as gpd

udf_url = "https://github.com/fusedio/udfs/tree/main/public/S2_explorer"
udf = fused.load(udf_url)

bbox = gpd.GeoDataFrame.from_features(
    {"type": "FeatureCollection", "features": [{"id": "0", "type": "Feature", "properties": {"x": 1270, "y": 3048, "z": 13}, "geometry": {"type": "Polygon", "coordinates": [[[-124.1455078125, 41.77131167976406], [-124.1455078125, 41.73852846935915], [-124.189453125, 41.73852846935915], [-124.189453125, 41.77131167976406], [-124.1455078125, 41.77131167976406]]]}}]}
)

df = fused.run(udf, bbox=bbox)
df.image.isel(band=0).plot()
```
