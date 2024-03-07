<!--fused:readme-->

UDF to spatially query the Overture Buildings dataset. 

This UDF fetches only the buildings that fall within the area of interest defined in a GeoDataFrame, specified with a `bbox` parameter. This means you can use this code to simplify your workflows by loading only the fraction of data you care about.

## Run this in any Jupyter Notebook

```python
import fused
import geopandas as gpd

udf_url = "https://github.com/fusedio/udfs/tree/main/public/Overture_Buildings"
udf = fused.load(udf_url)

bbox = gpd.GeoDataFrame.from_features(
    {"type": "FeatureCollection", "features": [{"id": "0", "type": "Feature", "properties": {"x": 5241, "y": 12667, "z": 15}, "geometry": {"type": "Polygon", "coordinates": [[[-124.1455078125, 41.77131167976406], [-124.1455078125, 41.73852846935915], [-124.189453125, 41.73852846935915], [-124.189453125, 41.77131167976406], [-124.1455078125, 41.77131167976406]]]}}]},
    crs="EPSG:4326"
)

gdf = udf.run_local(bbox=bbox)
gdf
```