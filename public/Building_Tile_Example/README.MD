<!--fused:preview-->
<p align="center"><img src="https://fused-magic.s3.us-west-2.amazonaws.com/thumbnails/udfs-staging/Building_Tile_Example.png" width="600" alt="UDF preview image"></p>

<!--fused:tags-->
Tags: `us` `buildings` `footprint` `geoparquet` `microsoft`

<!--fused:readme-->

Machine learning detected buildings footprints for United State using Bing Maps imagery. 
[Link](https://github.com/microsoft/USBuildingFootprints)

## Run this in any Jupyter Notebook

```python
import fused
import geopandas as gpd

udf_url = "https://github.com/fusedio/udfs/tree/main/public/Building_Tile_Example"
udf = fused.load(udf_url)

bbox = gpd.GeoDataFrame.from_features(
    {"type": "FeatureCollection", "features": [{"id": "0", "type": "Feature", "properties": {"x": 1270, "y": 3048, "z": 20}, "geometry": {"type": "Polygon", "coordinates": [[[-124.1455078125, 41.77131167976406], [-124.1455078125, 41.73852846935915], [-124.189453125, 41.73852846935915], [-124.189453125, 41.77131167976406], [-124.1455078125, 41.77131167976406]]]}}]}
)

gdf = udf.run_local(bbox=bbox)
gdf = gpd.GeoDataFrame(gdf, geometry='geometry', crs='epsg:4326')
gdf.plot()
```
