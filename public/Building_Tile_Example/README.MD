<!--fused:preview-->
<p align="center"><img src="https://fused-magic.s3.us-west-2.amazonaws.com/thumbnails/udfs-staging/Building_Tile_Example.png" width="600" alt="UDF preview image"></p>

<!--fused:tags-->
Tags: `geoparquet` `Microsoft`

<!--fused:readme-->

Machine learning detected buildings footprints for United State using Bing Maps imagery.
[Link](https://github.com/microsoft/USBuildingFootprints)

## Run this in any Jupyter Notebook

```python
import fused
import geopandas as gpd

udf = fused.load("https://github.com/fusedio/udfs/tree/main/public/Building_Tile_Example")
gdf_output = fused.run(udf=udf, x=9646, y=12321, z=15)
gdf = gpd.GeoDataFrame(gdf_output, geometry='geometry', crs='epsg:4326')
gdf.plot()
```
