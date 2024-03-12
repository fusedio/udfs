<!--fused:preview-->
<p align="center"><img src="https://fused-magic.s3.us-west-2.amazonaws.com/thumbnails/udfs-staging/Overture_Map_Example.png" width="600" alt="UDF preview image"></p>

<!--fused:tags-->
Tags: `buildings` `footprint` `geoparquet` `global` `places` `overture` `vector` `admin`

<!--fused:readme-->
Buildings footprints, places of interest (POIs), admin boundaries, and transportation globally from [Overture Maps](https://overturemaps.org/).

## Parameters

- `release`: Overture release ID. Defaults to `2024-02-15-alpha-0`.
- `theme`: One of `buildings` (default), `base`, `places`, `transportation`, `admins`.
- `type`: One of `landUse`, `water`, `administrativeBoundary`, `place`, `connector`, `segment`, `building`. If not specified, the default type for the theme will be used.
- `use_columns`: Load only these columns if specified. Default is to load all columns.

## Run this in any Jupyter Notebook

```python
import fused
import geopandas as gpd

udf_url = "https://github.com/fusedio/udfs/tree/main/public/Overture_Maps_Example"
udf = fused.load(udf_url)

gdf = fused.run(udf, x=2622, y=6333, z=14)
gdf = gpd.GeoDataFrame(gdf, geometry='geometry', crs='epsg:4326')
gdf.plot()
```
