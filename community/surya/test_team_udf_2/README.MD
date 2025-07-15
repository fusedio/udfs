<!--fused:pin=1-->
<!--fused:tags-->
Tags: `overture` `coop`

<!--fused:readme-->
Buildings footprints, places of interest (POIs), admin boundaries, and transportation globally from [Overture Maps](https://overturemaps.org/).

## Parameters

- `release`: Overture release ID. Defaults to `2024-12-18-0`. Note that `.` should be replaced with `-` in the ID.
- `type`: One of `infrastructure`, `land`, `land_use`, `water`, `division`, `division_boundary`, `division_area`, `place`, `connector`, `segment`, `address`, `building` (default).
- `theme`: One of `buildings`, `base`, `places`, `transportation`, `divisions`, `addresses`. If not specified, this will be inferred from the type.
- `use_columns`: Load only these columns if specified. Default is to load all columns.

## Run this in any Jupyter Notebook

```python
import fused
import geopandas as gpd

udf = fused.load("https://github.com/fusedio/udfs/tree/main/public/Overture_Maps_Example")
gdf_output = fused.run(udf, x=2622, y=6333, z=14)
gdf = gpd.GeoDataFrame(gdf_output, geometry='geometry', crs='epsg:4326')
gdf.plot()
```
