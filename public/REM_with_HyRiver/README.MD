<!--fused:preview-->
<p align="center"><img src="https://fused-magic.s3.us-west-2.amazonaws.com/thumbnails/udfs-staging/REM_with_HyRiver.png" width="600" alt="UDF preview image"></p>

<!--fused:tags-->
Tags: `dem` `10-meter` `tutorial` `river`

<!--fused:readme-->
## Overview

Height Above River (HAR) rasters, also known as Relative Elevation Models, are normalized rasters created by detrending a DEM and then subtracting this from the bare-earth DEM. These models facilitate the visualization of fluvial landforms, which might be difficult to identify using only an aerial image or a standard DEM. This UDF computes REM for a segment of Carson River and visualizes the results using `xarray-spatial` and `datashader`.

We use HyRiver software stack to download the required Digital Elevation Model (DEM) and river flowlines within a bounding box.

## External links

- [HyRiver](https://docs.hyriver.io/)
- [REM](https://wadnr.maps.arcgis.com/apps/Cascade/index.html?appid=36b4887370d141fcbb35392f996c82d9)

## Run this in any Jupyter Notebook

```python
import fused
import geopandas as gpd

udf_url = "https://github.com/fusedio/udfs/tree/main/public/REM_with_HyRiver"
udf = fused.load(udf_url)
img = udf.run_local()
img
```