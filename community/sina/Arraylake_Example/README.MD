<!--fused:preview-->
<p align="center"><img src="https://docs.earthmover.io/assets/images/fused-screenshot-Arraylake_RGB_Image-c42900408564d13df871564d416f6c9c.png" width="600" alt="Arraylake UDF preview image"></p>

<!--fused:tags-->
Tags: `raster` `Zarr` `Arraylake` `datacube`

<!--fused:readme-->
## Overview

This UDF shows how to open a continental-scale Zarr data cube stored in Arraylake and visualize it in Fused.
This datacube is a monthly composite at 30m resolution generated from Sentinel-2 data over South America.

The UDF opens a dataset from Arraylake with Xarray, subsets it to a bounding box, and coarsens it to an appropriate resolution.

## External links

- [Earthmover Blog Post on Planetary-Scale Data Cubes](https://earthmover.io/blog/serverless-datacube-pipeline)

## Run this in any Jupyter Notebook

```python
import fused

udf = fused.load("https://github.com/fusedio/udfs/tree/main/public/Arraylake_Example")
gdf = fused.run(udf=udf)
gdf
```


