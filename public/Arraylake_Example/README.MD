<!--fused:preview-->
<p align="center"><img src="https://avatars.githubusercontent.com/u/95875797?v=4" width="600" alt="UDF preview image"></p>

<!--fused:tags-->
Tags:  `raster` `Zarr` `Arraylake` `datacube`

<!--fused:readme-->
## Overview

This UDF shows how to open a continental-scale Zarr data cube stored in Arraylake and visualize it in Fused.

## External links

- [Earthmover Blog Post on Planetary-Scale Data Cubes](https://earthmover.io/blog/serverless-datacube-pipeline)

## Run this in any Jupyter Notebook

```python
import fused

udf = fused.load("https://github.com/fusedio/udfs/tree/main/public/Arraylake_Example")
gdf = fused.run(udf=udf)
gdf
```


