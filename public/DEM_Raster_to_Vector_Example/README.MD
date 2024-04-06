<!--fused:preview-->
<p align="center"><img src="https://fused-magic.s3.us-west-2.amazonaws.com/thumbnails/udfs-staging/DEM_Tile_Example.png" width="600" alt="UDF preview image"></p>

<!--fused:tags-->
Tags: `dem` `elevation` `raster` `30-meter`

<!--fused:readme-->
#### Overview

This UDF shows an example of vectorizing a raster image, based on a minimum elevation threshold.

The data for this UDF comes from the [Copernicus DEM](https://github.com/fusedio/udfs/tree/main/public/DEM_Tile_Example) example UDF.


## Run this in any Jupyter Notebook

```python
import fused

udf = fused.load("https://github.com/fusedio/udfs/tree/main/public/DEM_Raster_to_Vector_Example")
gdf = fused.run(udf=udf)
gdf
```
