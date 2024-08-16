<!--fused:pin=5-->
<!--fused:preview-->
<p align="center"><img src="https://fused-magic.s3.us-west-2.amazonaws.com/thumbnails/udfs-staging/cuatrocienegas.png" width="600" alt="UDF preview image"></p>

<!--fused:tags-->
Tags: `dem` `raster` `file` `pc`

<!--fused:readme-->
# Overview

This UDF is particularly useful for generating cloud-free, vegetation-optimized composite images from time series satellite data, which can be used for various remote sensing applications and analyses.

It first retrieves satellite imagery data from Microsoft's Planetary Computer based on specified parameters (collection, bands, time range, and query). It then processes the retrieved data to create a composite image using a selected method (max, median, or min) across the time series.

The function allows for flexibility in choosing the satellite data source (MODIS, Landsat, or Sentinel-2), specifying the area of interest (bbox), time range, and cloud cover constraints. It also provides options for customizing the output, such as the chip size and compositing method.

# External links

- [source](https://planetarycomputer.microsoft.com/dataset/)

## Run this in any Jupyter Notebook

```python
import fused

udf = fused.load("https://github.com/fusedio/udfs/tree/main/public/Satellite_Greenest_Pixel")
arr = fused.run(udf=udf, x=2915, y=6546, z=14)
arr
```
