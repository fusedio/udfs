<!--fused:preview-->
<p align="center"><img src="https://fused-magic.s3.us-west-2.amazonaws.com/thumbnails/udfs-staging/PC_File_Example.png" width="600" alt="UDF preview image"></p>

<!--fused:tags-->
Tags: `STAC` `MSFT-PC` `Rasterio` `Hillshade`

<!--fused:readme-->
# Overview

This udf uses Xarray-Spatial's surface tools on Planetary Computer data to quantify and visualize terrain landform data from the NASADEM digital elevation model for McMinnville, Tennessee.

# External links

- [source](https://github.com/microsoft/PlanetaryComputerExamples/blob/main/tutorials/surface_analysis.ipynb)

## Run this in any Jupyter Notebook

```python
import fused

udf = fused.load("https://github.com/fusedio/udfs/tree/main/public/PC_File_Example")
arr = fused.run(udf=udf)
arr
```
