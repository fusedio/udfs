<!--fused:preview-->
<p align="center"><img src="https://fused-magic.s3.us-west-2.amazonaws.com/thumbnails/udfs-staging/CDLS_Tile_Example.png" width="600" alt="UDF preview image"></p>

<!--fused:tags-->
Tags: `raster` `aws` `bbox` `lulc` `30-meter`

<!--fused:readme-->
# Overview

This UDF shows how to open [Cropland Data Layer](https://www.nass.usda.gov/Research_and_Science/Cropland/SARS1a.php) (CDL) tiff files. The CDL is a categorical land cover dataset that provides information about the types of crops and land cover on agricultural lands in the United States. The CDL is produced by the United States Department of Agriculture (USDA) National Agricultural Statistics Service (NASS) on an annual basis.

# External links

- [Original Download Links](https://www.nass.usda.gov/Research_and_Science/Cropland/Release/index.php)

## Run this in any Jupyter Notebook

```python
import fused

udf = fused.load("https://github.com/fusedio/udfs/tree/main/public/CDLs_Tile_Example")
arr = fused.run(udf=udf, x=9646, y=12321, z=15)
arr
```
