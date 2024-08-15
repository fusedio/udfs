<!--fused:pin=7-->
<!--fused:preview-->
<p align="center"><img src="https://fused-magic.s3.us-west-2.amazonaws.com/thumbnails/udfs-staging/Landsat_Tile_Example.png" width="600" alt="UDF preview image"></p>

<!--fused:tags-->
Tags:  `Element84` `S3` `Band algebra` `Remove-clouds` `STAC`

<!--fused:readme-->

## Overview

Landsat Collection 2 offers global Level-2 surface reflectance and temperature products. These are generated from Collection 2 Level-1 inputs meeting specific criteria, including a Solar Zenith Angle constraint of <76 degrees and required auxiliary data inputs.

## External links

- [Landsat Official Website](https://www.usgs.gov/landsat-missions/landsat-collection-2-level-2-science-products)

## Run this in any Jupyter Notebook

```python
import fused

udf = fused.load("https://github.com/fusedio/udfs/tree/main/public/LULC_Tile_Example")
arr = fused.run(udf=udf, x=5241, y=12667, z=15)
arr
```
