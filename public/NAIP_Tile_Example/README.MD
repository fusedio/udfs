<!--fused:preview-->
<p align="center"><img src="https://fused-magic.s3.us-west-2.amazonaws.com/thumbnails/udfs-staging/NAIP_Tile_Example.png" width="600" alt="UDF preview image"></p>

<!--fused:tags-->
Tags: `high-res` `raster` `aws` `bbox`

<!--fused:readme-->
# Overview

NAIP aerial imagery, obtained seasonally in the U.S., aims to deliver digital ortho photography to the government and the public within a year of acquisition.

# External links

- [National Agriculture Imagery Program (NAIP)](https://naip-usdaonline.hub.arcgis.com/)

## Run this in any Jupyter Notebook

```python
import fused

udf = fused.load("https://github.com/fusedio/udfs/tree/main/public/NAIP_Tile_Example")
arr = fused.run(udf=udf, x=5241, y=12667, z=15)
arr
```