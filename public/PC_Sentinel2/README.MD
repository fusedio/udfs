<!--fused:preview-->
<p align="center"><img src="https://fused-magic.s3.us-west-2.amazonaws.com/docs_assets/github_udfs_repo/mecca_sentinel2.png" width="600" alt="UDF preview image"></p>

<!--fused:tags-->
Tags: `dem` `raster` `file` `pc`

<!--fused:readme-->
# Overview

This UDF renders a RGB image from the Planetary Computer Sentinel-2 dataset.

The Sentinel-2 program offers global imagery across thirteen spectral bands with resolutions ranging from 10m to 60m and a revisit interval of about five days. This dataset encompasses the global Sentinel-2 archive from 2016 to the present, processed to Level 2A (bottom-of-atmosphere) using Sen2Cor and converted to cloud-optimized GeoTIFF format.


# External links

- [source](https://planetarycomputer.microsoft.com/dataset/sentinel-2-l2a)

## Run this in any Jupyter Notebook

```python
import fused

udf = fused.load("https://github.com/fusedio/udfs/tree/main/public/PC_Sentinel2")
arr = fused.run(udf=udf, x=2915, y=6546, z=14)
arr
```