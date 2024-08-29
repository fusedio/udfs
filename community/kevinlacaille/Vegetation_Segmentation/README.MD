<!--fused:preview-->
<p align="center"><img src="udf_example.png" width="600" alt="UDF preview image"></p>

<!--fused:tags-->
Tags: `Computer Vision` `Vegetation Analysis` `Segmentation`

<!--fused:readme-->
## Overview

This UDF performs scalable vegetation analysis using classical computer vision techniques. It allows users to choose a vegetation index, set threshold values, and dynamically process satellite imagery for real-time global vegetation segmentation, all without relying on machine learning models.


## External links
- [Visible Atmospherically Resistant Index (VARI)](https://space4water.org/space/visible-atmospherically-resistant-index-vari)

## Run The UDF

```python
import fused

udf = fused.load("https://github.com/fusedio/udfs/tree/main/public/Vegetation_Segmentation")
gdf = fused.run(udf=udf)
gdf
```
