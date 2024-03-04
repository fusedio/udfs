<<<<<<< HEAD
<!--fused:preview-->
<p align="center"><img src="https://fused-magic.s3.us-west-2.amazonaws.com/thumbnails/udfs-staging/s3_explorer_udf_2.png" width="600" alt="UDF preview image"></p>

<!--fused:readme-->
## Overview

Sentinel 2 satellite imaging pseudo RGB bands composed from the original 13 Sentinel 2 bands. The defaults are:

* `pseudoRed = Glacier Index = (B3 – B4) / (B3 + B4)`
* `pseudoGreen = B8 (Near InfraRed)`
* `pseudoBlue = B11 (Short Wave InfraRed)`

## Run this in any Jupyter Notebook

```python
import fused

udf = fused.load("https://github.com/fusedio/udfs/tree/main/public/S2_explorer")
df = fused.run(udf, x=1270, y=3048, z=13)
df.image.isel(band=0).plot()
```
=======
<!--fused:readme-->
Exported from Fused UDF Workbench

>>>>>>> 801216c (i added the s2 index example)
