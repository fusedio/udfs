<!--fused:preview-->
<p align="center"><img src="https://fused-magic.s3.us-west-2.amazonaws.com/thumbnails/udfs-staging/Compute_TWI.png" width="600" alt="UDF preview image"></p>

<!--fused:tags-->
Tags: `whitebox`

<!--fused:readme-->
#### Overview
Apply [WhitboxTools](https://www.whiteboxgeo.com/manual/wbt_book/) workflows across the US using the USGS 3D Elevation Program (3DEP) Datasets from the National Map at 30-m resolution as the source for elevation data. WhitboxTools provides a wide range of functionalities for geospatial analysis. This UDF relies on [PyWBT](https://pywbt.readthedocs.io) to run the WhitboxTools.

## Run this in any Jupyter Notebook

```python
import fused

udf = fused.load("https://github.com/fusedio/udfs/tree/main/public/Compute_TWI")
arr = fused.run(udf=udf, x=2411, y=3079, z=13)
arr
```
