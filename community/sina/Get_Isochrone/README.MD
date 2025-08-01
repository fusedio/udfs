<!--fused:pin=10-->
<!--fused:preview-->
<p align="center"><img src="https://fused-magic.s3.us-west-2.amazonaws.com/thumbnails/udfs-staging/Get_Isochrone.png" width="600" alt="UDF preview image"></p>

<!--fused:tags-->
Tags: `isochrone` `valhalla`

<!--fused:readme-->
# Overview

This UDF is using Valhalla API to create an isochrone. Valhalla, an open-source routing engine, and its accompanying libraries are designed for OpenStreetMap data, offering features such as time+distance matrix computation, isochrones, elevation sampling, map matching, and tour optimization (Travelling Salesman).

# External links

- [Valhalla Github repo](https://github.com/valhalla/valhalla)

## Run this in any Jupyter Notebook

```python
import fused

udf = fused.load("https://github.com/fusedio/udfs/tree/main/public/Get_Isochrone")
gdf = fused.run(udf=udf)
gdf
```
