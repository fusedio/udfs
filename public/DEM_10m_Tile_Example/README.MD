<!--fused:preview-->
<p align="center"><img src="https://fused-magic.s3.us-west-2.amazonaws.com/thumbnails/udfs-staging/DEM_10m_Tile_Example.png" width="600" alt="UDF preview image"></p>

<!--fused:tags-->
Tags: `band-algebra` `stac` `msft-pc`

<!--fused:readme-->
#### Overview
U.S.-wide digital elevation data at horizontal resolutions ranging from one to sixty meters.

The USGS 3D Elevation Program (3DEP) Datasets from the National Map are the primary elevation data product produced and distributed by the USGS. The 3DEP program provides raster elevation data for the conterminous United States, Alaska, Hawaii, and the island territories, at a variety of spatial resolutions. The seamless DEM layers produced by the 3DEP program are updated frequently to integrate newly available, improved elevation source data.

DEM layers are available nationally at grid spacings of 1 arc-second (approximately 30 meters) for the conterminous United States, and at approximately 1, 3, and 9 meters for parts of the United States. Most seamless DEM data for Alaska is available at a resolution of approximately 60 meters, where only lower resolution source data exist.
#### STAC Collections
* [Microsoft Planetary Computer](https://planetarycomputer.microsoft.com/api/stac/v1/collections/3dep-seamless)


## Run this in any Jupyter Notebook

```python
import fused

udf = fused.load("https://github.com/fusedio/udfs/tree/main/public/DEM_10m_Tile_Example")
arr = fused.run(udf=udf, x=2411, y=3079, z=13)
arr
```
