<!--fused:preview-->
<p align="center"><img src="https://fused-magic.s3.us-west-2.amazonaws.com/thumbnails/udfs-staging/Sentinel_Tile_Example.png" width="600" alt="UDF preview image"></p>

<!--fused:tags-->
Tags: `10-meter` `aws` `pc` `bbox` `raster`

<!--fused:readme-->

### Overview [link](https://planetarycomputer.microsoft.com/dataset/sentinel-2-l2a)
The [Sentinel-2](https://sentinel.esa.int/web/sentinel/missions/sentinel-2) program provides global imagery in thirteen spectral bands at 10m-60m resolution and a revisit time of approximately five days. This dataset represents the global Sentinel-2 archive, from 2016 to the present, processed to L2A (bottom-of-atmosphere) using Sen2Cor and converted to cloud-optimized GeoTIFF format.

### External links
* [Reference](https://planetarycomputer.microsoft.com/dataset/sentinel-2-l2a)
* [AWS Earth Search STAC](https://earth-search.aws.element84.com/v1/collections/sentinel-2-l2a)
* [Microsoft Planetary Computer STAC](https://planetarycomputer.microsoft.com/api/stac/v1/collections/sentinel-2-l2a)

## Run this in any Jupyter Notebook

```python
import fused

udf = fused.load("https://github.com/fusedio/udfs/tree/main/public/Sentinel_Tile_Example")
arr = fused.run(udf, x=5241, y=12667, z=15)
arr
```