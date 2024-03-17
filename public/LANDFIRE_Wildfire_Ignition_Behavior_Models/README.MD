https://landfire.gov/version_download.php#

<!--fused:preview-->
<p align="center"><img src="https://fused-magic.s3.us-west-2.amazonaws.com/thumbnails/udfs-staging/landfire-fused.png" width="600" alt="UDF preview image"></p>

<!--fused:tags-->
Tags: `dem` `elevation` `raster` `10-meter`

<!--fused:readme-->

Exported from Fused UDF Workbench

#### Overview
LANDFIRE provides a dataset across the United States that provides detailed data useful for wildfire analysis and prevention. Here is a more detailed description from [their website](https://landfire.gov/data_overviews.php):

"LF delivers vegetation, fuel, disturbance, and fire regimes geospatial data products for the entire nation. Methods are based on peer-reviewed science from multiple fields. LF products are consistent, comprehensive, and standardized, resulting in multiple applications to fire, fuel, and natural resources. LF has 20+ products and many versions of these products. The descriptions allow for ease of comparison between products. The Version Table shows data availability and links to data mosaics."

These raster files are provided at 30 meter resolution, but are quite large to work with if you are only interested in a small area. This UDF allows you to provide a lat/long pair and radius, and pull the data for that area into a GeoDataFrame. This specifically calls out to a COG with the LANDFIRE's (LF) 13 Anderson Fire Behavior Fuel Model dataset ([details here](https://landfire.gov/fbfm13.php)).

## Run this in any Jupyter Notebook

```python
import fused

udf = fused.load("https://github.com/fusedio/udfs/tree/main/public/LANDFIRE_Wildfire_Ignition_Behavior_Models")
gdf = fused.run(udf=udf, lat=35.6679783, lng=-101.4011159, radius=1609)
gdf
```
