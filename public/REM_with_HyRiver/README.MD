<!--fused:preview-->
<p align="center"><img src="https://fused-magic.s3.us-west-2.amazonaws.com/thumbnails/udfs-staging/REM_with_HyRiver.png" width="600" alt="UDF preview image"></p>

<!--fused:tags-->
Tags: `dem` `10-meter` `tutorial` `river`

<!--fused:readme-->
## Overview

Relative Elevation Model (REM) detrends a DEM based on the water surface of a stream. It’s especially useful for visualization of floodplains. This UDF computes REM for a segment of Carson River and visualizes the results using `xarray-spatial` and `datashader`.

We use HyRiver software stack to download the required Digital Elevation Model (DEM) and river flowlines within a bounding box.

## External links

- [HyRiver](https://docs.hyriver.io/)
- [REM](https://wadnr.maps.arcgis.com/apps/Cascade/index.html?appid=36b4887370d141fcbb35392f996c82d9)