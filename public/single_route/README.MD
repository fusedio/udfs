<!--fused:preview-->
<p align="center"><img src="https://fused-magic.s3.us-west-2.amazonaws.com/thumbnails/udfs-staging/route_lax.png" width="600" alt="UDF preview image"></p>

<!--fused:tags-->
Tags: `routing`

<!--fused:readme-->
## Overview

This UDF returns a single optimal (time minimized) route as a function of origin and destination coordinate pairs (input variables), using the Valhalla routing engine API. The geopandas dataframe returned contains three rows: start and end point geometries and the linestring geometry of the optimal path. (Simple driving directions are also printed.)

## External links

- [Valhalla GitHub Docs](https://valhalla.github.io/valhalla/)
