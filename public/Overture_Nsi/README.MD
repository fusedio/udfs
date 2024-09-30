<!--fused:preview-->
<p align="center"><img src="https://fused-magic.s3.us-west-2.amazonaws.com/thumbnails/udfs-staging/overture_nsi2.png" width="600" alt="UDF preview image"></p>

<!--fused:readme-->
## Overview

This UDF uses the `get_overture` helper function to read from the Overture dataset hosted in Source Coop. It then queries the NSI API to load NSI structures as a feature collection of points.

The UDF then performs spatial join between Overture Buildings and NSI using GeoPandas with `gdf_overture.sjoin(gdf)`. This operation returns a unified table with Overture building footprints enriched with NSI attributes. Any duplicates, where multiple NSI points fall within the same building footprint, can be dropped. The resulting dataset will have NSI attributes enriched with GERS IDs and the building polygons.

Read more in the associated [blog post](https://docs.fused.io/blog/overture-tiles/).
