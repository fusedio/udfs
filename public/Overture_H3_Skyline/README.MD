<!--fused:preview-->
<p align="center"><img src="https://fused-magic.s3.us-west-2.amazonaws.com/thumbnails/udfs-staging/nyc_skyline.png" width="600" alt="UDF preview image"></p>

<!--fused:readme-->
## Overview

This UDF uses the `get_overture` helper function to access the Overture dataset hosted in Source Coop. It calculates building metrics, including perimeter and area, and aggregates them by H3 cell. The H3 resolution dynamically adjusts with zoom level unless a specific h3_size is provided.
