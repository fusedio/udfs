<!--fused:preview-->
<p align="center"><img src="https://fused-magic.s3.us-west-2.amazonaws.com/blog-assets/zstats_viewer_mx.png" width="600" alt="UDF preview image"></p>

<!--fused:readme-->
# Zonal Stats Forest Observations - viewer

This UDF visualizes the final output of a zonal statistics workflow for forest cover for municipal areas around the world. This table shows aggregates of a raster of global forest cover across zones defined by a vector table of municipalities. The output table contains records for every municipality and columns with summary metrics such as percent forest coverage.

This UDF is called "viewer" because it clips the table of outputs it to fit within the bounds of the tile as a tile UDF and renders it as a choroplet map. The maps shows differences in color intensity, from 0 to 1, within municipality polygons to indicate the average values of forest cover.
