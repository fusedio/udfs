<!--fused:preview-->
<p align="center"><img src="https://fused-magic.s3.us-west-2.amazonaws.com/thumbnails/udfs-staging/overture_gers_lookup.png" width="600" alt="UDF preview image"></p>

<!--fused:readme-->
## Overture Buildings GERS Lookup

This UDF  accepts an Overture Buildings GERS defined by the `gers_id` parameter and returns the corresponding Building  polygon, id, and attributes. Because the first 16 digits of GERS correspond to an H3 cell, we can use the ID to create a polygon to spatially filter the dataset. 

Then, `get_overture` loads any buildings that intersect the H3 cell and the building that matches that ID is selected.

