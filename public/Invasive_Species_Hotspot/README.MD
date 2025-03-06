<!--fused:preview-->
<p align="center"><img src="https://fused-magic.s3.us-west-2.amazonaws.com/blog-assets/liz2.png" width="600" alt="UDF preview image"></p>

<!--fused:tags-->
Tags: `overture` `coop`

<!--fused:readme-->
# Invasive Species Hotspot

Map key hotspots where Arundo donax is likely to spread based on built-environment factors derived from Overture Maps data.

This UDF runs a simple model to identify hotspots susceptible to Arundo. The model uses a weighted sum of several base Overture data classes:

- Golf Courses
- Bridges
- Water bodies (rivers, streams, etc.)

The UDF performs the following steps:

1. Create GeoDataFrames from the Overture maps dataset using get_overture
2. Generate an H3 score based on buffers around each feature
3. Aggregate the H3 scores to create a weighted sum

