<!--fused:preview-->
<p align="center"><img src="https://fused-magic.s3.us-west-2.amazonaws.com/thumbnails/udfs-staging/coffeeshops.png" width="600" alt="UDF preview image"></p>

<!--fused:readme-->
# Overview

## Visualizing Buildings by Their Proximity to Coffee Shops in Bushwick, Brooklyn

This UDF uses three other Fused UDFs:

* [Foursquare_Open_Source_Places](https://github.com/fusedio/udfs/tree/main/public/Foursquare_Open_Source_Places) to obtain coffee shop points.
* [Get_Isochrone](https://github.com/fusedio/udfs/tree/main/public/Get_Isochrone) to generate 5-minute walking distance isochrones from the coffee shop points.
* `get_overture` from [Overture_Maps_Example](https://github.com/fusedio/udfs/tree/main/public/Overture_Maps_Example) to join with buildings.

The UDF retrieves points, generates isochrones using `fused.utils.common.run_pool`, and converts them into H3 cells with `get_cells`, resulting in a `coffee_score`. The `coffee_score` reflects proximity and changes with H3 resolution, making it a score rather than a static count.

The function `get_iso_gdf` acts as a File UDF within a Tile UDF. A hardcoded `bbox` for the area surrounding Bushwick is used to generate the points. Isochrone H3 cells are overlaid with boundaries from Bushwick, sourced from NYC Open Data, to visualize the neighborhood.

To flatten the map, set `extruded` to `false` in the visualize tab. Darker buildings indicate a higher `coffee_score`.

Note: Adding steps to `time_steps` will not automatically render them in the visualization.

Isochrones can also be visualized independently by returning `gdf_iso`. To visualize the isochrones and render additional `time_steps` accurately, copy the visualize tab settings from [Get_Isochrone](https://github.com/fusedio/udfs/tree/main/public/Get_Isochrone) in the Workbench.
