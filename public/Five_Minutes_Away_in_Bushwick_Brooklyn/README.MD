<!--fused:preview-->
<p align="center"><img src="https://fused-magic.s3.us-west-2.amazonaws.com/thumbnails/udfs-staging/coffeeshops.png" width="600" alt="UDF preview image"></p>

<!--fused:readme-->
# Overview

## Visualizing Buildings by Their Proximity to Differnent Types of Places in Bushwick, Brooklyn

This UDF uses three other Fused UDFs:

* [Foursquare_Open_Source_Places](https://github.com/fusedio/udfs/tree/main/public/Foursquare_Open_Source_Places) to obtain FSQ POI points.
* [Get_Isochrone](https://github.com/fusedio/udfs/tree/main/public/Get_Isochrone) to generate 5-minute walking distance isochrones from the FSQ POI points.
* `get_overture` from [Overture_Maps_Example](https://github.com/fusedio/udfs/tree/main/public/Overture_Maps_Example) to join with buildings.

The UDF retrieves points of interest (POIs), generates isochrones for these points using the fused.utils.common.run_pool function, and converts the resulting isochrones into H3 cells using the get_cells function. This process results in two outputs: a list of `poi_names` and a corresponding `poi_density`. Both `poi_density` and `poi_names` vary depending on the H3 resolution used.

The function `get_fsq_isochrones_gdf` acts as a File UDF within a Tile UDF. A hardcoded `bbox` for the area surrounding Bushwick is used to generate the points. Isochrone H3 cells are overlaid with boundaries from Bushwick, sourced from NYC Open Data, to visualize the neighborhood.

To flatten the map, set `extruded` to `false` in the visualize tab. Darker buildings indicate a higher `poi_density`.

The `poi_category` can be changed in the parameters section of the UDFs tab.

Note: Adding steps to `time_steps` will not automatically render them in the visualization.

Isochrones can also be visualized independently by returning `gdf_fsq_isochrones`. To visualize the isochrones and render additional `time_steps` accurately, copy the visualize tab settings from [Get_Isochrone](https://github.com/fusedio/udfs/tree/main/public/Get_Isochrone) in the Workbench.

[Four Square POI Docs](https://docs.foursquare.com/data-products/docs/places-data-overview)