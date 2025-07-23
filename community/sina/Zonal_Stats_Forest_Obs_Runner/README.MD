<!--fused:preview-->
<p align="center"><img src="https://fused-magic.s3.us-west-2.amazonaws.com/thumbnails/udfs-staging/forobs.png" width="600" alt="UDF preview image"></p>

<!--fused:readme-->
# Zonal Stats Forest Observations - runner

This UDF calculates zonal statistics for forest cover for municipal areas around the world. The workflow aggregates a raster of global forest cover across zones defined by a vector table of municipalities. The final result is a table with records for every municipality and columns with summary metrics such as percent forest coverage. This type of analysis can help identify regions with significant forest loss to prioritize conservation efforts or support sustainable land-use planning. 

This UDF is called "runner" because it loads raster and municipal data from a for the area defined by a predefined cell id, performs zonal statistics, and writes the result to a Parquet file stored on S3. This approach promotes scalability as the UDF can run concurrently for multiple grid cells. The resulting Parquet files can be structured to be read as a single table for downstream analysis.

## Datasets

🌲 [JRC Global map of Forest Cover for year 2020](https://forest-observatory.ec.europa.eu/forest/gfc2020) (forest cover raster)
🇪🇨 [Geo Boundaries ADM2](https://github.com/wmgeolab/geoBoundaries/) (municipality polygons)

The UDF returns a `GeoDataFrame` with polygons corresponding to municipalities that fall within the rectangular area defined by a given cell bounds. Each polygon is colored green with an opacity that corresponds to the calculated tree cover percentage. 

The output parameters to keep in mind are:
- `shapeID` is the unique identifier for the municipality polygon
- `tiff_url` is the S3 part of the GeoTIFF file the cell falls within
- `stats_sum` is the number of pixels within the polygon with a value of 1, indicating presence of forest
- `stats_count` is the total number of pixels within the polygon
- `stats_mean` is the ratio of forest pixels over the total `stats_sum / stats_count`

For each municipality polygon, defined by `shapeID`, `stats_count` is the total number of pixels and `stats_sum` the total number of forest pixels. The ratio between the two values gives the percentage of tree cover for each municipality. This effectively calculates statistical summaries for a zone, givin the analysis its name.
