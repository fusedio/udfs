<!--fused:pin=11-->
<!--fused:preview-->
<p align="center"><img src="https://fused-magic.s3.us-west-2.amazonaws.com/thumbnails/udfs-staging/raster_to_h3.png" width="600" alt="UDF preview image"></p>

<!--fused:tags-->
Tags: `raster` `h3`

<!--fused:readme-->
## Overview

Raster to H3 Tile Converter

Converts raster data to H3 hexagonal tiles. It processes a section of a GeoTIFF file and aggregates the data into H3 cells.

The function returns a pandas DataFrame with the following columns:
- hex: H3 cell ID (as a hexadecimal string)
- agg_data: Sum of data values in the cell
- metric: Same as agg_data (for compatibility)
