<!--fused:preview-->
<p align="center"><img src="https://fused-magic.s3.amazonaws.com/thumbnails/preview/fusedio/udfs/LULC_Impact_Observatory/8bb119d7-9fb1-4675-bbed-6ecc70b2a003" width="600" alt="UDF preview image"></p>

<!--fused:readme-->
## UDF: Filter LULC 10m Classes

This UDF filters a provided 10m land cover TIFF to include only user-specified LULC classes.

**Inputs:**
- **bounds**: Tile bounds to read.
- **lulc_type**: String with one or multiple land class names (comma-separated) to filter.

**Behavior:**
- Reads the input TIFF within the requested bounds.
- Converts `lulc_type` terms into class codes.
- Masks the array to include only pixels matching the selected classes.

**Outputs:**
- A filtered TIFF or array containing only the specified land cover classes.
