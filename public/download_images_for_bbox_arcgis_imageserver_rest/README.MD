<!--fused:readme-->
# Download Images from ArcGIS ImageServer for Bounding Box

This Fused UDF downloads images from an ArcGIS ImageServer REST service that intersect with a given geographical bounding box, preserving their original resolution and CRS. This is useful for downloading large areas while maintaining control over image pre-processing.

## Description

The UDF accepts a bounding box (as a Fused `Bounds` object or GeoPackage file) and downloads intersecting images in parallel to a specified directory. The default example downloads images from GIS STMK's [REST interface](https://gis.stmk.gv.at/image/rest/services/OGD_DOP) for a small area in Austria covering a national park.

## Hint
Also checkout the `create_cogs_from_tiffs_in_folder` UDF to create COGs from the downloaded images.
