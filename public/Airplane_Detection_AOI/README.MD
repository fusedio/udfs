<!--fused:preview-->
<p align="center"><img src="https://fused-magic.s3.us-west-2.amazonaws.com/thumbnails/udfs-staging/dl4eo_aoi.png" width="600" alt="UDF preview image"></p>

<!--fused:readme-->
# Airplane Detection

This UDF detects airplanes within a specified Area of Interest (AOI) by leveraging a deep learning model for object detection.

## How It Works

1. Tile Generation: The UDF divides the AOI into smaller tiles based on a given zoom level.
2. Model Inference: It applies the publicly available UDF, [DL4EO_Airplane_Detection](https://www.fused.io/workbench/catalog/DL4EO_Airplane_Detection-d0d1cee6-f7fe-4f26-9a84-a7cb130d20cb), to each tile.
3. Output: The result is a GeoDataFrame containing bounding box records for each detected airplane.


The development of this UDF was led by [Jeff Faudi](https://www.linkedin.com/in/faudi/) @ DL4EO. For a detailed overview of the methods and applications, refer to Jeff's [blog post](https://docs.fused.io/blog/ai-for-object-detection-on-50cm-imagery/).


