<!--fused:preview-->
<p align="center"><img src="https://fused-magic.s3.us-west-2.amazonaws.com/thumbnails/dl4eo_cpu.png" width="600" alt="UDF preview image"></p>

<!--fused:tags-->
Tags: `object-detection`

<!--fused:readme-->
## Overview

This UDF runs an object detection model on 50cm imagery. 

The workflow uses the `ultralytics` library to run a `YOLO` model on an RGB tile of a satellite image and returns a GeoDataFrame with bounding boxes for the predicted objects.

You can find the image in the [Public ArcGIS RGB UDF](https://www.fused.io/workbench/catalog/Arcgis_Rgb-b58b7a3c-3dc0-4e9d-9bdd-7092f3ea9817). For best visualization effects, render the output of that UDF as layer underneath.

The development of this UDF was led by [Jeff Faudi](https://www.linkedin.com/in/faudi/) @ DL4EO. For a detailed overview of the methods and applications, refer to Jeff's [blog post](https://docs.fused.io/blog/ai-for-object-detection-on-50cm-imagery/).


