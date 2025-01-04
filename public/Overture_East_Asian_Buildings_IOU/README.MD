<!--fused:preview-->
<p align="center"><img src="https://fused-magic.s3.us-west-2.amazonaws.com/thumbnails/udfs-staging/overture_iou.png" width="600" alt="UDF preview image"></p>

<!--fused:readme-->
# IOU Between Overture and East Asian Buildings

Calculates the Intersection over Union (IOU) ratio between Overture Buildings and the East Asian Buildings dataset (https://zenodo.org/records/8174931) by leveraging hex-based discretization of geometries. This UDF is compares the ratio between overlapping and non-overlapping regions of buildings and can be used to quantify overlap between buildings in each dataset, detect discrepancies, or evaluating changes.

- Blue is the `intersection`
- Red is the `symmetric_difference`
- Height, for each Overture Building, is based on the intersection over union score
