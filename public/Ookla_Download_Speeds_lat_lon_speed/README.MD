<!--fused:preview-->
<p align="center"><img src="https://fused-magic.s3.us-west-2.amazonaws.com/thumbnails/udfs-staging/milind.png" width="600" alt="UDF preview image"></p>

<!--fused:tags-->
Tags: `aws` `broadband` `hex` `collec-vancouver-open-data`

<!--fused:readme-->
## Overview

This example demonstrates how Ookla's mobile performance data can be dynamically processed into an H3 hexagonal grid system. The network metrics are aggregated (averaging download speeds) for H3 hexes at a resolution that adapts based on the zoom level. The performance data comes from Ookla's global speed test infrastructure, capturing real-world mobile network performance across diverse network operators and technologies. The data is stored in Parquet format on S3, structured by year and quarter, allowing for efficient geographic querying and temporal analysis. The resulting hexagonal grid provides a standardized way to visualize and analyze mobile network performance patterns across different geographic scales and regions.

##External Links

- [Ookla Open Dataset on AWS Marketplace](https://aws.amazon.com/marketplace/pp/prodview-breawk6ljkovm)
