<!--fused:preview-->
<p align="center"><img src="https://fused-magic.s3.us-west-2.amazonaws.com/thumbnails/udfs-staging/ibis_overture.png" width="600" alt="UDF preview image"></p>

<!--fused:readme-->
# Ibis DuckDB Overturemaps

Thanks to the support for DuckDB geospatial operations in Ibis, we can query the datasets from Overture Maps using Python with the performance of DuckDB without having to write SQL.

Using Ibis with DuckDB as the backend, this UDF reads the Overture Maps data directly from the source Overture S3 bucket. These queries will only fetch the attributes and rows that meet specified conditions, minimizing how much data is processed.
