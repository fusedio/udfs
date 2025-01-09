<!--fused:preview-->
<p align="center"><img src="https://fused-magic.s3.us-west-2.amazonaws.com/thumbnails/udfs-staging/cdmx.png" width="600" alt="UDF preview image"></p>

<!--fused:readme-->
## Cluster Embeddings of Overture Places by H3

This UDF groups business categories in Overture Places at the H3 grid level, we can analyze the distribution of services and their accessibility. This approach provides a quantifiable way to compare cities globally, shedding light on the urban inclusivity of different regions.
Workflow

The UDF follows this workflow:

1. Load Overture Places records for a given H3
2. Create business category column
3. Create an embedding to represent business categories
4. Run UDF for all H3 spanning an area of interest (polyfill)
5. Calculate Kmeans clustering based on cosine similarity.
6. Describe each cluster
