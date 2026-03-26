We can ran the monthly aggregate UDF in parallel with `udf.map()`

This will spin up new UDF instances in parallel to compute the 12 months x 5 years = 60 months all at once.

Read more about [parallel processing](https://docs.fused.io/guide/working-with-udfs/udf-best-practices/scaling-out) in Fused