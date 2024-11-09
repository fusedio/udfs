<!--fused:preview-->
<p align="center"><img src="./UDF_preview.jpg" width="600" alt="UDF preview image"></p>

<!--fused:tags-->
Tags: `file` `wireless` `tutorial` `h3` `duckdb` `Ibis`

<!--fused:readme-->
## Overview

This UDF allows you to explore the outputs of a coverage model with different site counts. The visualizations are done using [Ibis](https://ibis-project.org/) with a DuckDB backend. The h3 resolution changes based on zoom level.

## External links

- Network coverage model by [Digital Twin Sim](https://www.digitaltwinsim.com/)

## Run this in any Jupyter Notebook

```python
import fused

udf = fused.load("https://github.com/fusedio/udfs/tree/main/public/Coverage_model_ibis")
gdf = fused.run(udf=udf)
gdf
```
