<!--fused:readme-->
## Overview

This UDF takes H3 cells from [DuckDB_H3_Example](https://github.com/fusedio/udfs/tree/main/public/DuckDB_H3_Example) which aggregates cab pickups from January 2010 and joines them with [Overture_Maps_Example](https://github.com/fusedio/udfs/tree/main/public/Overture_Maps_Example) so it is possible to see the corresponding buildings.

The point of the UDF is to demosntrate that you can use the H3 index to join a lot of point data with sources like Overture Maps.
