import fused

table_to_tile = fused.load(
    "https://github.com/fusedio/udfs/tree/eda5aec/public/common/"
).utils.table_to_tile
geom_stats = fused.load(
    "https://github.com/fusedio/udfs/tree/eda5aec/public/common/"
).utils.geom_stats
dsm_to_tile = fused.load(
    "https://github.com/fusedio/udfs/tree/eda5aec/public/DSM_JAXA_Example"
).utils.dsm_to_tile
