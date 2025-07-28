import fused

common = fused.load("https://github.com/fusedio/udfs/tree/d0e8eb0/public/common/").utils
table_to_tile = common.table_to_tile
geom_stats = common.geom_stats
dsm_to_tile = fused.load(
    "https://github.com/fusedio/udfs/tree/91845c4/public/DSM_JAXA_Example"
).utils.dsm_to_tile
