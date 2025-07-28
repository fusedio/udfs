# Load utility functions.
common = fused.load("https://github.com/fusedio/udfs/tree/5cfb808/public/common/").utils

# Pick particular functions that will be used in this UDF.
visualize = common.visualize
read_tiff = common.read_tiff