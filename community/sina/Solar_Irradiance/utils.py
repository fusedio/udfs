# Load utility functions.
common_utils = fused.load(
    "https://github.com/fusedio/udfs/tree/5cfb808/public/common/"
).utils

# Pick particular functions that will be used in this UDF.
visualize = common_utils.visualize
read_tiff = common_utils.read_tiff