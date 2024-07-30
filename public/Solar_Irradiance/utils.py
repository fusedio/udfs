import fused
import numpy as np

read_tiff = fused.load(
    "https://github.com/fusedio/udfs/tree/3c4bc47/public/common/"
).utils.read_tiff
