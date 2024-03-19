import fused
import numpy as np

read_tiff = fused.load(
    "https://github.com/fusedio/udfs/tree/f928ee1/public/common/"
).utils.read_tiff
