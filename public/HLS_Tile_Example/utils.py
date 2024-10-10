# Load utility functions.
visualize = fused.load(
    "https://github.com/fusedio/udfs/tree/255e0fe/public/common_vis/"
).utils.visualize

earth_session= fused.load(
    "https://github.com/fusedio/udfs/tree/f928ee1/public/common/"
).utils.earth_session

mosaic_tiff= fused.load(
    "https://github.com/fusedio/udfs/tree/f928ee1/public/common/"
).utils.mosaic_tiff

def list_stac_collections(catalog):
    names = [x.id for x in catalog.get_collections()]
    for x in names: print(x)