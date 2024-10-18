import odc.stac
import pystac_client
from pystac.extensions.eo import EOExtension as eo

visualize = fused.load(
    "https://github.com/fusedio/udfs/tree/e1c15b5/public/common/"
).utils.visualize

@fused.cache
def get_dem(bbox):
    collection = 'cop-dem-glo-90'
    # Reduce the resolution to get a quicker, but less accurate, results.
    resolution = 90*2

    odc.stac.configure_s3_access(aws_unsigned=True)
    catalog = pystac_client.Client.open("https://earth-search.aws.element84.com/v1")

    items = catalog.search(
        collections=[collection],
        bbox=bbox.total_bounds,
    ).item_collection()
    
    ds = odc.stac.load(
        items,
        crs="EPSG:3857",
        bands=["data"],
        resolution=resolution,
        bbox=bbox.total_bounds,
    ).astype(float)
    xr_data = ds["data"].max(dim="time")
    return xr_data