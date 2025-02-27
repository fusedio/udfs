@fused.udf
def udf(
    bounds: fused.types.Tile = None,
    time_of_interest="2020-09-01/2021-10-30",
    collection="sentinel-2-l2a",
):
    import fused
    import numpy as np
    import odc.stac
    import planetary_computer
    import pystac_client
    from pystac.extensions.eo import EOExtension as eo

    utils = fused.load(
        "https://github.com/fusedio/udfs/tree/f928ee1/public/common/"
    ).utils

    odc.stac.configure_s3_access(requester_pays=True)

    @fused.cache
    def load_data(bounds, time_of_interest):
        # Instantiate PC client
        catalog = pystac_client.Client.open(
            "https://planetarycomputer.microsoft.com/api/stac/v1",
            modifier=planetary_computer.sign_inplace,
        )
        # Search catalog
        items = catalog.search(
            collections=[collection],
            bbox=bounds.total_bounds,
            datetime=time_of_interest,
            query={"eo:cloud_cover": {"lt": 10}},
        ).item_collection()

        selected_item = min(items, key=lambda item: eo.ext(item).cloud_cover)
        max_key_length = len(max(selected_item.assets, key=len))

        # Render a natural color image of the AOI
        bands_of_interest = ["red", "green", "blue"]
        data = odc.stac.stac_load(
            [selected_item],
            bands=bands_of_interest,
            bbox=bounds.total_bounds,
        ).isel(time=0)
        return data

    # Run cached function
    data = load_data(bounds, time_of_interest)

    # Normalize each band between 0 and 256
    vmin_red, vmax_red = 0, 6644.0
    vmin_green, vmax_green = 0, 5768.0
    vmin_blue, vmax_blue = 0, 5216.0
    red_normalized = ((data["red"] - vmin_red) / (vmax_red - vmin_red)) * 256
    green_normalized = ((data["green"] - vmin_green) / (vmax_green - vmin_green)) * 256
    blue_normalized = ((data["blue"] - vmin_blue) / (vmax_blue - vmin_blue)) * 256

    # Stack the normalized bands to form an RGB image

    arr = np.stack([red_normalized, green_normalized, blue_normalized], axis=0).astype(
        np.uint8
    )

    return arr
