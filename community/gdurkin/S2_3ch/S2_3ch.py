@fused.udf
@fused.cache
def udf(west="-120.485537", south="34.879334",  east="-120.400163", north="34.951613",
    provider="AWS",
    time_of_interest="2021-07-15/2021-10-01",
    cloud_cover_perc="10",pixel_perc = "75",
    brightness="1.0",zoom="14"):
    north=float(north);south=float(south);east=float(east);west=float(west);zoom=float(zoom);brightness=float(brightness);cloud_cover_perc=float(cloud_cover_perc);pixel_perc=float(pixel_perc);
    import os
    import numpy as np
    import odc.stac
    import pystac_client
    import rasterio
    import requests
    from rasterio.plot import show
    from io import BytesIO
 
    from gabeutils import (
        channel_band_dict,
        color_dict_256,
        norm_dict,
        normalize,
        reverse_channel_band_dict,
        arr_to_discrete, 
    named_color_array
    )
    from PIL import Image
    bounds = [west, south, east, north]
    minx, miny, maxx, maxy = bounds 

    index_name_dict = {
        "burn": ["B08", "B12"],
        "glacier": ["B03", "B04"],
        "moisture": ["B08", "B11"],
        "snow": ["B03", "B11"],
        "veg": ["B08", "B04"],
        "water": ["B03", "B08"],
    }
    vis_stats_keys = []
    band_list = []
    raw_channels = []

    # overwrite default channel set, can be 1D or 3D
    # channels = ['water']
    #channels = ['B11','veg','snow']
    channels = ["glacier", "B08", "B11"]
    decoded_channels = []
    for b in channels:
        if b in reverse_channel_band_dict.keys():
            band_list += [b]
            raw_channels += [b]
            decoded_channels += [b]
        elif b in ["burn", "glacier", "moisture", "snow", "veg", "water"]:
            band_list += index_name_dict[b]
            c = "-".join(index_name_dict[b])
            vis_stats_keys += [c]
            decoded_channels += [c]

    band_list = list(set(band_list))
    s2_bands = [reverse_channel_band_dict[el] for el in band_list]

    from pystac.extensions.eo import EOExtension as eo

    if provider == "AWS":
        # odc.stac.configure_s3_access(requester_pays=True)

        catalog = pystac_client.Client.open("https://earth-search.aws.element84.com/v1")
    elif provider == "MSPC":
        import planetary_computer

        catalog = pystac_client.Client.open(
            "https://planetarycomputer.microsoft.com/api/stac/v1",
            modifier=planetary_computer.sign_inplace,
        )
    else:
        raise ValueError(
            f'{provider=} does not exist. Provider options are "AWS" and "MSPC"'
        )

    items = catalog.search(
        collections=["sentinel-2-l2a"],
        bbox=bounds,
        datetime=time_of_interest,
        query={"eo:cloud_cover": {"lt": cloud_cover_perc}},
    ).item_collection()
    # least_cloudy_item = min(items, key=lambda item: eo.ext(item).cloud_cover)
    # print(least_cloudy_item.assets.keys())
    
    resolution = int(5 * 2 ** (15 - zoom))
    ds = odc.stac.load(
        items,
        crs="EPSG:3857",
        bands=s2_bands,
        resolution=resolution,
        bbox=bounds,
    ).astype(float)

    ch_set = []
    ds_band_dict = {channel_band_dict[b]: ds[b] for b in s2_bands}
    for el in decoded_channels:
        stats_dict = norm_dict[el]

        if el in raw_channels:
            ch_set += [
                255.0
                * normalize(
                    ds_band_dict[el],
                    stats_dict["p99"],
                    0.5 * (stats_dict["p01"] + stats_dict["p02"]),
                    eps=0.0,
                    sqrt_=False,
                    clip_=True,
                )
            ]

        elif el in vis_stats_keys:
            el_pair = el.split("-")
            ch_set += [
                255.0
                * normalize(
                    np.array(
                        (ds_band_dict[el_pair[0]] - ds_band_dict[el_pair[1]])
                        / (
                            ds_band_dict[el_pair[0]]
                            + ds_band_dict[el_pair[1]]
                            + 1.0e-10
                        ),
                        "float",
                    ),
                    0.5 * (stats_dict["p99"] + stats_dict["p99"]),
                    0.5 * (stats_dict["p01"] + stats_dict["p02"]),
                    eps=0.0,
                    sqrt_=False,
                    clip_=True,
                )
            ]
    perc_arr = [pixel_perc] * 3
    ch_set[0][ch_set[0]==0] = np.nan
    ch_set[1][ch_set[1]==0] = np.nan
    ch_set[2][ch_set[2]==0] = np.nan
    Ch0_agg = np.nanpercentile(ch_set[0], perc_arr[0], axis=0)
    Ch1_agg = np.nanpercentile(ch_set[1], perc_arr[1], axis=0)
    Ch2_agg = np.nanpercentile(ch_set[2], perc_arr[2], axis=0)

    img_arr = np.stack([Ch0_agg, Ch1_agg, Ch2_agg])
    img_arr = np.array(img_arr, "float")
    arr3 = np.array(np.clip(brightness * img_arr, 0, 252), "uint8")
    arr1_x = arr3.shape[1]
    arr1_y = arr3.shape[2]

    return np.array(np.clip(brightness * arr3, 0, 252), "uint8"), bounds

    
        
