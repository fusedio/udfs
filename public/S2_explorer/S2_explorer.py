def udf(
    bbox,
    provider="AWS",
    channels=["B11", "veg", "snow"],
    time_of_interest="2023-05-01/2023-09-13",
    cloud_cover_perc=10,
    brightness=1.0,
    perc_arr=[50] * 3,
    palette="terrain_r",
):
    import os

    import numpy as np
    import odc.stac
    import pystac_client
    import rasterio
    from gabeutils import (
        arr_to_cmap,
        channel_band_dict,
        color_dict_256,
        norm_dict,
        normalize,
        reverse_channel_band_dict,
    )
    from PIL import Image

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
    # channels = ['B11','veg','snow']
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
    print("original channels:", channels)
    print("decoded channels:", decoded_channels)
    band_list = list(set(band_list))
    print("raw channels:", raw_channels)
    print("nd channels:", vis_stats_keys)
    s2_bands = [reverse_channel_band_dict[el] for el in band_list]
    print("all channels:", band_list)
    print("s2 bands used:", s2_bands)

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
        bbox=bbox.total_bounds,
        datetime=time_of_interest,
        query={"eo:cloud_cover": {"lt": cloud_cover_perc}},
    ).item_collection()
    # least_cloudy_item = min(items, key=lambda item: eo.ext(item).cloud_cover)
    # print(least_cloudy_item.assets.keys())
    if len(items) == 0:
        print(
            "no satellite imagery available for the current viewport and time period. Please explore other regions or timeframes."
        )
    print(f"Returned {len(items)} Items")
    resolution = int(5 * 2 ** (15 - bbox.z[0]))
    # print(f'{resolution=}')
    ds = odc.stac.load(
        items,
        crs="EPSG:3857",
        bands=s2_bands,
        resolution=resolution,
        bbox=bbox.total_bounds,
    ).astype(float)
    print(str(time_of_interest))

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
    # overwrite default percentiles
    perc_arr = [25] * 3
    if len(channels) == 3:
        Ch0_agg = np.percentile(ch_set[0], perc_arr[0], axis=0)
        Ch1_agg = np.percentile(ch_set[1], perc_arr[1], axis=0)
        Ch2_agg = np.percentile(ch_set[2], perc_arr[2], axis=0)

        print("max pixel:", np.max(Ch0_agg))
        print("min pixel:", np.min(Ch0_agg))
        img_arr = np.stack([Ch0_agg, Ch1_agg, Ch2_agg])
        img_arr = np.array(img_arr, "float")
        print("array shape", img_arr.shape)
        return np.array(np.clip(brightness * img_arr, 0, 252), "uint8")
    else:
        Ch0_agg = np.percentile(ch_set[0], perc_arr[0], axis=0)
        print("max pixel:", np.max(Ch0_agg))
        print("min pixel:", np.min(Ch0_agg))
        img_arr = np.array(Ch0_agg, "float")
        print("array shape", img_arr.shape)
        arr = np.array(np.clip(brightness * img_arr, 0, 255), "uint8")
        return arr_to_cmap(arr, cmap=palette, norm_range=[0, 220])
