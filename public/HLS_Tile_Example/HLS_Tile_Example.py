@fused.udf
def udf(
    bounds: fused.types.Bounds,
    collection_id="HLSS30_2.0",  # Landsat:'HLSL30_2.0' & Sentinel:'HLSS30_2.0'
    band="B8A",  # Landsat:'B05' & Sentinel:'B8A'
    date_range="2023-10/2024-01",
    max_cloud_cover=25,
    n_mosaic=5,
    min_max=(0, 3000),
):
    import json
    import numpy as np
    import palettable
    from pystac_client import Client
    from utils_local import list_stac_collections

    # convert bounds to tile
    utils = fused.load("https://github.com/fusedio/udfs/tree/bb712a5/public/common/").utils
    zoom = utils.estimate_zoom(bounds)
    tile = utils.get_tiles(bounds, zoom=zoom)

    STAC_URL = "https://cmr.earthdata.nasa.gov/stac"

    try:
        creds_json = fused.secrets.earthdata
        creds_dict = json.loads(creds_json)
        username = creds_dict["username"]
        password = creds_dict["password"]
        env = creds_dict.get("env", "earthdata")
    except:
        print(
            "Unable to authenticate to NASA Earthdata.\n"
            "Please update the `earthdata` secret.\n"
            "Docs: https://docs.fused.io/workbench/account/#secrets-management\n"
            "It should have content that looks like:\n"
            "\n"
            '{"username":"my_username", "password":"my_password"}\n'
            "\n"
            "To register and manage your Earthdata Login account,\n"
            "go to: https://urs.earthdata.nasa.gov "
        )
        return None

    z = zoom
    if z >= 9:

        catalog = Client.open(f"{STAC_URL}/LPCLOUD/")

        # Uncomment the following line to see a list of collections.
        # utils.list_stac_collections(catalog)

        # Check authentication.
        cred = {"env": env, "username": username, "password": password}
        aws_session = utils.earth_session(cred=cred)

        search = catalog.search(
            collections=[collection_id],
            bbox=tile.total_bounds,
            datetime=date_range,
            limit=100,
        )
        item_collection = search.get_all_items()
        band_urls = []
        for i in item_collection:
            if (
                i.properties["eo:cloud_cover"] <= max_cloud_cover
                and i.collection_id == collection_id
                and band in i.assets
            ):
                url = i.assets[band].href
                url = url.replace(
                    "https://data.lpdaac.earthdatacloud.nasa.gov/", "s3://"
                )
                band_urls.append(url)
        if len(band_urls) == 0:
            try:
                collection = catalog.get_collection(collection_id)
                print(f"Collection '{collection_id}' exists")
                print(
                    "No items were found. Please check your `band` and/or widen your `date_range`."
                )
            except Exception:
                print(f"Collection '{collection_id}' was not found in the catalog.")
            return None

        arr = utils.mosaic_tiff(
            tile,
            band_urls[:n_mosaic],
            reduce_function=lambda x: np.max(x, axis=0),
            overview_level=max(0, 12 - z),
            cred=cred,
        )
        # Visualize data as an RGB image.
        rgb_image = utils.visualize(
            data=arr,
            min=min_max[0],
            max=min_max[1],
            colormap=palettable.scientific.sequential.Oslo_20,
        )
        return rgb_image
    elif z >= 8:
        print("Almost there! Please zoom in a bit more. 😀")
    else:
        print("Please zoom in more.")
