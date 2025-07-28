def udf(
    bounds: fused.types.Bounds = [-88.841,16.129,-87.490,17.508],
    year: int = 2023,
    variable: str = "evi2",
):
    import odc.stac
    import palettable
    import stacrs
    from pystac import Item

    # Load pinned versions of utility functions.
    common = fused.load("https://github.com/fusedio/udfs/tree/bb712a5/public/common/").utils
    zoom = common.estimate_zoom(bounds)

    # Load utility functions
    visualize = fused.load(
        "https://github.com/fusedio/udfs/tree/5cfb808/public/common/"
    ).common.visualize

    # Author: Alex Leith, with inspiration from other examples

    # Find Items that intersect the bounding box and time period
    # year can be passed in as a parameter for the function
    item_dicts = stacrs.search(
        "https://data.ldn.auspatious.com/geo_ls_lp/geo_ls_lp_0_1_0.parquet",
        bbox=bounds,
        datetime=f"{year}-01-01T00:00:00.000Z/{year}-12-31T23:59:59.999Z",
    )

    if not item_dicts:
        return None

    # Convert the dictionaries to PySTAC Items
    items = [Item.from_dict(d) for d in item_dicts]

    # Calculate the resolution based on zoom level
    power = 13 - zoom
    if power < 0:
        resolution = 30
    else:
        resolution = int(20 * 2**power)

    # Load the data into an Xarray dataset, removing the time dimension with squeeze()
    # variable is dynamic, and is also passed in as a parameter for the function
    data = odc.stac.load(
        items,
        crs="EPSG:3857",
        bands=[variable],
        resolution=resolution,
        bbox=bounds,
    ).squeeze()

    # Create a mask where data is nan
    mask = (~data[variable].isnull()).squeeze().to_numpy()

    # Visualize that data as an RGB image.
    rgb_image = visualize(
        data=data[variable],
        mask=mask,
        min=0,
        max=360,
        colormap=palettable.matplotlib.Viridis_20,
    )

    return rgb_image