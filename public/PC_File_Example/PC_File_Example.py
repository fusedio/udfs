# Ref: https://github.com/microsoft/PlanetaryComputerExamples/blob/main/tutorials/surface_analysis.ipynb
@fused.udf
def udf():
    import numba

    numba.config.CACHE_DIR = "/tmp/numba_cache"

    import stackstac
    import planetary_computer
    import pystac_client
    import stackstac
    import rasterio
    from datashader import Canvas
    from datashader.colors import Elevation
    from datashader.transfer_functions import shade
    from datashader.transfer_functions import stack
    from xrspatial.utils import height_implied_by_aspect_ratio
    from xrspatial import hillshade

    catalog = pystac_client.Client.open(
        "https://planetarycomputer.microsoft.com/api/stac/v1/",
        modifier=planetary_computer.sign_inplace,
    )

    point = {"type": "Point", "coordinates": [-85.990693, 35.718427]}
    search = catalog.search(collections=["nasadem"], intersects=point, limit=5)
    nasadem_item = next(search.items())
    elevation_raster = stackstac.stack(
        [nasadem_item.to_dict()],
        epsg=4326,
        resampling=rasterio.enums.Resampling.bilinear,
        chunksize=2048,
    ).squeeze()

    # get full extent of raster
    full_extent = (
        elevation_raster.coords["x"].min().item(),
        elevation_raster.coords["y"].min().item(),
        elevation_raster.coords["x"].max().item(),
        elevation_raster.coords["y"].max().item(),
    )
    # get ranges
    x_range = (full_extent[0], full_extent[2])
    y_range = (full_extent[1], full_extent[3])

    # set width and height
    W = 300
    H = height_implied_by_aspect_ratio(W, x_range, y_range)

    cvs = Canvas(plot_width=W, plot_height=H, x_range=x_range, y_range=y_range)

    elevation_small = cvs.raster(
        elevation_raster,
    )[::-1, :]

    elevation_img = shade(elevation_small, cmap=Elevation, how="linear")

    hillshade_raster = hillshade(elevation_raster)
    hillshade_img = shade(
        cvs.raster(hillshade_raster),
        cmap=["#333333", "#C7C7C7"],
        alpha=200,
    )
    terrain_img = shade(elevation_small, cmap=Elevation, alpha=100, how="linear")
    da_final = stack(hillshade_img, terrain_img)
    return da_final
