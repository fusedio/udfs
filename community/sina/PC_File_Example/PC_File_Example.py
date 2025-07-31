# Ref: https://github.com/microsoft/PlanetaryComputerExamples/blob/main/tutorials/surface_analysis.ipynb
@fused.udf
def udf(bounds: fused.types.Bounds = [-86.23623988112621,35.11513233659201,-84.76403790099684,35.886387427376675]):
    import numba

    numba.config.CACHE_DIR = "/tmp/numba_cache"

    import planetary_computer
    import pystac_client
    import rasterio
    import stackstac
    from datashader import Canvas
    from datashader.colors import Elevation
    from datashader.transfer_functions import shade, stack
    from xrspatial import hillshade
    from xrspatial.utils import height_implied_by_aspect_ratio

    catalog = pystac_client.Client.open(
        "https://planetarycomputer.microsoft.com/api/stac/v1/",
        modifier=planetary_computer.sign_inplace,
    )

    # Use bounds instead of point
    search = catalog.search(collections=["nasadem"], bbox=bounds, limit=5)
    nasadem_item = next(search.items())
    elevation_raster = stackstac.stack(
        [nasadem_item.to_dict()],
        epsg=4326,
        resampling=rasterio.enums.Resampling.bilinear,
        chunksize=2048,
    ).squeeze()

    # Use the bounds directly for the extent
    x_range = (bounds[0], bounds[2])
    y_range = (bounds[1], bounds[3])

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
    return da_final, bounds