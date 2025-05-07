import os

import rasterio
from osgeo import gdal


def create_cog(
    input_path: str,
    output_path: str,
    category: str = "orthophoto",
    outSRS: int = None,
    out_nodata_value: int | float = None,
) -> None:
    """Create a Cloud Optimized GeoTIFF (COG) from a GeoTIFF file.

    Args:
        input_path (str): The file path to the input GeoTIFF file.
        output_path (str): The file path where the COG will be created.
        category (str, optional): The type of dataset, either 'orthophoto' or 'elevation'. Defaults to 'orthophoto'.
        outSRS (int, optional): The desired output spatial reference system (EPSG code). If not provided, the input SRS will be used.
        out_nodata_value (int | float, optional): The defined nodata value for the output. If not provided, the input nodata value will be used.

    Raises:
        ValueError: If the specified category is not recognized.
        RuntimeError: If the translation or warping process fails.

    Returns:
        None
    """
    # Enable GDAL exceptions
    gdal.UseExceptions()

    # Read source nodata value and original SRS
    ds = gdal.Open(input_path)
    srcNodata = ds.GetRasterBand(1).GetNoDataValue()
    original_srs = ds.GetProjection()  # Get the original projection from the input file

    if not out_nodata_value:
        out_nodata_value = srcNodata
    # Set up creation options based on category
    if category == "orthophoto":
        compression = "JPEG"
        photometric = "YCBCR" if ds.RasterCount > 1 else "MINISBLACK"
        predictor = "1"
    elif category == "elevation":
        compression = "LZW"
        photometric = "MINISBLACK"
        predictor = "2"
    else:
        raise ValueError(f"Category '{category}' not known.")

    # Determine the output SRS
    dstSRS = (
        f"EPSG:{outSRS}" if outSRS else original_srs
    )  # Use original SRS if outSRS is None

    # TODO: Add mask layer for JPEG compressed output if needed to avoid artifacts
    warp_options = {
        "dstSRS": dstSRS,
        "format": "GTiff",
        "srcNodata": srcNodata,
        "dstNodata": out_nodata_value,
        "resampleAlg": "bilinear",
        "creationOptions": [
            f"COMPRESS={compression}",
            f"PHOTOMETRIC={photometric}",
            f"PREDICTOR={predictor}",
            "TILED=YES",
            "BLOCKXSIZE=256",
            "BLOCKYSIZE=256",
            "num_threads=20",
            "BIGTIFF=YES",
        ],
    }
    # print(srcNodata)
    # print(out_nodata_value)
    # Use gdal.Warp to handle both reprojection and COG creation
    result = gdal.Warp(output_path, input_path, **warp_options)

    if result is None:
        raise RuntimeError(f"Failed to create COG from {input_path} to {output_path}")

    # Close the dataset to ensure it's properly written
    result = None

    # Open for overview building
    ds = gdal.Open(output_path, 1)
    if ds is None:
        raise RuntimeError(f"Failed to open {output_path} for building overviews")

    ds.BuildOverviews("AVERAGE", [2, 4, 8, 16, 32])
    ds = None

    # Verify output exists and can be opened
    if not os.path.exists(output_path):
        raise RuntimeError("COG file was not created")

    with rasterio.open(output_path) as src:
        print(f"Final raster shape: {src.shape}")
        print(f"Final raster CRS: {src.crs}")
        print(f"Final resolution: {src.res}")

    return
