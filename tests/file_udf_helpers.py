import os
import uuid
import zipfile

import geopandas as gpd
import pandas as pd


def _random_filename(directory: str, extension: str | None):
    """Generate a random filename with the given extension in the specified directory"""
    random_id = str(uuid.uuid4())[:6]  # Use first 6 chars of UUID for brevity
    file_name = f"sample_{random_id}"
    if extension:
        file_name += f".{extension}"
    return os.path.join(directory, file_name)


def get_sample_file_from_dataframe(
    temp_directory: str, df: pd.DataFrame, extension: str
):
    """Create a temporary file from a DataFrame for testing."""
    filepath = _random_filename(temp_directory, extension)
    if extension == "csv":
        df.to_csv(filepath, index=False)
    elif extension == "parquet":
        df.to_parquet(filepath, index=False)
    elif extension == "json":
        df.to_json(filepath, orient="records")
    elif extension == "xlsx":
        df.to_excel(filepath, index=False)
    return filepath


def get_sample_file_from_text(temp_directory: str, input: str):
    """Create a temporary text file for testing."""
    filepath = _random_filename(temp_directory, "txt")
    with open(filepath, "w") as f:
        f.write(input)
    return filepath


def get_sample_single_band_netcdf(
    temp_directory: str, georeferenced: bool = True
) -> str:
    """Create a temporary single-band NetCDF file for testing.

    A NetCDF file with a single variable is opened by rioxarray as an
    xarray.DataArray instead of an xarray.Dataset.
    """
    import numpy as np
    import rasterio
    import rioxarray
    import xarray as xr

    filepath = _random_filename(temp_directory, "nc")
    data = xr.DataArray(
        np.arange(64, dtype="float32").reshape(8, 8),
        dims=("y", "x"),
        coords={"y": np.linspace(9, 10, 8), "x": np.linspace(0, 1, 8)},
        name="sif",
    )
    if georeferenced:
        data = data.rio.write_crs("EPSG:4326")
        data = data.rio.write_transform(
            rasterio.transform.from_bounds(0, 0, 8, 8, 8, 8)
        )
    data.to_dataset().to_netcdf(filepath, engine="scipy")
    return filepath


def get_sample_file_from_gdf(
    temp_directory: str, gdf: gpd.GeoDataFrame, extension: str | None
):
    """Create a temporary file from a GeoDataFrame for testing."""
    filepath = _random_filename(temp_directory, extension)

    if extension == "zip":
        with zipfile.ZipFile(filepath, "w") as zip:
            geojson_file = get_sample_file_from_gdf(temp_directory, gdf, "geojson")
            zip.write(geojson_file)
            zip.close()
            return filepath

    gdf.to_file(filepath)
    return filepath
