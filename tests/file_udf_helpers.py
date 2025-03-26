import uuid
import os
import pandas as pd
import geopandas as gpd
import zipfile


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
