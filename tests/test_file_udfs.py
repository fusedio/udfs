import os
import pandas as pd
import fused
import geopandas as gpd
import geopandas.testing as gpd_testing
import pandas.testing as pd_testing
import xarray as xr
import pytest
from file_udf_helpers import (
    get_sample_file_from_dataframe,
    get_sample_file_from_gdf,
    get_sample_file_from_text,
    get_sample_netcdf_file,
)


FILES_PATH = os.path.join(os.path.abspath(os.curdir), "files")


@pytest.mark.parametrize(
    "file_format,udf_name",
    [
        ("csv", "DuckDB_CSV"),
        ("json", "DuckDB_JSON"),
        ("parquet", "DuckDB_Parquet"),
    ],
)
def test_udf_loading_with_duckdb(
    sample_dataframe: pd.DataFrame, temp_directory: str, file_format: str, udf_name: str
):
    """
    Tests loading and processing various file formats using DuckDB UDFs.
    Verifies that the UDF correctly loads the file and returns the expected DataFrame.
    """

    file_path = get_sample_file_from_dataframe(
        temp_directory, sample_dataframe, file_format
    )
    udf_path = os.path.join(FILES_PATH, udf_name)
    udf = fused.load(udf_path)
    result = fused.run(udf, path=file_path, engine="local")
    pd_testing.assert_frame_equal(result, sample_dataframe)


@pytest.mark.parametrize(
    "file_format,udf_name",
    [
        ("geojson", "GeoPandas_File"),
        ("gpkg", "GeoPandas_File"),
        ("shp", "GeoPandas_File"),
        ("zip", "GeoPandas_ZIP"),
    ],
)
def test_loading_with_geopandas(
    sample_geodataframe: gpd.GeoDataFrame,
    temp_directory: str,
    file_format: str,
    udf_name: str,
):
    """
    Tests loading and processing various geospatial file formats using GeoPandas UDFs.
    Verifies that the UDF correctly loads the file and returns the expected GeoDataFrame.
    """

    file_path = get_sample_file_from_gdf(
        temp_directory, sample_geodataframe, file_format
    )
    udf = fused.load(os.path.join(FILES_PATH, udf_name))
    result = fused.run(udf, path=file_path, engine="local")
    gpd_testing.assert_geodataframe_equal(result, sample_geodataframe)


def test_loading_with_geopandas_gpx(
    sample_track_dataframe: gpd.GeoDataFrame, temp_directory: str
):
    """
    Tests loading and processing GPX files using GeoPandas UDFs.
    Verifies that the UDF correctly loads the GPX file and returns a GeoDataFrame with the expected name and geometry columns.
    """
    file_path = get_sample_file_from_gdf(temp_directory, sample_track_dataframe, "gpx")
    udf = fused.load(os.path.join(FILES_PATH, "GeoPandas_GPX"))
    result = fused.run(udf, path=file_path, engine="local")
    # just check the name and geometry columns
    pd_testing.assert_series_equal(result["name"], sample_track_dataframe["name"])
    pd_testing.assert_series_equal(result["geometry"], sample_track_dataframe["geometry"])



def test_udf_loading_with_netcdf(sample_xr_dataset: xr.Dataset, temp_directory: str):
    """
    Tests loading and processing NetCDF files using xarray UDFs.
    Verifies that the UDF can load and process NetCDF files without errors.
    """
    # Save the dataset to a NetCDF file
    file_path = get_sample_netcdf_file(temp_directory, sample_xr_dataset)
    udf_path = os.path.join(FILES_PATH, "NetCDF_File")
    udf = fused.load(udf_path)
    fused.run(udf, path=file_path, engine="local")
    # just check if no errors


@pytest.mark.parametrize(
    "file_format,udf_name",
    [
        ("csv", "Pandas_CSV"),
        ("xlsx", "Pandas_Excel"),
        ("parquet", "Pandas_Parquet"),
    ],
)
def test_loading_with_pandas(
    sample_dataframe: pd.DataFrame, temp_directory: str, file_format: str, udf_name: str
):
    """
    Tests loading and processing various file formats using Pandas UDFs.
    Verifies that the UDF correctly loads the file and returns the expected DataFrame.
    """

    file_path = get_sample_file_from_dataframe(
        temp_directory, sample_dataframe, file_format
    )
    udf = fused.load(os.path.join(FILES_PATH, udf_name))
    result = fused.run(udf, path=file_path, engine="local")
    pd_testing.assert_frame_equal(result, sample_dataframe)


def test_loading_with_text(temp_directory: str, sample_text: str):
    """
    Tests loading and processing text files using Text UDFs.
    Verifies that the UDF can load and process text files without errors.
    """

    file_path = get_sample_file_from_text(temp_directory, sample_text)
    udf = fused.load(os.path.join(FILES_PATH, "Text_File"))
    fused.run(udf, path=file_path, engine="local")
    # just check that the function runs with no errors
