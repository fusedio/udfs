import os
import pandas as pd
import fused
import geopandas as gpd
import geopandas.testing as gpd_testing
import pandas.testing as pd_testing
import pytest
from file_udf_helpers import (
    get_sample_file_from_dataframe,
    get_sample_file_from_gdf,
    get_sample_file_from_text,
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
        ("kml", "GeoPandas_KML"),
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


def test_loading_geotiff(sample_geotiff_file: str):
    udf = fused.load(os.path.join(FILES_PATH, "GeoTIFF_File"))
    res = fused.run(udf, path=sample_geotiff_file, x=1, y=2, z=1, engine="local")


def test_loading_gpx(sample_gpx_file: str):
    udf = fused.load(os.path.join(FILES_PATH, "GeoPandas_GPX"))
    res = fused.run(udf, path=sample_gpx_file, layer=None, engine="local")
    assert isinstance(res, gpd.GeoDataFrame)
    assert res.shape == (86, 20)

