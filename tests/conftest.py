import os
import pytest
import tempfile

import fused
import geopandas as gpd
from shapely.geometry import Point
import pandas as pd

# Authenticate
if os.environ.get("CI") and os.environ.get("AUTH0_REFRESH_TOKEN"):
    fused._auth.refresh_token(os.getenv("AUTH0_REFRESH_TOKEN"))


@pytest.fixture
def temp_directory():
    """Create a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        yield tmp_dir


@pytest.fixture
def sample_dataframe():
    """Helper function to create a sample pandas DataFrame"""
    return pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "value": [10.1, 20.2, 30.3],
        }
    )


@pytest.fixture
def sample_geodataframe():
    """Helper function to create a sample GeoPandas GeoDataFrame"""
    geometry = [
        Point(x, y) for x, y in zip([-122.4, -122.5, -122.6], [37.8, 37.7, 37.6])
    ]
    return gpd.GeoDataFrame(
        pd.DataFrame(
            {
                "data": pd.Series([1, 2, 3], dtype="int64"),
                # geopandas converts column to int32 after reading from file
                # test will fail if column is not int32
            }
        ),
        geometry=geometry,
        crs="EPSG:4326",
    )


@pytest.fixture
def sample_text():
    return "Hello, World! \nThis is a sample text file."


@pytest.fixture
def sample_gpx_file():
    return os.path.join(os.curdir, "tests/sample_files/sample_gpx.gpx")


@pytest.fixture
def sample_geotiff_file():
    return os.path.join(os.curdir, "tests/sample_files/sample_geotiff.tif")