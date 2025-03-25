import os
import pytest
import tempfile

import fused
import geopandas as gpd
from shapely.geometry import Point, LineString
import pandas as pd
import numpy as np
import xarray as xr

# Authenticate
if os.environ.get("CI"):
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
                "data": pd.Series([1, 2, 3], dtype="int32"),
                # geopandas converts column to int32 after reading from file
                # test will fail if column is not int32
            }
        ),
        geometry=geometry,
        crs="EPSG:4326",
    )


@pytest.fixture
def sample_track_dataframe():
    """Helper function to create a sample GeoPandas GeoDataFrame for gpx file"""
    coords = [(-122.4, 37.8), (-122.5, 37.7), (-122.6, 37.6)]
    line = LineString(coords)
    gdf = gpd.GeoDataFrame(
        {"name": ["Sample Track"]}, geometry=[line], crs="EPSG:4326"
    )
    return gdf


@pytest.fixture
def sample_text():
    return "Hello, World! \nThis is a sample text file."


@pytest.fixture
def sample_xr_dataset():
    """Helper function to create a sample xarray Dataset that can be opened as a NetCDF file"""
    return xr.Dataset(
        {"foo": (("x", "y"), np.random.rand(4, 5))},
        coords={
            "x": [10, 20, 30, 40],
            "y": pd.date_range("2000-01-01", periods=5),
            "z": ("x", list("abcd")),
        },
    )
