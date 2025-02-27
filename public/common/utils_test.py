import geopandas as gpd
import numpy as np
import shapely
import utils

# Sample bounding box for the San Francisco area (x/y/z = 18/49/7)
bounds_sf_area = gpd.GeoDataFrame(
        {'geometry': [shapely.geometry.box(minx=-135.0, miny=31.952162, maxx=-123.75, maxy=40.979898)]},
        crs="EPSG:4326"
    )

def test_mosaic_tiff_with_some_404s():
    """Test on a list of URLs that result in 404 errors."""

    # Note this test only checks if a mosaic is returned without an error, but does not currently check if the mosaic is valid.
    tiff_list = [
        # Invalid URLs below
        'https://s3.ap-northeast-1.wasabisys.com/je-pds/cog/v1/JAXA.EORC_ALOS.PRISM_AW3D30.v3.2_global/2021-02/2/W140.00-W130.00/W140.00-N30.00-W130.00-N40.00-DSM.tiff',
        'https://s3.ap-northeast-1.wasabisys.com/je-pds/cog/v1/JAXA.EORC_ALOS.PRISM_AW3D30.v3.2_global/2021-02/2/W130.00-W120.00/W130.00-N30.00-W120.00-N40.00-DSM.tiff',
        # Valid URLs below
        'https://s3.ap-northeast-1.wasabisys.com/je-pds/cog/v1/JAXA.EORC_ALOS.PRISM_AW3D30.v3.2_global/2021-02/2/W130.00-W120.00/W130.00-N30.00-W120.00-N40.00-DSM.tiff', 
        ]
    mosaic = utils.mosaic_tiff(
        bbox=bbox_sf_area,
        tiff_list=tiff_list,
        overview_level=1
    )


def test_read_tiff():
    input_tiff_path = 'https://s3.ap-northeast-1.wasabisys.com/je-pds/cog/v1/JAXA.EORC_ALOS.PRISM_AW3D30.v3.2_global/2021-02/2/W140.00-W130.00/W140.00-N30.00-W130.00-N40.00-DSM.tiff'

    utils.read_tiff(
        bbox=bbox_sf_area,
        input_tiff_path=input_tiff_path
    )


def test_visualize_simple_array():
    image_array = utils.visualize(
        data=np.array([[0, 1],
                       [2, 3]]),
        min=0,
        max=3,
        opacity=0.5,
    )
    assert(
        np.array_equal(
            image_array,
            np.array([[[  0, 104],
                       [198, 255]],
                      [[  0, 104],
                       [198, 255]],
                      [[  0, 104],
                       [198, 255]],
                      [[127, 127],
                       [127, 127]]])
        )
    )

def test_visualize_array_with_mask():
    image_array = utils.visualize(
        data=np.array([[1, 1],
                       [1, 1]]),
        mask=np.array([[0, 1],
                       [1, 0]]),
    )
    assert(
        np.array_equal(
            image_array,
            np.array([[[255, 255],
                       [255, 255]],
                      [[255, 255],
                       [255, 255]],
                      [[255, 255],
                       [255, 255]],
                      [[  0, 255],
                       [255,   0]]])
        )
    )


def test_visualize_numpy_masked_array():
    image_masked_array = np.ma.array(
        data=np.array([[0, 1],
                       [2, 3]]),
        mask=[[False, True],
              [True, False]]
    )
    rgba_array = utils.visualize(
        data=image_masked_array, mask=np.array([[0, 0], [1, 1]]),
        min=0,
        max=3,
    )
    assert(
        np.array_equal(
            rgba_array,
            np.array([[[  0,   0],
                       [  0, 255]],
                      [[  0,   0],
                       [  0, 255]],
                      [[  0,   0],
                       [  0, 255]],
                      [[  0,   0],
                       [255,   0]]])
        )
    )
