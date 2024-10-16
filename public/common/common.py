import numpy as np
import utils

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
    return image_array

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
    return image_array


@fused.udf
def udf(bbox):
    # Run tests.
    image_array = test_visualize_simple_array()
    image_array = test_visualize_array_with_mask()
    
    return image_array
    