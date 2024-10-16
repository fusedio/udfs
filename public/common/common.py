@fused.udf
def udf(
    bbox,
    test_name = 'Test visualize - simple array',
):
    import numpy as np
    import utils
    
    match test_name:
        case 'Test visualize - simple array':
            arr = np.array([[0, 1],
                            [2, 3]])

            rgb = utils.visualize(
                data=arr,
                min=0,
                max=4,
                opacity=0.5,
            )
            return rgb

    return bbox