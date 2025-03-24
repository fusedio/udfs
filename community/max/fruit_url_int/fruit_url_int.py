@fused.udf
def udf(fruit_id: str = "apple"):
    """
    This UDF returns the URL of image of which ever 3 supported fruits is passed as input
    Mapping represented as:
    1 - Orange
    2 - Banana
    3 - Apple
    """
    import pandas as pd

    
    if fruit_id == "orange":
        # Orange
        url = "https://www.wileslakefarmmarket.com/uploads/1/3/0/6/130665087/s850555378219019228_p64_i1_w2560.jpeg"
    elif fruit_id == "banana":
        # banana
        url = "https://images3.alphacoders.com/658/658610.jpg"
    elif fruit_id == "apple":
        # Apple
        url = "https://images.pexels.com/photos/39803/pexels-photo-39803.jpeg"
    else:  
        url = "https://www.nicepng.com/png/detail/775-7752286_empty-basket-for-gifts-wood-basket-with-handle.png"

    return pd.DataFrame({'url': [url]})
