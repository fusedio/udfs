@fused.udf
def udf(
    # bbox,
    time_of_interest="2023-12-01/2023-12-10"
):  
    import geopandas as gpd
    import shapely
    bbox = gpd.GeoDataFrame({}, geometry=[shapely.box(1.0327936764743764,51.428602071115954,1.137246717432179,51.49368102911712)])
    @fused.cache
    def get_data(bbox, time_of_interest):
        import odc.stac
        import planetary_computer
        import pystac_client
        from pystac.extensions.eo import EOExtension as eo
        import utils
        catalog = pystac_client.Client.open(
                "https://planetarycomputer.microsoft.com/api/stac/v1",
                modifier=planetary_computer.sign_inplace,
            )
        items = catalog.search(
            collections=["sentinel-1-grd"],
            bbox=bbox.total_bounds,
            datetime=time_of_interest,
            query=None,
        ).item_collection()
        # print(items.to_dict())
        # Capping resolution to min 10m, the native Sentinel 2 pixel size
        # resolution = int(10 * 2 ** max(0, (15 - bbox.z[0])))
        # print(f"{resolution=}")
        resolution=10
        if len(items) < 1:
            print(f'No items found. Please either zoom out or move to a different area')
        else:
            print(f"Returned {len(items)} Items")
            @fused.cache
            def fn(bbox,resolution, time_of_interest):
                ds = odc.stac.load(
                        items,
                        crs="EPSG:3857",
                        bands=['vv'],
                        resolution=resolution,
                        bbox=bbox.total_bounds,
                    ).astype(float)
                return ds
            ds=fn(bbox,resolution, time_of_interest)
            da =  ds['vv'].isel(time=0)
            print(da)
            return da
    da=get_data(bbox, time_of_interest) 
    image = da.values*1.


    import numpy as np
    from PIL import Image
    # Function to pad and shift image
    def pad_and_shift_image(img, x_shift, y_shift, pad_value=0):
        """Pad and shift an image by x_shift and y_shift with specified pad_value."""
        # Add padding around the image
        padded_img = np.pad(img, pad_width=3, mode='constant', constant_values=pad_value)
        # Shift the padded image
        shifted_img = np.roll(np.roll(padded_img, y_shift, axis=0), x_shift, axis=1)
        # Crop back to the original size after shifting
        return shifted_img[3:-3, 3:-3]
    
    # Convert image to NumPy array
    image_array = np.array(image)
    
    # List to store shifted images
    shifted_images = []
    
    # Shifting the image in all combinations of directions (x, y) with padding
    for x in [-3, 0, 3]:  # Shift left (-3), no shift (0), right (3)
        for y in [-3, 0, 3]:  # Shift up (-3), no shift (0), down (3)
            shifted = pad_and_shift_image(image_array, x, y, pad_value=0)
            shifted_images.append(shifted)
    
    print(shifted_images[0].shape)
    # Create a stacked image for visualization
    stacked_image = np.stack(shifted_images)
    # Display the stacked image
    print(stacked_image.shape)
    image=stacked_image.std(axis=0)
    print(image.shape)
    # return (image).astype('uint8'), bbox.total_bounds
    utils = fused.load(
    "https://github.com/fusedio/udfs/tree/2b25cb3/public/common/"
    ).utils
    return utils.arr_to_plasma(image), bbox.total_bounds
