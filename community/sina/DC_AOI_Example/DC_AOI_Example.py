@fused.udf
def udf(
    time_of_interest = "2021-09-01/2021-12-30",
    chip_len: int = None,
    scale: float = 0.1,
    show_ndvi = True
):
    import numpy as np   
    import geopandas as gpd  


    # Load pinned versions of utility functions.
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
 
    # Lad the geometry
    gdf = gpd.read_file('s3://fused-asset/data/tiger/TIGER_RD18/STATE/11_DISTRICT_OF_COLUMBIA/11/tl_rd22_11_tract.zip')
    if not chip_len:
        xmin,ymin,xmax,ymax = common.to_gdf(gdf, crs='UTM').total_bounds
        chip_len = int(max(xmax-xmin, ymax-ymin) / 10) # considering pixel size of 10m
    
    # Check to make sure the image is not too big
    print(f'{chip_len = }')
    if chip_len>3000:
        raise ValueError(f'THe image is too big ({chip_len=}>3000). Consider reducing your area of interest.')  

    # Get the data
    arr_rgbi = get_arr(gdf, time_of_interest, output_shape=(chip_len, chip_len), nth_item=None)

    # Scale the values for visualization purpose
    if show_ndvi:
        arr = rgbi_to_ndvi(arr_rgbi)
    else:
        arr = np.clip(arr_rgbi[:3] * scale, 0, 255).astype("uint8")
    
    # Create clip using geom and convert to masked array
    geom_mask = common.gdf_to_mask_arr(gdf, arr.shape[-2:])
    if len(arr.shape) == 2:
        arr = np.ma.masked_array(arr, [geom_mask])
    else:
        arr = np.ma.masked_array(arr, [geom_mask]*arr.shape[0])
    return arr, gdf.total_bounds


@fused.cache
def get_arr(
    bounds,
    time_of_interest,
    output_shape,
    nth_item=None,
    max_items=30
):
    
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")

    greenest_example = fused.load('https://github.com/fusedio/udfs/tree/a0af8a/community/sina/Satellite_Greenest_Pixel')

    stac_items = greenest_example.search_pc_catalog(
        bounds=bounds,
        time_of_interest=time_of_interest,
        query={"eo:cloud_cover": {"lt": 5}},
        collection="sentinel-2-l2a"
    )
    if not stac_items: return
    df_tiff_catalog = greenest_example.create_tiffs_catalog(stac_items, ["B02", "B03", "B04", "B08"])
    if len(df_tiff_catalog) > max_items:
        raise ValueError(f'{len(df_tiff_catalog)} > max number of images ({max_items})')  
    if nth_item:
        if nth_item > len(df_tiff_catalog):
            raise ValueError(f'{nth_item} > total number of images ({len(df_tiff_catalog)})') 
        df_tiff_catalog = df_tiff_catalog[nth_item:nth_item + 1]
        arrs_out = greenest_example.run_pool_tiffs(bounds, df_tiff_catalog, output_shape)
        arr = arrs_out.squeeze()
    else:
        arrs_out = greenest_example.run_pool_tiffs(bounds, df_tiff_catalog, output_shape)
        arr = greenest_example.get_greenest_pixel(arrs_out, how='median', fillna=True)
    return arr
    
def rgbi_to_ndvi(arr_rgbi): 
    
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")

    ndvi = (arr_rgbi[-1] * 1.0 - arr_rgbi[-2] * 1.0) / (
        arr_rgbi[-1] * 1.0 + arr_rgbi[-2] * 1.0
    )
    rgb_image = common.visualize(
        data=ndvi,
        min=0,
        max=1,
        colormap=['black', 'green']
    )
    return rgb_image

        