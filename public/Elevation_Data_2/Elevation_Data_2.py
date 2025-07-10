"""
Requests
- Granularity: H3 L11
- Steps:
    - Pull the elevation band
    - Calculate the slope in degrees and percent
    - Summarize the elevation and slope values by **mean** in each H3
"""
# TODO: why are there gaps in the output?


@fused.udf
def udf(bounds: fused.types.Tile):  
    import odc.stac
    import planetary_computer
    import pystac_client
    from pystac.extensions.eo import EOExtension as eo
    import utils
    

    catalog = pystac_client.Client.open(
        "https://planetarycomputer.microsoft.com/api/stac/v1",
        modifier=planetary_computer.sign_inplace,
    )

    threedep = catalog.get_child(id="3dep-seamless")
    # The 3DEP seamless dataset covers the entire United States, as you can see from the bounds:
    
    print(threedep.extent.spatial.to_dict())
    
        
    search = catalog.search(collections="3dep-seamless", bbox=bounds.total_bounds)
    items = list(search.item_collection())
    print(f"{len(items)} items found")

    # You can asso choose 30m or 10m res
    items_low_res = [item.to_dict() for item in items if item.properties["gsd"] == 30]
    items_high_res = [item.to_dict() for item in items if item.properties["gsd"] == 10]


    if len(items) < 1:
        print(f'No items found. Please either zoom out or move to a different area')
    else:
        print(f"Returned {len(items)} Items")
        resolution = int(10 * 2 ** max(0, (15 - bounds.z[0])))
    
        ds = odc.stac.load(
                items,
                crs="EPSG:3857",
                # bands=[nir_band, red_band],
                resolution=resolution,
                bbox=bounds.total_bounds,
            ).astype(float)
        # print('ds', ds)
        # print(ds['data'])
        arr = ds['data']


    # TODO: probably want to select a specific band
    arr=arr[0,:,:]
    df = fused.utils.DC_AOI_Tile_Hex.tile_to_df(bounds, arr)
    h3_size = min(int(3+bounds.z[0]/1.5),15)
    # h3_size=10
    print('h3_size', h3_size)
    # data_cols = [f'band{i+1}' for i in range(len(arr))]
    data_cols=['band1']
    df = fused.utils.DC_AOI_Tile_Hex.df_to_hex(df, data_cols=data_cols, h3_size=h3_size, hex_col='hex', return_avg_lalng=True)
    
    # calculate stats: mean pixel value for each hex
    mask=1
    for col in data_cols:
        df[f'agg_{col}']=df[f'agg_{col}'].map(lambda x:x.sum())
        mask=mask*df[f'agg_{col}']>0
    
    # convert the h3_int to h3_hex
    df['hex'] = df['hex'].map(lambda x:hex(x)[2:])
    print(df.T)
    return df

    print('arr', arr.shape) # (8, 123, 123)
    return fused.utils.common.visualize(arr[0,:,:])
       