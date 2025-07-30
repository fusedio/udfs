@fused.udf
#@fused.cache
def udf(west="-120.485537", south="34.879334",  east="-120.400163", north="34.951613",zoom="15", year="2022",time_of_interest="2022-07-01/2022-07-20",cloud_cover_perc = "15",pixel_perc = "95"):
    north=float(north);south=float(south);east=float(east);west=float(west);zoom=float(zoom);cloud_cover_perc = int(cloud_cover_perc);pixel_perc = int(pixel_perc);
    import numpy as np
    import rasterio, shapely
    common = fused.load("https://github.com/fusedio/udfs/tree/6e8abb9/public/common/")
    bounds = [west, south, east,north]
    bbox = common.to_gdf(bounds)
    def try_func():
        success = False
        i = cloud_cover_perc
        pixel = pixel_perc
        while not success:
            try:
                out =fused.run("gabriel@fused.io/S2_3ch", west=west, south=south,  east=east, north=north,
                               time_of_interest=time_of_interest,
                               cloud_cover_perc=str(i),
                               brightness="1.0",
                               zoom=str(zoom),
                               pixel_perc = str(pixel))
                success = True
            except Exception as e:
                #print(e)
                i = i+5
                pixel = max(pixel - 5,25)
                print('cloud cover perc = ',i)
                print('pixel perc = ',pixel)
        return out
    img = try_func()
    img= np.array(img.image[0:3], dtype='uint8')
    print('image dimensions:',img.shape)
    img_x = img.shape[-2]
    img_y = img.shape[-1]
    
    mask = fused.run("gabriel@fused.io/CDL_mask", west=west, south=south,  east=east, north=north, year=year, zoom = int(zoom))
    mask0= np.array(mask.image[0], dtype='uint8')
    print('mask dimensions:',mask0.shape)
    mask= np.expand_dims(mask0, 0)
    m_x = mask.shape[-2]
    m_y = mask.shape[-1]
    
    arr_minx = min(img_x,m_x)
    arr_miny = min(img_y,m_y)
    img_clip = img[:,:arr_minx,:arr_miny]
    mask_clip = mask[:,:arr_minx,:arr_miny]
    arr =  np.concatenate([img_clip,mask_clip])
    return arr, bbox.total_bounds
