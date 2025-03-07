import fused
import shapely
import numpy as np
import geopandas as gpd
import rasterio
from rasterio.transform import from_gcps
from rasterio.control import GroundControlPoint as GCP
from rasterio.warp import reproject, Resampling
import PIL
from io import StringIO, BytesIO
from base64 import b64encode
import matplotlib as mpl
import matplotlib.pyplot as plt

k_ring_count = 0
def bounds_to_gdf(bounds_list, crs = 4326):
    box = shapely.box(*bounds_list)
    return gpd.GeoDataFrame(geometry=[box], crs=crs)

def run_async(fn, arr_args, delay=0, max_workers=None):
    import numpy as np
    import concurrent.futures
    import time
    if max_workers is None:
        max_workers = min(len(arr_args), 32)
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
    def delayed_fn(*args, **kwargs):
        time.sleep(delay * np.random.random())
        return fn(*args, **kwargs)
    fn2 = fn if delay == 0 else delayed_fn
    return [r for r in pool.map(fn2, arr_args)]

def poly_fill(geom, zooms=[15], compact=True, k=None):
    import mercantile
    import shapely
    import geopandas as gpd
    tile_list = list(mercantile.tiles(*geom.bounds,zooms=zooms))
    gdf_tiles = gpd.GeoDataFrame(tile_list, geometry=[shapely.box(*mercantile.bounds(i)) for i in tile_list], crs=4326)
    gdf_tiles_intersecting = gdf_tiles[gdf_tiles.intersects(geom)]
    if k:
        temp_list = gdf_tiles_intersecting.apply(lambda row:mercantile.Tile(row.x,row.y,row.z),1)
        clip_list = k_ring_list(temp_list,k)
        if not compact:
            gdf = gpd.GeoDataFrame(clip_list, geometry=[shapely.box(*mercantile.bounds(i)) for i in clip_list], crs=4326)
            return gdf
    else:
        if not compact:
            return gdf_tiles_intersecting
        clip_list = gdf_tiles_intersecting.apply(lambda row:mercantile.Tile(row.x,row.y,row.z),1)
    simple_list = mercantile.simplify(clip_list)
    gdf = gpd.GeoDataFrame(simple_list, geometry=[shapely.box(*mercantile.bounds(i)) for i in simple_list], crs=4326)
    return gdf#.reset_index(drop=True)

def k_ring(tile, k):
    #ToDo: Remove invalid tiles in the globe boundries (e.g. negative values)
    import mercantile
    result = []
    for x in range(tile.x - k, tile.x + k + 1):
        for y in range(tile.y - k, tile.y + k + 1):
            result.append(mercantile.Tile(x, y, tile.z))
    return result

def k_ring_list(tiles, k):
    a = []
    for tile in tiles:
        a.extend(k_ring(tile, k))
    return list(set(a))

def make_tiles_gdf(bounds,zoom = 14):
    df_tiles = poly_fill(shapely.box(*bounds), zooms=[zoom], compact=0, k=k_ring_count)
    df_tiles['bounds'] = df_tiles['geometry'].apply(lambda x:x.bounds,1)
    return df_tiles

def center_of_bounds(bb):
    return 0.5*(bb[2] + bb[0]), 0.5*(bb[3] + bb[1])

def reproject_raster(arr,bounds, crs_in = 4326, crs_out = 3857):
    bbox_src = bounds_to_gdf(bounds, crs = crs_in)
    bbox_dest = bounds_to_gdf(bounds, crs = crs_out)
    with rasterio.Env():
        ul = (bounds[0], bounds[3])  # in lon, lat / x, y order
        ll = (bounds[0], bounds[1])
        ur = (bounds[2], bounds[3])
        lr = (bounds[2], bounds[1])
        rows, cols, depth = arr.shape

        gcps = [
            GCP(0, 0, *ul),
            GCP(0, cols, *ur),
            GCP(rows, 0, *ll),
            GCP(rows, cols, *lr)
        ]

        src_transform = from_gcps(gcps)

        source = np.squeeze(arr)

        dst_crs = crs_out #{'init': 'EPSG:3857'}
        dst_transform, width, height = rasterio.warp.calculate_default_transform(crs_in, crs_out, cols, rows, *bbox_dest.total_bounds)
        if depth ==1:
            dst_shape =  height, width
        else:
            dst_shape =  max(rows,height), max(width,cols), depth

        destination = np.zeros(dst_shape)

        reproject(source,destination,src_transform=src_transform,src_crs=crs_in,dst_transform=dst_transform,dst_crs=crs_out,resampling=Resampling.nearest)

    arr_web = destination
    return arr_web

@fused.cache
def divide_and_conquer(bb, udf_fn, zm = 12, as_list=False):
    gdf_tiles = make_tiles_gdf(bb,zoom = zm)
    coords_total = gdf_tiles.total_bounds
    xdim_total = coords_total[2] - coords_total[0]
    ydim_total = coords_total[3] - coords_total[1]

    coords = np.array(gdf_tiles.iloc[0]['bounds'])
    xdim = coords[2] - coords[0]
    ydim = coords[3] - coords[1]
    x_cnt = int(xdim_total//xdim)
    y_cnt = int(ydim_total//ydim)

    tile_list = list(gdf_tiles['bounds'].values)
    if len(tile_list)>300:
        print('there are ',len(tile_list), 'tiles, too large a bounds, shrink it until there are less than 300 tiles')
    pair_list = run_async(udf_fn, tile_list, delay=0., max_workers=300)
    print('got pair list')
    pair_list_mercator = [(reproject_raster(img,bounds, crs_in = 4326, crs_out = 3857),bounds) for img,bounds in pair_list]

    hlist = [pair_list_mercator[i][0].shape[0] for i in range(len(pair_list_mercator))]
    wlist = [pair_list_mercator[i][0].shape[1] for i in range(len(pair_list_mercator))]
    h = min(hlist)
    w = min(wlist)
    print('img dims:',h,w)
    try:
        depth = pair_list_mercator[0][0].shape[2]
    except:
        depth = 1
        pair_list_mercator = [(np.expand_dims(tup[0], axis=2),tup[1]) for tup in pair_list_mercator]
    hw = min(h,w)
    img_list = [tup[0][:hw,:hw,:] for tup in pair_list_mercator]

    loc_list = [center_of_bounds(tup[1]) for tup in pair_list_mercator]
    print('img loc:',loc_list[0])
    zipped_list = list(zip(loc_list,img_list))
    sorted_list = sorted(zipped_list, key=lambda x: (x[0][1],-x[0][0]))
    #sorted_list = sorted(zipped_list, key=lambda x: x[0][1])
    img_arr = np.array([el[1] for el in sorted_list])
    if as_list:
        return img_arr, coords_total
    # if depth!=4:
    #     stitched_img = img_arr[::-1].reshape(y_cnt,x_cnt,hw,hw,depth).swapaxes(1,2).reshape(y_cnt*hw,x_cnt*hw,depth)
    # else:
    #     return img_arr
    try:
        stitched_img = img_arr[::-1].reshape(y_cnt,x_cnt,hw,hw,depth).swapaxes(1,2).reshape(y_cnt*hw,x_cnt*hw,depth)
    except:
        y_cnt+=1
    try:
        stitched_img = img_arr[::-1].reshape(y_cnt,x_cnt,hw,hw,depth).swapaxes(1,2).reshape(y_cnt*hw,x_cnt*hw,depth)
    except:
        y_cnt = y_cnt-1
        x_cnt +=1
    try:
        stitched_img = img_arr[::-1].reshape(y_cnt,x_cnt,hw,hw,depth).swapaxes(1,2).reshape(y_cnt*hw,x_cnt*hw,depth)
    except:
        y_cnt+=1
    stitched_img = img_arr[::-1].reshape(y_cnt,x_cnt,hw,hw,depth).swapaxes(1,2).reshape(y_cnt*hw,x_cnt*hw,depth)
    return np.array(stitched_img,'uint8'), coords_total

