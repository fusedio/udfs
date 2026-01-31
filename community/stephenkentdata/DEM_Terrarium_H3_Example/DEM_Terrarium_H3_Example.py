@fused.udf
def udf(bounds: fused.types.Bounds= None, 
        res:int=None, 
        stats_type:str="mean",
        png:bool=False,
        color_scale:float=1
):

    # Read tiles from S3 and return as image or H3
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    tile = common.get_tiles(bounds)
    print(tile)
    x, y, z = tile.iloc[0][["x", "y", "z"]]
    image_path = f"s3://elevation-tiles-prod/terrarium/{z}/{x}/{y}.png"
    print(image_path)



    if tile.iloc[0].z < 10:
        print("Zoom in more to load DEM")
        return None 

    # Load Terrarium DEM array
    arr = load(image_path)

    if png:
        return common.arr_to_plasma(arr, min_max=(-1000,2000/color_scale**0.5), colormap='plasma')
    else:
        if res is None:
            res_offset = 0  # lower makes the hex finer
            res = max(min(int(3 + z / 1.5), 12) - res_offset, 2)
        print(res)
        # raster to vector using common Fused function
        df_latlng = common.arr_to_latlng(arr, bounds)
      
        # Create H3 hexagons with DuckDB and numpy
        df = aggregate_df_hex(tile, df_latlng, res, latlng_cols=("lat", "lng"), stats_type=stats_type)

        # Change hexagon apearance in the visualization pallete
        print(df)
        return df


def load(image_path):
    import imageio.v3 as iio
    import s3fs

    # open the images and transform the RBB values to one elevation metric
    with s3fs.S3FileSystem().open(image_path) as f:
        im = iio.imread(f)
        r, g, b = im[:, :, 0], im[:, :, 1], im[:, :, 2]
        elevation = (r * 256 + g + b / 256) - 32768
        return elevation

# @fused.cache
def df_to_hex(tile, df, res, latlng_cols=("lat", "lng")):  
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    xmin, ymin, xmax, ymax = tile.geometry.iloc[0].bounds
    qr = f"""
        SELECT 
            h3_latlng_to_cell({latlng_cols[0]}, {latlng_cols[1]}, {res}) AS hex, 
            ARRAY_AGG(data) as agg_data
        FROM df
        WHERE
            h3_cell_to_lat(hex) >= {ymin} -- make sure we don't have overlap bewtween tiles
            AND h3_cell_to_lat(hex) < {ymax}
            AND h3_cell_to_lng(hex) >= {xmin}
            AND h3_cell_to_lng(hex) < {xmax}
        GROUP BY 1
        """
    con = common.duckdb_connect()
    return con.query(qr).fetchnumpy() # return as a numpy array

# @fused.cache
def aggregate_df_hex(tile, df, res, latlng_cols=("lat", "lng"), stats_type="mean"):
    import numpy as np
    import pandas as pd

    result = df_to_hex(tile, df, res=res, latlng_cols=latlng_cols)

    # result is {'hex': array(...), 'agg_data': array of lists}
    hex_arr = result['hex']
    agg_data_arr = result['agg_data']

    # Apply numpy function to each list in the array
    if stats_type == "sum":
        metric = np.array([np.sum(x) for x in agg_data_arr])
    elif stats_type == "max":
        metric = np.array([np.max(x) for x in agg_data_arr]) 
    elif stats_type == "mean":
        metric = np.array([np.mean(x) for x in agg_data_arr])
    else:
        metric = np.array([np.mean(x) for x in agg_data_arr])

    return pd.DataFrame({
                        'hex': hex_arr,
                         # 'agg_data': agg_data_arr, # keep if you need: list of every value per cell
                         'metric': metric
                                        })