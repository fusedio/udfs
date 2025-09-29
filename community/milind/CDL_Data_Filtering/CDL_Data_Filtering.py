common = fused.load("https://github.com/fusedio/udfs/tree/c576dc9/public/common/")

@fused.udf
def udf(bounds: fused.types.Bounds = [-138.80276917029278,-10.699680369962277,-54.08965045331159,65.89861176695153], buffer_multiple: float = 1, hex_res: int = 4):
    crop_type=''
    
    import pandas as pd
    path = "s3://fused-users/fused/asset/CDL_h12k1p1/year=2024/"

    df = read_hexfile_bounds(bounds=bounds, url=f"{path}overview/hex{hex_res}.parquet", clip=1)
    CDL = fused.load("UDF_CDLs_Tile_Example")
    df = df[df["data"].isin(CDL.crop_type_list(crop_type))]
    import h3.api.basic_int as h3
    df['lat'] = df.hex.map(lambda x:h3.cell_to_latlng(x)[0])
    df['lng'] = df.hex.map(lambda x:h3.cell_to_latlng(x)[1])
    
    print(CDL.crop_stats(df))

    crop_stats = CDL.crop_stats(df)
    name_mapping = crop_stats['name'].to_dict()
    
    con = fused.utils.common.duckdb_connect()

    df = con.sql(
        """
        INSTALL spatial;
        LOAD spatial;
        
        SELECT 
            data,
            lat, 
            lng,
            area, 
            area / (h3_cell_area(hex,'m^2')/100) as pct,
        FROM df
        WHERE data != 0
        """
    ).df()

    df['name'] = df['data'].map(name_mapping)
    

    sample_cols = ['data', 'name', 'lat', 'lng', 'area', 'pct']
    available_cols = [col for col in sample_cols if col in df.columns]
    print(df[available_cols].head(3))
    
    df = df.sort_values('pct', ascending=False)
    df = df.sort_values('data').set_index('data')
    df['pct']=df['pct'].astype(int)
    import h3.api.basic_int as h3
    df['hex']=df.apply(lambda r: h3.latlng_to_cell(r[0],r[1],hex_res),1)
    del df['lat']
    del df['lng']
    del df['name']
    print(df)
    return df 


def read_hexfile_bounds(bounds:list  = [-84.62623688728367,38.24817244542944,-84.59976376273033,38.27050399126871]
                        , url: str = "/Users/sinakashuk/Desktop/_hex6", clip=True):
    common = fused.load("https://github.com/fusedio/udfs/tree/beb4259/public/common/").utils
    from io import BytesIO
    import geopandas as gpd
    import pandas as pd
    import pyarrow.parquet as pq
    import base64
    import shapely
    with pq.ParquetFile(url) as file:
        # do not use pq.read_metadata as it may segfault in versions >= 12 (tested on 15.0.1)
        file_metadata = file.metadata
        metadata = file_metadata.metadata
    metadata_bytes = metadata[b"fused:_metadata"]
    metadata_bytes = base64.decodebytes(metadata_bytes)
    metadata_bio = BytesIO(metadata_bytes)
    df = pd.read_parquet(metadata_bio)
    geoms = shapely.box(
        df["bbox_minx"], df["bbox_miny"], df["bbox_maxx"], df["bbox_maxy"]
    )
    df_meta = gpd.GeoDataFrame(df, geometry=geoms, crs="EPSG:4326")
    
    bbox = common.to_gdf(bounds)
    df_meta = df_meta.sjoin(bbox)
    chunk_ids = df_meta.chunk_id.values
    print(chunk_ids)
    import pyarrow.parquet as pq   
    table=pq.ParquetFile(url)
    df = table.read_row_groups(chunk_ids).to_pandas()#, columns=columns).to_pandas()   
    if clip:
        return common.filter_hex_bounds(df, bounds)
    return df