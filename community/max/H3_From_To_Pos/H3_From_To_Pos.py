common = fused.load("https://github.com/fusedio/udfs/tree/351515e/public/common/")

bounds = [-96.9705389218286,48.89962286159903,-96.87872570408243,48.93306667646431]
@fused.udf
def udf(bounds: fused.types.Bounds=bounds, path: str='s3://fused-users/fused/clients/bcg/cdls_hexify_v2/year=2024/2024_30m_cdls_1523x48736_33*_0.parquet'):
    import geopandas as gpd
    import pandas as pd
    from datetime import datetime
    s = datetime.now()
    con = common.duckdb_connect()  
    # path='s3://fused-users/fused/sina/_del_/hex15cell_compress.parquet'
    path = 's3://fused-users/fused/clients/bcg/cdls_hexify_v2/year=2024/2024_30m_cdls_1523x48736_50_0.parquet'
    qr=f'''select *, h3_cell_to_parent(hex15, 12) as hex12 FROM read_parquet("{path}") where value>0 limit 10_000_000'''
    df = con.sql(qr).df()
    print(datetime.now()-s)
    df2 = h3_to_pos_test(df, 12)
    print('post h3_to_pos_test',datetime.now()-s)
    print(datetime.now()-s)
    
    df3 = h3_from_pos_test(df2,10,'df')
    print('post h3_from_pos_test',datetime.now()-s)

    df3 = df3.drop_duplicates('hex')
    # df3 = common.filter_hex_bounds(df3, bounds=bounds, col_hex='hex')
    print(df3.shape) 
    print(datetime.now()-s)
    return df3
    # print(datetime.now()-s)
    # print(df3)
    # return df3.head()



# @fused.cache
def h3_to_pos_test(_df, res = 12, base_res=6):
    df=_df
    con = common.duckdb_connect()
    qr = h3_to_pos_query('df', hex_col=f'hex{res}', hex_res=res, base_res=base_res, add_pos_all_col=False)
    print(qr)
    df2 = con.sql(qr).df() 
    del df2[f'hex{res}']
    del df2[f'hex15']
    return df2

# @fused.cache
def h3_from_pos_test(df, res = 12, table='df', base_res=6):
    con = common.duckdb_connect()
    qr = h3_from_pos_query(table, hex_res=res, base_res=base_res, hex_col=f'hex', exclude_pos_cols=False)
    print(qr)
    df2 = con.sql(qr).df()
    return df2


def h3_to_pos_query(table='df', hex_res=12, hex_col='hex', base_res=0, columns = '*', pos_col_prefix='pos', add_pos_all_col=False):
    pos_all=''
    if add_pos_all_col:
        pos_list = [f"{pos_col_prefix}{i}" for i in range(base_res+1, hex_res+1)]+['0']*(15-hex_res)
        pos_all = f'\n, CAST({"||".join(pos_list)} AS UINT64) AS {pos_col_prefix}_all'       
    # base_pos = f'CAST(h3_get_base_cell_number(h3_cell_to_parent({hex_col},0)) AS UINT8) AS {pos_col_prefix}0'
    base_pos = f'CAST(h3_cell_to_parent({hex_col}, {base_res}) AS UINT64) AS {pos_col_prefix}{base_res}'
    query_parts=[f'SELECT {columns},\n {base_pos}']
    for res in range(base_res+1, hex_res+1):
        query_parts.append(f"CAST(h3_cell_to_child_pos(h3_cell_to_parent({hex_col},{res}), {res-1}) AS UINT8) AS {pos_col_prefix}{res}")    
    query = '('+',\n   '.join(query_parts)+f"""{pos_all}
    FROM ({table})
    ORDER BY {hex_col}
    )"""
    return query

def h3_from_pos_query(table='df', hex_col='hex', hex_res=12, base_res=0, columns = '*', pos_col_prefix='pos', exclude_pos_cols=True):
        # pos_to_cell = f'CAST ({pos_col_prefix}0*2*16**11 as UINT64) + 576495936675512319'
        pos_to_cell = f'{pos_col_prefix}{base_res}'
        if hex_res<base_res:
            pos_to_cell = f'''h3_cell_to_parent({pos_col_prefix}{base_res}, {hex_res})'''
        else:
            for res in range(base_res+1, hex_res+1):
                pos_to_cell = f'''h3_child_pos_to_cell(
                                  {pos_col_prefix}{res}, {pos_to_cell},{res})'''
        
            
        if exclude_pos_cols and ('*' in columns) and hex_res>=base_res:
            pos_list = ', '.join([f"{pos_col_prefix}{i}" for i in range(base_res, hex_res+1)])
            columns=columns.replace('*', f'* EXCLUDE({pos_list})')
        query = f'''(
        SELECT {columns},
        {pos_to_cell} AS {hex_col}
        FROM ({table})
        )'''
        return query

def kring_parent_query(table="df", metric="value", res=12, k=1, parent_offset=2, hex_col="hex"):
    return f"""(
    SELECT {metric}, 
            h3_cell_to_parent(hexk.unnest, {res - parent_offset}) hex, 
            CAST(COUNT({hex_col}) AS INT) as cnt
    FROM ({table}),
         UNNEST(h3_grid_disk(h3_cell_to_parent({hex_col},{res}), {k})) AS hexk
    GROUP BY 1,2)
    """
def file_chunk_hexmeta_query(table='df', file_res=1, chunk_res=3, filename_col="filename", hex_col='hex', k=1):
    qr=f"""(select 
            h3_cell_to_parent({hex_col},{chunk_res}) as hex,
            h3_cell_to_parent({hex_col},{file_res}) as file_id,
            h3_cell_to_child_pos(h3_cell_to_parent({hex_col},{chunk_res}),{chunk_res-file_res}) as chunk_id,
            filename_part
            min(h3_cell_to_lng(hexk.unnest)) as minx,
            min(h3_cell_to_lat(hexk.unnest)) as miny,
            max(h3_cell_to_lng(hexk.unnest)) as maxx,
            max(h3_cell_to_lat(hexk.unnest)) as maxy,
        FROM ({table}) 
        , UNNEST(h3_grid_disk({hex_col}, {k})) AS hexk
        group by 1,2,3
        order by 1)
            """
    if filename_col:
        qr=qr.replace('filename_part','array_agg(filename) filename,')
    else:
        qr=qr.replace('filename_part','')
    return qr

def groupby_cnt_query(table="df", hex_col="hex", groupby_cols=["value"], cnt_col="cnt", add_pct=True):
    if hex_col in groupby_cols:
        groupby_cols.remove(hex_col)
        print(f"{hex_col=} is already in {groupby_cols=}. Removing it from the groupby_cols list.")
    groupby_str = ", ".join([i for i in groupby_cols])
    gb_str = ", ".join([str(i + 1) for i in range(len(groupby_cols))])
    if add_pct:
        pct = f"100*sum({cnt_col})/SUM(sum({cnt_col})) OVER (PARTITION BY {groupby_str})::FLOAT as pct,"
    else:
        pct = ""
    return f"""(
    SELECT {hex_col}, {groupby_str}, 
            sum({cnt_col})::INT as {cnt_col},
            SUM(sum({cnt_col})) OVER (PARTITION BY {groupby_cols[0]})::INT as total_{cnt_col},
            {pct}  
    FROM {table}
    GROUP BY {hex_col}, {groupby_str} 
    )"""

def hex_range_to_bounds(hex_min=608658850827993087, hex_max=608658863813558271, chunk_res=2, k=0):
    import h3.api.basic_int as h3
    import pandas as pd
    import geopandas as gpd
    from shapely.geometry import box
    res = h3.get_resolution(hex_min)
    h_list = h3.cell_to_children(h3.cell_to_parent(hex_min,chunk_res),res)
    ind_min = h3.cell_to_child_pos(hex_min, chunk_res)
    ind_max = h3.cell_to_child_pos(hex_max, chunk_res)
    L = h_list[ind_min:ind_max+1]
    flat_list = [item for h in L for item in h3.grid_disk(h,k)]
    df = pd.DataFrame(list(map(h3.cell_to_latlng, flat_list)), columns=['lat','lng'])
    miny,minx = df.min().values
    maxy,maxx = df.max().values
    geom = box(minx, miny, maxx, maxy)
    gdf = gpd.GeoDataFrame([{'bbox_minx': minx,'bbox_miny': miny,'bbox_maxx': maxx,'bbox_maxy': maxy,'geometry': geom}],crs="EPSG:4326")
    return gdf

def get_meta_bounds(file_path, base_res=7, k_buffer=1, pos_col_prefix='pos'):
    import pandas as pd
    import h3.api.basic_int as h3
    hex_col=f'{pos_col_prefix}{base_res}'
    a = file_path.split('/')
    file_id = a[-1].split('.')[0]
    df_stats = common.get_parquet_stats(file_path, cache_reset=True)
    df_stats["k_buffer"] = k_buffer
    df_stats["file_res"] = h3.get_resolution(int(file_id))
    vals = df_stats[[hex_col + "_min", hex_col + "_max", "file_res", "k_buffer"]].values
    df = pd.concat(map(lambda x: hex_range_to_bounds(*x), vals)).reset_index(drop=True)
    df["chunk_id"] = df_stats["row_group"].values
    df["file_id"] = file_id
    return df[["file_id", "chunk_id", "bbox_minx", "bbox_miny", "bbox_maxx", "bbox_maxy", "geometry"]]

@fused.cache
def get_chunk_bounds(hex_id, base_res=7, k_buffer=1):
    import numpy as np
    import h3.api.basic_int as h3
    latlngs = np.asanyarray([h3.cell_to_latlng(item) for h in h3.cell_to_children(hex_id, base_res) for item in h3.grid_disk(h,k_buffer)])
    import geopandas as gpd
    from shapely.geometry import box
    miny = latlngs[:,0].min()
    minx = latlngs[:,1].min()
    maxy = latlngs[:,0].max()
    maxx = latlngs[:,1].max()
    geom = box(minx, miny, maxx, maxy)
    return gpd.GeoDataFrame([{'hex_id':hex_id,'bbox_minx': minx,'bbox_miny': miny,'bbox_maxx': maxx,'bbox_maxy': maxy,'geometry': geom}],crs="EPSG:4326")

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