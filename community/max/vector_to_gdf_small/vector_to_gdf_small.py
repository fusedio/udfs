@fused.udf
def udf(
    bounds: fused.types.Bounds = [8.4452104984018,41.76046948393174,8.903258920921276,42.053137175457145]
):
    """
    On the fly vector to hex (counting the number of vector in each hex at res 15)
    NOTE: This is not meant for datasets larger than 100k vectors. 
    Reach out to Fused at info@fused.io for scaling to larger datasets or for any questions!
    """
    common = fused.load("https://github.com/fusedio/udfs/tree/208c30d/public/common/")
    res = common.bounds_to_res(bounds, offset=3)
    res = max(9, res)
    print(res)
     
    gdf = get_data() # Replace with your own data. Using Overture Maps for example

    if gdf.shape[0] > 100_000:
        print("This method is meant for small areas. Reach out to Fused for scaling this up at info@fused.io")
        return

    # Tiling gdf into smaller chunks of equal size & hexagonifying
    vector_chunks = common.split_gdf(gdf[["geometry"]], n=32, return_type="file")
    df = fused.submit(hexagonify_udf, vector_chunks, res=res, engine="remote").reset_index(drop=True)
    
    df = df.groupby('hex').sum(['cnt','area']).sort_values('hex').reset_index()[['hex', 'cnt', 'area']]
    df['area'] = df['area'].astype(int) # This method of calculating area is accurate only down to 1m, so rounding to closest int

    return df 


@fused.udf
def hexagonify_udf(geometry, res: int = 12):
    common = fused.load("https://github.com/fusedio/udfs/tree/208c30d/public/common/")
    gdf = common.to_gdf(geometry)
    gdf = common.gdf_to_hex(gdf[['geometry']], res=15)
    con = common.duckdb_connect()
    ### ToDo: break down geom_area and properly calculate the area for each hex
    df = con.sql(f""" select h3_cell_to_parent(hex,{res}) as hex, 
        count(1) as cnt, 
        sum(h3_cell_area(hex, 'm^2')) as area
        from gdf
        group by 1
        order by 1
        """).df()
    return df


@fused.cache
def get_data():
    common = fused.load("common")
    gdf = fused.get_chunk_from_table(
        "s3://us-west-2.opendata.source.coop/fused/overture/2025-12-17-0/theme=buildings/type=building/part=3", 10, 0
    )
    return gdf
