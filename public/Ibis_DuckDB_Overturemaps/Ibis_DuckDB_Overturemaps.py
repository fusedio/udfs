@fused.udf
def udf(bbox: fused.types.ViewportGDF):
    import duckdb
    import ibis
    from ibis import _

    @fused.cache 
    def read_data(bbox, url, con):

        minx, miny, maxx, maxy = bbox.bounds.values[0]

        t = con.read_parquet(url, table_name="infra-usa")
        
        expr = t.filter(
            _.bbox.xmin > minx,
            _.bbox.ymin > miny,
            _.bbox.xmax < maxx,
            _.bbox.ymax < maxy,
            _.subtype.isin(["pedestrian", "water"]),
        ).select(["geometry", "subtype", "class"])
        
        return expr


    con_ddb = ibis.duckdb.connect()
    url = (
        "s3://overturemaps-us-west-2/release/2024-11-13.0/theme=base/type=infrastructure/*"
        )

    info = read_data(bbox, url, con_ddb)
    info = info.rename(infra_class="class")

    info = info.filter(info.infra_class.isin(["toilets", 
                                             "drinking_water",
                                             "bench"]))
     
    print(info.infra_class.value_counts().to_pandas())

    gdf = info.to_pandas()


    return gdf