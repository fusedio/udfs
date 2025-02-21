@fused.udf
def udf(bbox: fused.types.Bounds = None):
    import ibis
    from ibis import _

    # Connect to an in-memory database
    con = ibis.duckdb.connect()
    
    # Converting bounds to GeoDataFrame
    bbox = fused.utils.common.bounds_to_gdf(bbox)
    # Overture S3 bucket
    url = "s3://overturemaps-us-west-2/release/2024-11-13.0/theme=base/type=infrastructure/*"

    # Structure bounding box to spatially filter viewport
    if bbox is None:
        minx, miny, maxx, maxy = [
            4.83097697496089,
            52.32640789400446,
            4.982257776088744,
            52.40928707739445,
        ]
    else:
        minx, miny, maxx, maxy = bbox.bounds.values[0]

    # Read data
    t = con.read_parquet(url, table_name="infra-usa")

    ibis_table = t.filter(
        _.bbox.xmin > minx,
        _.bbox.ymin > miny,
        _.bbox.xmax < maxx,
        _.bbox.ymax < maxy,
        _.subtype.isin(["pedestrian", "water"]),
    ).select(["geometry", "subtype", "class"])

    # Rename column for convenience
    ibis_table = ibis_table.rename(infra_class="class")

    # Filter by infrastructure class
    ibis_table = ibis_table.filter(
        ibis_table.infra_class.isin(
            [
                "toilets",
                "atm",
                "drinking_water",
                "information",
                "vending_machine"
            ]
        )
    )

    # Show value counts
    print(ibis_table.infra_class.value_counts().to_pandas())

    # Return as GeoPandas
    return ibis_table.to_pandas()
