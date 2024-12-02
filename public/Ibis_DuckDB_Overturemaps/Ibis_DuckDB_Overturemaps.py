@fused.udf
def udf(bbox: fused.types.ViewportGDF):
    import duckdb
    import ibis
    from ibis import _

    # Instantiate Ibis client
    con_ddb = con = ibis.duckdb.connect()

    # Overture S3 bucket
    url = "s3://overturemaps-us-west-2/release/2024-11-13.0/theme=base/type=infrastructure/*"

    # Structure bounding box to spatially filter viewport
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
                "information",
                "vending_machine",
                "fountain",
                "viewpoint",
                "post_box",
                "drinking_water",
            ]
        )
    )

    # Show value counts
    print(ibis_table.infra_class.value_counts().to_pandas())

    # Return as Pandas
    return ibis_table.to_pandas()
