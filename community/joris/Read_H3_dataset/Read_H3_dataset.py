common = fused.load("https://github.com/fusedio/udfs/tree/6b98ee5/public/common/")

@fused.udf
def udf(
    bounds: fused.types.Bounds = [
        -125.82165127797666,21.313670812049978,-65.62955940309448,52.58604956417555
    ], # Default to global contentinal US (without Alaska)
    value: int = 111, # 111 - Water bodies
    res: int = None,
):
    # Example using USDA's Crop Data Layer (CDL) dataset for 2024
    path = "s3://fused-asset/hex/cdls_v8/year=2024/"

    df = read_h3_dataset(path, bounds, res=res, value=value)
    print(df)

    if "cnt" in df.columns and "pct" not in df.columns:
        # for overview files with cnt statistic (TODO add more accurate version in reader)
        df["pct"] = df["cnt"] / df["cnt_total"] * 100
    
    return df


def read_h3_dataset(
    path: str,
    bounds: list | None = None,
    res: int | None = None,
    value: int | None = None,
):
    """
    Read a spatial subset of an H3 Parquet dataset.

    Parameters
    ----------
    path : str
        Path or URI pointing to the directory containing the H3 Parquet dataset. 
    bounds : list
        The spatial bounding box for reading a subset of the dataset.
    res : int, optional
        The desired H3 cell resolution of the result. By default, an optimal
        resolution will be inferred from the zoom level based on `bounds`
    value : scalar, optional
        If specified, the resulting data is filtered for `"data" == value`.

    Returns
    -------
    pd.DataFrame
    """
    if res is None:
        res = bounds_to_res(bounds)
        print(f"using resolution {res}")

    available_overviews = list_available_overviews(path, cache_verbose=False)

    if res in available_overviews:
        df = read_overview(path, bounds, res, value)
        return df

    df = read_dataset(path, bounds, res, value)
    return df


def bounds_to_res(bounds, res_offset=1, max_res=11, min_res=3):
    z = common.estimate_zoom(bounds)
    return max(min(int(3 + z / 1.5 - res_offset), max_res), min_res)


@fused.cache(cache_max_age="30m")
def list_available_overviews(path):
    available_overviews = [
        int(path.split("/")[-1].removeprefix("hex").removesuffix(".parquet"))
        for path in fused.api.list(path.strip("/") + "/overview/")
    ]
    return available_overviews


def read_overview(path, bounds, res, value):
    import pyarrow.dataset as ds

    dataset = ds.dataset(path.strip("/") + f"/overview/hex{res}.parquet", format="parquet")
    col_names = dataset.schema.names

    filtered = False
    if "lat" in col_names and "lng" in col_names:
        # "new" type of overview files -> we can directly filter on the lat/lng columns
        filter = (
            (ds.field("lat") > bounds[1]) & (ds.field("lat") < bounds[3])
            & (ds.field("lng") > bounds[0]) & (ds.field("lng") < bounds[2])
        ) 
        col_names.remove("lat")
        col_names.remove("lng")
        df = dataset.to_table(filter=filter, columns=col_names).to_pandas()
        filtered = True

    elif dataset.schema.metadata and b"fused:_metadata" in dataset.schema.metadata:
        # overview files with fused metadata in FileMetadata
        from io import BytesIO
        import base64
        import pandas as pd
        import geopandas
        import shapely

        metadata = dataset.schema.metadata[b"fused:_metadata"]
        df = pd.read_parquet(BytesIO(base64.decodebytes(metadata)))
        geoms = shapely.box(df["bbox_minx"], df["bbox_miny"], df["bbox_maxx"], df["bbox_maxy"])
        df_meta = geopandas.GeoDataFrame(df, geometry=geoms, crs="EPSG:4326")

        bbox = shapely.box(*bounds)
        df_meta = df_meta[df_meta.geometry.intersects(bbox)]

        fragment = list(dataset.get_fragments())[0]
        fragment = fragment.subset(row_group_ids=list(df_meta["chunk_id"]))
        dataset_filtered = ds.FileSystemDataset([fragment], dataset.schema, dataset.format, dataset.filesystem)

        df = dataset_filtered.to_table().to_pandas()

    else:
        df = dataset.to_table().to_pandas()

    if value is not None:
        df = df[df["data"] == value]

    if not filtered:
        df = common.filter_hex_bounds(df, bounds, col_hex="hex")
    return df


def read_dataset(path, bounds, res, value, base_res=7):
    import pandas as pd
    import shapely

    import h3.api.basic_int as h3

    # Get data and filter based on value and hex_bounds
    dataset = common.get_dataset_from_table(path, bounds)
    df = dataset.to_table().to_pandas()

    data_res = h3.get_resolution(df["hex"].iloc[0])
    if res > data_res:
        res = data_res
        print(f"truncating to data resolution {res}")

    if "pos7" in df.columns:
        # old style files with pos7, pos8, etc columns
        
        # List Cell ids of the requested resolution filling the bbox
        # hex_bounds = common.bounds_to_hex(bounds, 7)
        hex_bounds = h3.geo_to_cells(shapely.box(*bounds), 7)
        df = df[df.pos7.isin(hex_bounds)]

        # Construct "hex" column with cell ids for the requested resolution
        # + recalculate pct
        con = common.duckdb_connect()
        H3_From_To_Pos = fused.load("https://github.com/fusedlabs/fusedudfs/tree/5738389/H3_From_To_Pos/")
        qr = H3_From_To_Pos.h3_from_pos_query("(select * from df)", columns="*", hex_res=res, base_res=7)
        qr = f"""
            SELECT
                hex,
                data,
                (100*sum(cnt/cnt_total)/7^{11-res})::FLOAT pct,
                (h3_cell_area(hex,'m^2')*pct/100) as area,
                (sum(cnt/cnt_total)*h3_cell_area(h3_cell_to_center_child(any_value(hex),11),'m^2'))::DOUBLE area2
            FROM ({qr}) 
            GROUP BY 1,2
        """
        df = con.sql(qr).df()

    # files with "hex" column
    elif res == data_res:
        # reading at originnal data resolution -> no aggregation needed
        con = common.duckdb_connect()
        qr = f"""
            SELECT
                *,
                (100*cnt/cnt_total)::FLOAT AS pct
            FROM df
            WHERE h3_cell_to_lat(hex) BETWEEN {bounds[1]} AND {bounds[3]}
              AND h3_cell_to_lng(hex) BETWEEN {bounds[0]} AND {bounds[2]}
        """
        df = con.sql(qr).df()
        
    else:
        con = common.duckdb_connect()
        qr = f"""
            SELECT * EXCLUDE(hex), h3_cell_to_parent(hex, {res}) AS hex FROM df
            WHERE h3_cell_to_lat(h3_cell_to_parent(hex, {res})) BETWEEN {bounds[1]} AND {bounds[3]}
              AND h3_cell_to_lng(h3_cell_to_parent(hex, {res})) BETWEEN {bounds[0]} AND {bounds[2]}
        """
        if "cnt" in df.columns:
            qr = f"""
                SELECT
                    hex,
                    data,
                    SUM(cnt)::INT AS cnt,
                    SUM(SUM(cnt)) OVER (PARTITION BY hex)::INT AS cnt_total,
                    (100*SUM(cnt/cnt_total)/7^{data_res-res})::FLOAT AS pct
                FROM ({qr}) 
                GROUP BY 1,2
            """
        else:
            data_cols = []
            if "data_sum" in df.columns:
                data_cols.append("SUM(data_sum) as data_sum")
            if "data_avg" in df.columns:
                data_cols.append("AVG(data_avg) as data_avg")
            if "data_min" in df.columns:
                data_cols.append("MIN(data_min) as data_min")
            if "data_max" in df.columns:
                data_cols.append("AVG(data_max) as data_max")

            data_cols_str = ", ".join(data_cols)
            qr = f"""
                SELECT
                    hex,
                    {data_cols_str}
                FROM ({qr}) 
                GROUP BY 1
            """
        df = con.sql(qr).df()

    if value is not None:
        df = df[df["data"] == value].reset_index(drop=True)

    return df
