@fused.udf
def udf(
    image_basepath="https://fused-asset.s3.us-west-2.amazonaws.com/gfc2020/",
    xy_grid_n=4,
    s3_out_dir="s3://fused-users/fused/plinio/",
        output_suffix="2jan2025",
):
    import os
    import geopandas as gpd
    import pandas as pd
    import boto3
        
    list_of_images = [obj['Key'].split('/')[-1] for obj in boto3.client('s3').list_objects_v2(Bucket='fused-asset', Prefix='gfc2020/').get('Contents', [])]
    df_images = pd.DataFrame({'url': list_of_images})
    
    xy_grid = (xy_grid_n, xy_grid_n)
    s3_object_name = "_".join(["assets_with_bounds",str(xy_grid[0]),str(xy_grid[1]),"antimeridian",output_suffix + ".parquet"])
    s3_file_path = os.path.join(s3_out_dir, s3_object_name)

    # 1. Handle antimeridian
    gdf_grid = create_gdf_antimer_aware(df_images, image_basepath, xy_grid=xy_grid)

    # 2. Handle overlapping cells
    gdf_final = handle_overlapping_cells(gdf_grid)

    # 3. Save
    gdf_final["ind"] = range(len(gdf_final))
    gdf_final.crs = 4326
    gdf_final.to_parquet(s3_file_path, index=False)
    return gdf_final


def get_raster_bounds(filename, image_basepath, xy_grid=(5, 5)):
    from pathlib import Path
    import rasterio
    from shapely.geometry import box
    import geopandas as gpd
    import shapely
    
    path = Path(image_basepath)
    ds = rasterio.open(path / filename)

    # Create a GeoDataFrame with the bounds polygon
    bbox = box(ds.bounds.left, ds.bounds.bottom, ds.bounds.right, ds.bounds.top)
    gdf_bounds = gpd.GeoDataFrame({"geometry": bbox}, index=[0], crs=ds.crs)

    # Define the number of divisions
    gridsize = (nx, ny) = xy_grid

    # Calculate the size of each smaller polygon in degrees
    crs = gdf_bounds.crs
    minx, miny, maxx, maxy = gdf_bounds.total_bounds

    width = (maxx - minx) / nx
    height = (maxy - miny) / ny
    bounding_boxes = []

    for i in range(nx):
        for j in range(ny):
            sub_bbox = gpd.GeoDataFrame(
                geometry=[
                    shapely.geometry.box(
                        minx + i * width,
                        miny + j * height,
                        minx + (i + 1) * width,
                        miny + (j + 1) * height,
                    )
                ],
                crs=crs,
            )
            sub_bbox["fused_sub_id"] = f"{i}_{j}"
            bounding_boxes.append(sub_bbox)
    return [
        [bbox.geometry.total_bounds, bbox["fused_sub_id"].iloc[0]]
        for bbox in bounding_boxes
    ]


@fused.cache
def create_gdf_antimer_aware(df, image_basepath, xy_grid=(4, 4)):
    import pandas as pd
    import geopandas as gpd
    from shapely.geometry import box
    import shapely
    from shapely.geometry.polygon import Polygon
    
    df["out"] = df["url"].apply(
        lambda x: get_raster_bounds(x, xy_grid=xy_grid, image_basepath=image_basepath)
    )
    df = df.explode("out")
    # Create separate columns for sub_bounds and fused_sub_id
    df["sub_bounds"] = df["out"].apply(lambda x: x[0] if isinstance(x, list) else None)
    df["fused_sub_id"] = df["out"].apply(lambda x: x[1])
    df = df.drop(columns=["out"])
    gdf = gpd.GeoDataFrame(df)
    gdf.geometry = [box(*each) for each in df.sub_bounds]
    # gdf['antimeridian'] = False

    def needs_clip_antimeridian(geom):
        xmin, ymin, xmax, ymax = geom.bounds
        if xmin < 180 and xmax > 180:
            return "Must break"
        elif xmin > 180 and xmax > 180:
            return "Past antimeridian"
        else:
            return "Not past antimeridian"

    def clip_antimeridian(geom):
        xmin, ymin, xmax, ymax = geom.geometry.bounds

        split_geoms = gpd.GeoDataFrame(
            {"is_antimer_residual": [False, True]},
            geometry=[
                Polygon(box(xmin, ymin, 180, ymax)),
                Polygon(box(180, ymin, xmax, ymax)),
            ],
        )
        split_geoms["url"] = geom.url
        split_geoms["fused_sub_id"] = geom.fused_sub_id  # 3_3_0 or 3_3_1
        return split_geoms

    def is_antimeridian(row):
        if row.antimeridian_status == "Not past antimeridian":
            return False
        if row.antimeridian_status == "Past antimeridian":
            return True
        else:
            return row.is_antimer_residual

    gdf["antimeridian_status"] = gdf.geometry.apply(needs_clip_antimeridian)
    gdf_to_break = gdf[gdf["antimeridian_status"] == "Must break"]

    if len(gdf_to_break) > 0:
        out = pd.concat([clip_antimeridian(row) for i, row in gdf_to_break.iterrows()])
        # Join antimeridian-clipped cells with all else
        out2 = pd.concat([gdf[gdf["antimeridian_status"] != "Must break"], out])
    else:
        out2 = gdf
    out2["is_antimer_residual"] = out2.apply(is_antimeridian, axis=1)
    out2["sub_bounds"] = out2.geometry.apply(lambda x: list(x.bounds))
    return out2


def handle_overlapping_cells(out2):
    import pandas as pd
    import geopandas as gpd
    
    out2["fused_sub_id"] = (
        out2["fused_sub_id"] + "_" + (out2["is_antimer_residual"] * 1).astype(str)
    )
    gdf_orig = out2
    gdf = out2.copy()

    no_overlaps = []
    for itr in range(10):
        # Create unique gid
        gdf["gid"] = range(len(gdf))
        # Calculate difference
        gdf.set_index("gid", inplace=True)
        dfj = gdf.sjoin(gdf, predicate="overlaps", how="left")
        mask = dfj.index.isin(dfj[dfj.gid_right.isna()].index)
        no_overlaps.append(dfj[mask][["geometry"]].reset_index())
        dfj = dfj[~mask]

        df_other = gpd.GeoDataFrame(
            dfj.groupby("gid_right")["geometry"].apply(lambda x: x.unary_union)
        )
        df_difference = gdf.difference(df_other).to_frame("geometry")

        # Calculate intersection
        df_intersection = (
            gdf[["geometry"]]
            .intersection(dfj.set_index("gid_right")["geometry"])
            .to_frame("geometry")
        )

        # Combine intersection & difference
        df_all = pd.concat([df_intersection, df_difference])
        df_all[df_all.geometry.is_valid]
        if len(df_all) != 0:
            df_all["gid"] = df_all.index
            gdf = (
                df_all.groupby(
                    [df_all["geometry"].normalize().to_wkt()], as_index=False
                )
                .agg(
                    {
                        "geometry": lambda x: list(x)[0],
                    }
                )
                .set_geometry("geometry")
            )
        else:
            df_all = pd.concat(no_overlaps)
            gdf_split_dedup = (
                df_all.groupby(
                    [df_all["geometry"].normalize().to_wkt()], as_index=False
                )
                .agg(
                    {
                        "geometry": lambda x: list(x)[0],
                    }
                )
                .set_geometry("geometry")
            )
            break

    gdf_split_dedup["geom"] = gdf_split_dedup["geometry"]
    gdf_split_dedup["geometry"] = gdf_split_dedup["geometry"].centroid
    dfj = gdf_split_dedup.sjoin(gdf_orig)
    dfj["geometry"] = dfj["geom"]
    gdf_final = (
        dfj.groupby([dfj["geometry"].centroid.to_wkt()])
        .agg(
            {
                "geometry": lambda x: list(x)[0],
                "fused_sub_id": lambda x: list(x),
                "url": lambda x: list(x),
                "sub_bounds": lambda x: list(x),
            }
        )
        .reset_index(drop=True)
        .set_geometry("geometry")
    )
    return gdf_final