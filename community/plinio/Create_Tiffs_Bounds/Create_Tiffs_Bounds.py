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
    
    from utils import create_gdf_antimer_aware, handle_overlapping_cells
    
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
