@fused.udf 
def udf(
    bounds: fused.types.Tile = None,
    release:str= "2025-01-22-0",
    s3_bucket:str= "s3://us-west-2.opendata.source.coop",
):
    from utils import get_overture
    
    gdf = get_overture(
        bbox=bounds,
        release=release,
    )

    gdf['area'] = gdf.to_crs(gdf.estimate_utm_crs()).geometry.area

    gdf = gdf[gdf['area'] < 1000]
    gdf['author'] = ['Max'] * gdf.shape[0]

    # Changes here
    return gdf
