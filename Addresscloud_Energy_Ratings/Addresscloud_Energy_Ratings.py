@fused.udf
def udf(
    bbox: fused.types.TileGDF
):
    # Load Addresscloud data
    path: str='s3://us-west-2.opendata.source.coop/addresscloud/epc/geoparquet-local-authority/Liverpool.parquet'
    utils = fused.load("https://github.com/fusedio/udfs/tree/95872cd/public/common").utils
    gdf = utils.read_gdf_file(path).to_crs("EPSG:4326")

    # Load Overture data
    gdf_overture = fused.run(
        "UDF_Overture_Maps_Example",
        theme='buildings',
        overture_type='building',
        bbox=bbox,
    )

    # Join
    gdf_joined = gdf_overture.sjoin(gdf)
    return gdf_joined[['geometry', 'POTENTIAL_ENERGY_RATING']]
