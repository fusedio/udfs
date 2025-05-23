@fused.udf
def udf(bounds: fused.types.Bounds = [-2.997,53.399,-2.975,53.412]):

    # convert bounds to tile
    common_utils = fused.load("https://github.com/fusedio/udfs/tree/2f41ae1/public/common/").utils
    tile = common_utils.get_tiles(bounds, clip=True)

    # Load Addresscloud data
    path: str = "s3://us-west-2.opendata.source.coop/addresscloud/epc/geoparquet-local-authority/Liverpool.parquet"
    utils = fused.load(
        "https://github.com/fusedio/udfs/tree/95872cd/public/common"
    ).utils
    gdf = utils.read_gdf_file(path).to_crs("EPSG:4326")

    # Load Overture data
    udf = fused.load("https://github.com/fusedio/udfs/tree/2ea46f3/public/Overture_Maps_Example/")
    gdf_overture = fused.run(
        udf,
        theme="buildings",
        overture_type="building",
        bounds=tile,
    )

    # Join
    gdf_joined = gdf_overture.sjoin(gdf)
    return gdf_joined[["geometry", "POTENTIAL_ENERGY_RATING"]]
