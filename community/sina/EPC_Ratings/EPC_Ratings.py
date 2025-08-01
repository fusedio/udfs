@fused.udf
def udf(bounds: fused.types.Bounds = [-2.997,53.399,-2.975,53.412]):

    # convert bounds to tile
    common = fused.load("https://github.com/fusedio/udfs/tree/b672adc/public/common/")
    tile = common.get_tiles(bounds, clip=True)

    # Load Addresscloud data
    path: str = "s3://us-west-2.opendata.source.coop/addresscloud/epc/geoparquet-local-authority/Liverpool.parquet"
    gdf = common.read_gdf_file(path).to_crs("EPSG:4326")

    # Load Overture data
    udf = fused.load("https://github.com/fusedio/udfs/tree/38ff24d/public/Overture_Maps_Example/")
    gdf_overture = fused.run(
        udf,
        theme="buildings",
        overture_type="building",
        bounds=tile,
    )

    # Join
    gdf_joined = gdf_overture.sjoin(gdf)
    return gdf_joined[["geometry", "POTENTIAL_ENERGY_RATING"]]
