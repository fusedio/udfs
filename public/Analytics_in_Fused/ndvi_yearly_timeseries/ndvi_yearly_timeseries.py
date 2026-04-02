@fused.udf
def udf(
    state_fips: str='53', 
    county_fips: str='033', 
    year: int=2024, 
    max_cloud_cover: int=20, 
    resolution: int=500
):
    import pandas as pd

    ndvi_udf = fused.load('ndvi_monthly_mean')

    # Build arg list for 12 months
    arg_list = [
        dict(state_fips=state_fips, county_fips=county_fips, year=year, month=m,
             max_cloud_cover=max_cloud_cover, resolution=resolution)
        for m in range(1, 13)
    ]

    # Run all 12 months in parallel
    # pool = ndvi_udf.map(
    #     arg_list,
    #     max_workers=6, # only do 6 requests at the same time to prevent rate limiting
    # )
    # results = pool.collect()
    results = fused.submit(
        ndvi_udf,
        arg_list,
        max_workers=6, # only do 6 requests at the same time to prevent rate limiting
    )

    return results