@fused.udf
def udf(
    year: int = 2024,
    max_cloud_cover: int = 20,
    resolution: int = 500,
):
    import pandas as pd

    ndvi_udf = fused.load('ndvi_monthly_mean')

    # 5 AOIs from us_counties_from_api (small counties, diverse climates)
    AOIS = [
        {"state_fips": "53", "county_fips": "055"},  # San Juan County, WA
        {"state_fips": "04", "county_fips": "023"},  # Santa Cruz County, AZ
        {"state_fips": "19", "county_fips": "053"},  # Decatur County, IA
        {"state_fips": "01", "county_fips": "087"},  # Macon County, AL
        {"state_fips": "23", "county_fips": "013"},  # Knox County, ME
    ]

    # Build arg list: 5 AOIs × 12 months = 60 jobs
    arg_list = [
        dict(
            state_fips=aoi["state_fips"],
            county_fips=aoi["county_fips"],
            year=year,
            month=m,
            max_cloud_cover=max_cloud_cover,
            resolution=resolution,
        )
        for aoi in AOIS
        for m in range(1, 13)
    ]

    print(f"Total jobs: {len(arg_list)}")


    pool = ndvi_udf.map(
        arg_list,
        max_workers=30,  # Limit to 30 concurrent calls to prevent rate limiting from Microsoft Planetary Computer
    )
    results = pool.results(return_exceptions=True)

    # Filter out any exceptions from failed jobs, then concatenate into a single DataFrame
    valid = [r for r in results if isinstance(r, pd.DataFrame)]
    return pd.concat(valid, ignore_index=True)