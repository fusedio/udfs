@fused.udf
def udf(bounds: fused.types.Bounds = [-125, 32, -114, 42]):
    import pandas as pd

    monthly_udf = fused.load('era5_temp_monthly_average')

    # All months from 2019 to 2024
    months = [f'{y}-{m:02d}' for y in range(2019, 2025) for m in range(1, 13)]

    # Run all months in parallel
    pool = monthly_udf.map([{
        'month': m,
        'bounds': list(bounds)
    } for m in months])
    df = pool.df()

    df = df.reset_index(drop=True).sort_values('date').reset_index(drop=True)

    print(f"{df.shape=}")
    print(df.head())

    return df