common = fused.load("https://github.com/fusedio/udfs/blob/main/public/common/")
@fused.udf
def udf(bounds: fused.types.Bounds = None):
    res = bounds_to_res(bounds)
    print(res)
    releases = ['2024-02-15-alpha-0', '2024-03-12-alpha-0', '2024-08-20-0', '2024-09-18-0', '2024-10-23-0', '2024-11-13-0', '2024-12-18-0', '2025-01-22-0', '2025-03-19-1', '2025-04-23-0', '2025-05-21-0']
    release1 = releases[-1]
    release2 = releases[-2]
    df1 = common.read_hexfile(bounds, f"s3://fused-users/fused/sina/overture_overview/{release1}/hex{res}.parquet", clip=True)
    df2 = common.read_hexfile(bounds, f"s3://fused-users/fused/sina/overture_overview/{release2}/hex{res}.parquet", clip=True)
    df_all = df1.set_index('hex').join(df2.set_index('hex'), lsuffix='1', rsuffix='2').reset_index()
    df_all['ndiff'] = 100000*(df_all.area_meters1-df_all.area_meters2)/(df_all.area_meters1+df_all.area_meters2)
    df_all['ndiff'] = df_all['ndiff'].abs()
    # print(df_all['ndiff'])
    return df_all

@fused.cache
def bounds_to_res(bounds, res_offset=0, max_res=11, min_res=3):
    z = common.estimate_zoom(bounds)
    return max(min(int(3 + max(0, z - 3) / 1.7 + res_offset), max_res), min_res)

    