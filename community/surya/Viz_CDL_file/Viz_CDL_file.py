year = 2014
hex_res = 9
crop="111"
# path= f"s3://fused-users/fused/fused-tmp/tmp/_del_cnt_all/h12k1p1b7_900_{year}_partitioned"
# path= f"s3://fused-users/fused/fused-tmp/tmp/_del_cnt_all/h12k1p1b7_900_2012_partitioned4/"
# path = f"s3://fused-users/fused/asset/CDL_h12k1p1/year={year}/"
# path = "s3://fused-users/fused/asset/landscan_conus_day_h11k1p1/year=2020/"
path = "s3://fused-users/fused/asset/landscan_conus_night_h11k1p1/year=2020/"


@fused.udf
def udf(hex_res: int = hex_res, path=path):
    L = fused.api.list(path)[:]
    df = fused.submit(udf_, [{"hex_res": hex_res, "path": i} for i in L], debug_mode=0, max_workers=100)
    print(df)
    return df


@fused.udf
def udf(crop=crop, hex_res: int = hex_res, base_res: int = 7, path=f"{path}586141402430177279.parquet"):
    common = fused.load("https://github.com/fusedio/udfs/tree/4d5ec01/public/common/").utils
    H3_From_To_Pos = fused.load("https://github.com/fusedlabs/fusedudfs/tree/5738389/H3_From_To_Pos/")
    qr = f'select * from read_parquet("{path}")'
    qr = H3_From_To_Pos.h3_from_pos_query(qr, columns="*", hex_res=hex_res, base_res=base_res)
    qr = f"""SELECT hex, data, (100*sum(cnt/cnt_total)/7^{11-hex_res})::FLOAT pct, (h3_cell_area(hex,'m^2')*pct/100) as area, (sum(cnt/cnt_total)*h3_cell_area(h3_cell_to_center_child(any_value(hex),11),'m^2'))::DOUBLE area2 FROM ({qr})  GROUP BY 1,2   """
    # qr = f"select * from ({qr}) where data={crop}"
    qr = f"select hex,  sum(data*pct)/sum(pct) as pct from ({qr}) group by 1"
    con = common.duckdb_connect()
    df = con.sql(qr).df()
    df = df[df.pct>10]

    print(df)
    # print(df.sum())
    # print(df.describe().T)
    return df
    
    ### best practice is to get pct first and then do the sum or other analysis -- cnt_total varies because of number of pixel in hex (13-18) which make the denom to not be summable. after division then things are summable
    # qr = f"""SELECT hex, data, (100*sum(cnt/cnt_total)/7^{11-hex_res})::FLOAT pct, (h3_cell_area(hex,'m^2')*pct/100)::INT as area, (sum(cnt/cnt_total)*h3_cell_area(h3_cell_to_center_child(any_value(hex),11),'m^2'))::INT area2 FROM ({qr})  GROUP BY 1,2   """
    # qr = f"""SELECT hex, data, h3_cell_area(any_value(hex),'m^2')*sum(cnt) / SUM(sum(cnt)) OVER (PARTITION BY hex) as area
    #     FROM ({qr})
    # GROUP BY 1,2   """
    # qr = f"""SELECT hex, data, 100*sum(cnt)/sum(cnt_total) as pct, sum(cnt)/sum(cnt_total)*h3_cell_area(any_value(hex),'m^2') area
    #     FROM ({qr})
    # GROUP BY 1,2   """
    # qr = f"""SELECT hex, data, 100*sum(cnt/cnt_total)/7^3 as pct, sum(cnt/cnt_total)/7^3*h3_cell_area(any_value(hex),'m^2') area FROM ({qr}) GROUP BY 1,2   """
    # qr = f"""SELECT hex, data, SUM(sum(cnt)) OVER (PARTITION BY hex) as cnt_all
    #     FROM ({qr})
    # GROUP BY 1,2   """
    # qr = f"""SELECT hex, data, sum(cnt) cnt, sum(cnt_total) cnt_total, sum(cnt/cnt_total)*h3_cell_area(h3_cell_to_center_child(any_value(hex),11),'m^2') area FROM ({qr})  GROUP BY 1,2   """
    # qr = f'select * from ({qr}) where data=111 '
    # qr = f'select sum(cnt_total) from ({qr}) '
    # qr = f"""SELECT * FROM ({qr}) where hex=626673397745655807"""
    # qr = f"""SELECT * FROM ({qr}) where h3_cell_to_parent(hex,4)=h3_cell_to_parent(626673397745655807,4)"""
