common = fused.load("https://github.com/fusedio/udfs/tree/1ed3d54/public/common/").utils

@fused.udf
def udf(
    query: str = "show me tomato ",
    cell_to_parent_res: int = 7,
    min_ratio: float = 0, 
    path: str='s3://us-west-2.opendata.source.coop/fused/hex/release_2025_04_beta/cdl/hex7_2024.parquet'
):
    con = common.duckdb_connect()

    crop_value_list = common.query_to_params(query)

    
    print(crop_value_list)
    
    if cell_to_parent_res > 7:
        qr_hex=f'h3_cell_to_parent(hex, {cell_to_parent_res})' 
    else:
        qr_hex = 'hex'

    @fused.cache
    def get_hex_data(qr_hex, path):
        return con.sql(f'''select 
                    {qr_hex} as hex,   
                    value,    
                    sum(area)::INT as area,
                    100*sum(area)/SUM(sum(area)) OVER (PARTITION BY {qr_hex})::FLOAT as pct
                    from read_parquet("{path}")  
                group by 1, 2 
                order by value DESC
                ''').df()
    df2 = get_hex_data(qr_hex, path)
    df2 = df2[df2['value'].isin(crop_value_list)]
    df2 = df2[df2['pct']>100*min_ratio]
    print(f"{df2.shape=}")
    return df2
