@fused.udf
def udf(
    crop_value_list: list = [54], # 54 - Tomatoes
    cell_to_parent_res: int = 4,
    min_ratio: float = 0, # Filtering any value below this percentage out
    path: str='s3://us-west-2.opendata.source.coop/fused/hex/release_2025_04_beta/cdl/hex7_2020.parquet'
):
    """
    Load Hex data from fused-partitioned datasets hosted on source.coop
    https://source.coop/repositories/fused/hex/

    Args:
        - crop_value_list: List of all the crop values for CDL
            Metadata details: https://www.nass.usda.gov/Research_and_Science/Cropland/metadata/metadata_CDL24_FGDC-STD-001-1998.htm
        - cell_to_parent_res: Desired output Hex cell resolution
        - min_ratio: any percentage of crop below this value will be filtered out
        - path: Path to S3 bucket
    """

    # Loading common Fused helper functions to setup DuckDB in this UDF
    common = fused.load("https://github.com/fusedio/udfs/tree/f5cf238/public/common/").utils
    con = common.duckdb_connect()
    if cell_to_parent_res < 7:
        qr_hex=f'h3_cell_to_parent(hex, {cell_to_parent_res})' 
    else:
        qr_hex = 'hex'

    @fused.cache
    def get_hex_data(qr_hex, path):
        # This parses all of CDL at the resolution we'd like & calculates percentage of value in each resulting polygon
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
