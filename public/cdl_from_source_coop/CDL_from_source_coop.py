@fused.udf
def udf(
    crop_value_list: list = [111], # 54 - Tomatoes
    cell_to_parent_res: int = 9,
    min_ratio: float = 0, # Filtering any value below this percentage out
    year: int = 2024,
):
    """
    Load Hex data from fused-partitioned datasets hosted on source.coop
    https://source.coop/repositories/fused/hex/

    Args:
        - crop_value_list: List of all the crop values for CDL
            Metadata details: https://www.nass.usda.gov/Research_and_Science/Cropland/metadata/metadata_CDL24_FGDC-STD-001-1998.htm
        - cell_to_parent_res: Desired output Hex cell resolution
        - min_ratio: any percentage of crop below this value will be filtered out
        - year: Year to display data from (only supports available years from data on Source Coop: 2012, 2014, 2016, 2018, 2020, 2022, 2024)
    """

    # Loading common Fused helper functions to setup DuckDB in this UDF
    common = fused.load("https://github.com/fusedio/udfs/tree/f5cf238/public/common/").utils
    con = common.duckdb_connect()

    # Default to using the low resolution version first
    s3_resolution = 7
    qr_hex = 'hex'
    
    if cell_to_parent_res < 7:
        qr_hex=f'h3_cell_to_parent(hex, {cell_to_parent_res})' 
    elif cell_to_parent_res == 8:
        s3_resolution = 8
    elif cell_to_parent_res >= 8:
        print("Any resolution above 8 isn't currently supported. Defaulting to hex8 dataset...")
        s3_resolution = 8
        
    path = f's3://us-west-2.opendata.source.coop/fused/hex/release_2025_04_beta/cdl/hex{s3_resolution}_{year}.parquet'
    print(f"{s3_resolution=}")
    print(f"{path=}")

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
    return
    # return df2
