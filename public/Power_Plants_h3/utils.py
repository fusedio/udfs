def run_query(bounds, pplant_table, resolution):
        xmin, ymin, xmax, ymax = bounds
        # Here we make cells ake cells and see top power plant fuel type for each cell
        query = f"""    
        WITH h3_cte AS (
            SELECT
                primary_fuel AS name,
                h3_latlng_to_cell(latitude, longitude, $resolution) AS cell_id
            FROM pplant_table
            WHERE primary_fuel IS NOT NULL
                AND latitude >= $ymin
                AND latitude < $ymax
                AND longitude >= $xmin
                AND longitude < $xmax
        ),
        name_counts AS (
            SELECT
                cell_id,
                name,
                COUNT(*) AS name_count
            FROM h3_cte
            GROUP BY cell_id, name
        ),
        most_frequent_names AS (
            SELECT
                cell_id,
                name,
                name_count AS cnt
            FROM name_counts
            QUALIFY ROW_NUMBER() OVER (PARTITION BY cell_id ORDER BY name_count DESC) = 1
        )
        SELECT 
            cell_id, -- Using the UBIGINT
            name AS primary_fuel,
            cnt
        FROM most_frequent_names;
    
        """
        # Load pinned versions of utility functions.
        utils = fused.load("https://github.com/fusedio/udfs/tree/ee9bec5/public/common/").utils
        # Connect to DuckDB
        con = utils.duckdb_connect()

        # Execute the query and get the DataFrame
        return con.sql(query, params={'xmin': xmin, 'xmax': xmax, 'ymin': ymin, "ymax": ymax, 'resolution': resolution}).df()



# Here we query the Global Power Plant Database, create a Pandas Dataframe and then return an Arrow table
@fused.cache
def get_data():
    import pandas as pd
    import pyarrow as pa
    import requests
    import io
    import zipfile
    url = "https://wri-dataportal-prod.s3.amazonaws.com/manual/global_power_plant_database_v_1_3.zip"
    file_name = "global_power_plant_database.csv"
    with zipfile.ZipFile(io.BytesIO(requests.get(url).content)) as zip_ref:
        df = pd.read_csv(io.BytesIO(zip_ref.read(file_name)))
    table = pa.Table.from_pandas(df)
    return table

# This how we add our CMAP below to the DataFrame for visualizing the power types
# I borrowed this code from Alex Martin's NWS Hazards H3 UDF
def add_rgb_cmap(gdf, key_field, cmap_dict):
    import pandas as pd
    def get_rgb(value):
        if pd.isna(value):
            print(f"Warning: NaN value found in {key_field}")
            return [128, 128, 128]  # Default color for NaN values
        if value not in cmap_dict:
            print(f"Warning: No color found for {value}")
        return cmap_dict.get(value, [128, 128, 128])  # Default to gray if not in cmap

    rgb_series = gdf[key_field].apply(get_rgb)
    
    gdf['r'] = rgb_series.apply(lambda x: x[0])
    gdf['g'] = rgb_series.apply(lambda x: x[1])
    gdf['b'] = rgb_series.apply(lambda x: x[2])
    
    
    
    return gdf

CMAP = {
  "Biomass": [107, 142, 35],
  "Other": [169, 169, 169],
  "Coal": [0, 0, 0],
  "Waste": [128, 0, 128],
  "Petcoke": [105, 105, 105],
  "Geothermal": [255, 140, 0],
  "Storage": [255, 255, 0],
  "Cogeneration": [255, 69, 0],
  "Hydro": [30, 144, 255],
  "Solar": [255, 215, 0],
  "Oil": [255, 69, 0],
  "Wind": [173, 216, 230],
  "Wave and Tidal": [0, 191, 255],
  "Gas": [0, 128, 0],
  "Nuclear": [75, 0, 130]
}
