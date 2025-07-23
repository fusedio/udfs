@fused.udf
def udf(path: str = 's3://fused-asset/misc/marko/traffic-germany/Unfallorte2023_LinRef.csv', res: int = 6):
    import geopandas as gpd
    import pandas as pd
    from shapely.geometry import Polygon
    import h3
    from collections import Counter
    import matplotlib.pyplot as plt

    # Load the CSV data with semicolon as the delimiter
    df = pd.read_csv(path, delimiter=";")
    # print("Data columns:", df.columns)
    
    # Convert large identifier columns to strings to avoid overflow issues
    df['UIDENTSTLAE'] = df['UIDENTSTLAE'].astype(str)
    
    # Convert XGCSWGS84 and YGCSWGS84 columns to float by replacing commas with dots
    df['XGCSWGS84'] = df['XGCSWGS84'].str.replace(',', '.').astype(float)
    df['YGCSWGS84'] = df['YGCSWGS84'].str.replace(',', '.').astype(float)

    # Convert each point to an H3 cell based on latitude and longitude
    df['h3_cells'] = df.apply(lambda row: h3.latlng_to_cell(row['YGCSWGS84'], row['XGCSWGS84'], res), axis=1)

    # Aggregate by H3 cell to find the most common accident severity in each hexagon
    aggregated_data = (
        df.groupby('h3_cells')['UKATEGORIE']
        .apply(lambda x: Counter(x).most_common(1)[0][0])  # Get most common severity
        .reset_index()
    )

    # Convert H3 cells to GeoDataFrame geometry for visualization
    aggregated_data['geometry'] = aggregated_data['h3_cells'].apply(
        lambda x: Polygon([ (lng, lat) for lat, lng in h3.cell_to_boundary(x) ])
    )
    gdf_h3 = gpd.GeoDataFrame(aggregated_data, geometry='geometry', crs="EPSG:4326")
    
    # Define label mapping for accident type (UKATEGORIE)
    type_map = {3: "Accident with slightly injured ", 2: "Accident with seriously injured ", 1: "Accident with persons killed"}
    gdf_h3['type'] = gdf_h3['UKATEGORIE'].map(type_map)
    
    return gdf_h3