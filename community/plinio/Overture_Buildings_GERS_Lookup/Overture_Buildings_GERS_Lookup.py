@fused.udf
def udf(gers_id: str='08b2a100d2cb6fff02000821de8bdff1'):
    import h3
    from shapely.geometry import Polygon
    import geopandas as gpd
    import pandas as pd

    # 1. H3 from GERS
    h3_index = gers_id[:16]
    print('h3_index', h3_index)

    # 2. Polygon from H3
    bounds = Polygon([coord[::-1] for coord in h3.cell_to_boundary(h3_index)])
    bbox = gpd.GeoDataFrame({'h3_index': [h3_index], 'geometry': [bounds]})

    # 3. Load Overture Buildings
    overture_maps = fused.load("https://github.com/fusedio/udfs/tree/38ff24d/public/Overture_Maps_Example/")
    bounds_tuple = bbox.total_bounds  # [minx, miny, maxx, maxy]
    gdf = overture_maps.get_overture(bounds=bounds_tuple, overture_type='building', min_zoom=10)
    
    print(f"Found {len(gdf)} buildings in H3 cell {h3_index}")
    
    # 4. Subselect building - try exact match first, then partial match
    gdf_result = gdf[gdf['id'] == gers_id]
    
    if len(gdf_result) == 0:
        # Try partial match with just the base ID (without the suffix)
        base_id = gers_id.split('_')[0] if '_' in gers_id else gers_id
        gdf_result = gdf[gdf['id'].str.contains(base_id, na=False)]
        print(f"No exact match found, trying partial match with base ID: {base_id}")
    
    if len(gdf_result) == 0:
        # If still no match, return all buildings in the cell for debugging
        print("No matching building found. Returning all buildings in the H3 cell:")
        print(gdf[['id']].head())
        return gdf
    
    # 5. De-struct the primary names column
    gdf_result['primary'] = gdf_result['names'].apply(lambda x: x.get('primary') if isinstance(x, dict) else None)
    cols = ['id', 'primary', 'subtype', 'class', 'geometry']
    
    print(f"Found {len(gdf_result)} matching building(s)")
    print(gdf_result[cols].T)
    
    return gdf_result[cols]