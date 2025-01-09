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
    gdf = fused.utils.Overture_Maps_Example.get_overture(bbox=bbox, overture_type='building', min_zoom=10)

    # 4. Subselect building
    gdf = gdf[gdf['id'] == gers_id]
    
    # 5. De-struct the names column
    gdf['primary'] = gdf['names'].apply(lambda x: x.get('primary') if isinstance(x, dict) else None)
    cols = ['id', 'primary', 'subtype', 'class', 'geometry']
    print(gdf[cols].T)
    return gdf[cols]



















    