@fused.udf 
def udf(
    bbox: fused.types.TileGDF = None,
    h3_res: int=12
):
    import h3
    import pandas as pd
    from utils import get_strahler_gdf, create_h3_buffer_scored
    
    # A. Bridges
    gdf_bridges = fused.utils.Overture_Maps_Example.get_overture(bbox=bbox, overture_type='infrastructure')
    gdf_bridges = gdf_bridges[gdf_bridges['subtype'] == 'bridge']
    
    # B. Water 
    gdf_water = fused.utils.Overture_Maps_Example.get_overture(bbox=bbox,overture_type='water')
    gdf_water = gdf_water[gdf_water['class'].isin(['river', 'stream', 'lagoon', 'pond', 'drain'])]

    # Keep only bridges that intersect non-oceanic water; (riparean) rivers
    gdf_bridges = gdf_bridges.sjoin(gdf_water[['geometry']], how='inner')

    # C. Golf Courses
    gdf_golf = fused.utils.Overture_Maps_Example.get_overture(bbox=bbox,theme = 'base',overture_type = 'land_use')
    gdf_golf = gdf_golf[gdf_golf['class'] == 'golf_course'].dissolve()

    # D. Strahler
    try:
        # Skip if no GEE
        gdf_strahler = get_strahler_gdf(bbox)
        gdf_water = gdf_water.sjoin(gdf_strahler, how='left')
        # Sort the dataframe by 'strahler_value' in descending order
        gdf_water = gdf_water.sort_values('strahler_value', ascending=False).drop_duplicates(subset='geometry', keep='first')
    except Exception as e:
        pass
    
    # Create a buffer
    buffers_water = {
        "internal_perimeter": {"distance": 10, "score": -10},
        "near_perimeter": {"distance": 75, "score": 3},
        # "furthest_perimeter": {"distance": 2000, "score": 10},
    }
    buffers_bridges = {
        "internal_perimeter": {"distance": 10, "score": 0},
        "near_perimeter": {"distance": 30, "score": 2},
        "furthest_perimeter": {"distance": 60, "score": 1},
    }
    buffers_golf = {
        "internal_perimeter": {"distance": 0, "score": -10},
        "near_perimeter": {"distance": 100, "score": 2},
        "furthest_perimeter": {"distance": 300, "score": 1},
    }

    # Create buffers and scores
    gdf_bridges_buffers=create_h3_buffer_scored(gdf_bridges, buffers_bridges)
    gdf_water_buffers=create_h3_buffer_scored(gdf_water, buffers_water)
    gdf_golf_buffers=create_h3_buffer_scored(gdf_golf, buffers_golf)

    gdfs = [gdf[['cell_id', 'cnt', 'score']] for gdf in [
        gdf_water_buffers, 
        gdf_bridges_buffers,
        gdf_golf_buffers
    ] if len(gdf)>0]
    if len(gdfs) == 0: return
    gdf_concat = pd.concat(gdfs)
    gdf_concat = gdf_concat.groupby('cell_id').agg({'score': 'sum', 'cnt': 'sum'}).reset_index()
    return gdf_concat
