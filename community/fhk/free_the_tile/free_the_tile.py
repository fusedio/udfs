"""
Often data is converted to vector tiles but what if we want to go
the other way!?

#freethetile

https://gis.stackexchange.com/questions/475398/extract-features-in-lat-long-co-ordinates-from-vector-tile-layer-pbf-file-pytho
"""

import math
def pixel2deg(xtile, ytile, zoom, xpixel, ypixel, extent = 4096):
    # from https://gis.stackexchange.com/a/460173/44746
    n = 2.0 ** zoom
    xtile = xtile + (xpixel / extent)
    ytile = ytile + ((extent - ypixel) / extent)
    lon_deg = (xtile / n) * 360.0 - 180.0
    lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * ytile / n)))
    lat_deg = math.degrees(lat_rad)
    return (lon_deg, lat_deg)


@fused.udf
def udf(bbox: fused.types.TileGDF=None):
    import geopandas as gpd
    import shapely
    import requests, mapbox_vector_tile, json
    tile_x = bbox.x.iloc[0]
    tile_y = bbox.y.iloc[0]
    tile_z = bbox.z.iloc[0]

    token = '{insert-mapbox-token}'
    r = requests.get(f'https://api.mapbox.com/v4/fhk.b3ivguut/{tile_z}/{tile_x}/{tile_y}.vector.pbf?sku=101JdYmJ0PpRQ&access_token={token}')


    vector3 = mapbox_vector_tile.decode(tile=r.content, 
        transformer = lambda x, y: pixel2deg(tile_x, tile_y, tile_z, x, y)
    )
    json_data = vector3['nrel-windpro_airports-3iv5au']['features']
    gdf = gpd.GeoDataFrame.from_features(json_data)

    return gdf