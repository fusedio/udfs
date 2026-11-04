@fused.udf
def udf():
    import pandas as pd
    import shapely
    import geopandas as gpd

    @fused.cache
    def load_data():
        return pd.read_json('https://raw.githubusercontent.com/visgl/deck.gl-data/master/website/pointcloud.json')

    data = load_data()

    data['position0'] = data['position'].apply(lambda x: x[0])
    data['position1'] = data['position'].apply(lambda x: x[1])
    data['position2'] = data['position'].apply(lambda x: x[2])
    data['normal0'] = data['normal'].apply(lambda x: x[0])
    data['normal1'] = data['normal'].apply(lambda x: x[1])
    data['normal2'] = data['normal'].apply(lambda x: x[2])
    data['color0'] = data['color'].apply(lambda x: x[0])
    data['color1'] = data['color'].apply(lambda x: x[1])
    data['color2'] = data['color'].apply(lambda x: x[2])
    
    print(data.T)

    gdf = gpd.GeoDataFrame(data, geometry=[shapely.Point(-122.4, 37.74) for i in range(len(data))])
    return gdf
