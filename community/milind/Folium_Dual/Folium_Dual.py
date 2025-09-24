@fused.udf
def udf(zoom:int=15, name='World'):
    import pandas as pd
    import geopandas as gpd
    from shapely.geometry import Point
    from movingpandas import Trajectory
    import folium

    # Sample Data
    data = {
        'id': [1, 1, 1, 1],
        'time': pd.to_datetime(['2024-01-01 10:00:00', '2024-01-01 10:05:00',
                                '2024-01-01 10:10:00', '2024-01-01 10:15:00']),
        'lat': [40.748817, 40.748900, 40.749000, 40.749200],
        'lon': [-73.985428, -73.986500, -73.988600, -73.99700]
    }

    # Convert to GeoDataFrame
    df = pd.DataFrame(data)
    geometry = [Point(xy) for xy in zip(df['lon'], df['lat'])]
    gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")

    # Create Trajectory
    trajectory = Trajectory(gdf, traj_id=1, t="time")

    # Create Folium Map
    m = folium.Map(location=[40.748817, -73.985428], zoom_start=zoom)

    # Plot Trajectory on Map
    for point in trajectory.df.itertuples():
        folium.Marker(
            location=[point.lat, point.lon],
            popup=str(point.Index)
        ).add_to(m)

    from folium.plugins import SideBySideLayers

    m = folium.Map(height=700,
                  location=[37.803972, -122.421297],
                zoom_start=17,
                max_zoom=20,
                min_zoom=10)

    left_layer = folium.TileLayer(tiles="https://staging.fused.io/server/v1/realtime-shared/fsh_2Ygdq2tXqnj2RQTY1HMDnE/run/tiles/{z}/{x}/{y}?dtype_out_raster=png&dtype_out_vector=csv&var=RGB", attr="RGB", name="RGB", overlay=True, control=False)
    left_layer2 = folium.TileLayer(tiles="https://staging.fused.io/server/v1/realtime-shared/fsh_2Ygdq2tXqnj2RQTY1HMDnE/run/tiles/{z}/{x}/{y}?dtype_out_raster=png&dtype_out_vector=csv&var=RGB", attr="RGB2", name="RGB2", overlay=True, control=False)
    right_layer = folium.TileLayer(tiles="https://staging.fused.io/server/v1/realtime-shared/fsh_2Ygdq2tXqnj2RQTY1HMDnE/run/tiles/{z}/{x}/{y}?dtype_out_raster=png&dtype_out_vector=csv&var=NDVI", attr="NDVI", name="NDVI", overlay=True, control=False)

    left_layer.add_to(m)
    left_layer2.add_to(m)
    right_layer.add_to(m)
    sbs = SideBySideLayers(left_layer, right_layer)
    sbs.add_to(m)

    html_str = m.get_root().render()
    # Removed the inserted "Hello {name}!" HTML string
    # html_str = html_str[:23] + f'Hello {name}!' + html_str[23:]

    common = fused.load("https://github.com/fusedio/udfs/tree/351515e/public/common/")
    return common.html_to_obj(html_str)