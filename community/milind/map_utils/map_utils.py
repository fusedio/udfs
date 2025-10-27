import json
import geopandas as gpd
import pandas as pd
import pydeck as pdk



@fused.udf(cache_max_age=0)
def udf(
    gdf = {
        "type": "Feature",
        "properties": {"name": "world"},
        "geometry": {"type": "Point", "coordinates": [-73.94391387988864, 40.8944276435547]}
    },
    config: dict = {
    "color": [0, 144, 255, 200],
    "radius": 50,
    "opacity": 1.0,
    "pickable": True,
    "tooltip": "{name}",
    "center_lat": 40.8944276435547,
    "center_lon": -73.94391387988864,
    "zoom": 13,
    "pitch": 0,
    "bearing": 0
}

):
    html = pydeck_point(gdf, config)
    return html



def pydeck_point(gdf, config):
    if isinstance(config, str):
        config = json.loads(config)
    if config is None:
        config = {}
    if isinstance(gdf, dict):
        gdf = gpd.GeoDataFrame.from_features([gdf])

    df = pd.DataFrame(gdf.drop(columns="geometry"))
    df["longitude"] = gdf.geometry.x
    df["latitude"] = gdf.geometry.y

    color = config.get("color", [0, 144, 255, 200])
    radius = config.get("radius", 50)
    opacity = config.get("opacity", 1.0)
    pickable = config.get("pickable", True)
    tooltip_template = config.get("tooltip", "{name}")
    basemap = config.get("basemap", "https://basemaps.cartocdn.com/gl/positron-gl-style/style.json")

    center_lat = config.get("center_lat", float(df["latitude"].mean()))
    center_lon = config.get("center_lon", float(df["longitude"].mean()))
    zoom = config.get("zoom", 10)
    pitch = config.get("pitch", 0)
    bearing = config.get("bearing", 0)

    layer = pdk.Layer(
        "ScatterplotLayer",
        data=df,
        get_position=["longitude", "latitude"],
        get_fill_color=color,
        get_radius=radius,
        opacity=opacity,
        pickable=pickable,
    )

    view_state = pdk.ViewState(
        latitude=center_lat,
        longitude=center_lon,
        zoom=zoom,
        pitch=pitch,
        bearing=bearing,
    )

    deck = pdk.Deck(
        layers=[layer],
        initial_view_state=view_state,
        map_style=basemap,
        tooltip={"text": tooltip_template},
    )

    return deck.to_html(as_string=True)


