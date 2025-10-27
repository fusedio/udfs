DEFAULT_CONFIG = {
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

@fused.udf(cache_max_age=0)
def udf(
    gdf = {
        "type": "Feature",
        "properties": {"name": "world"},
        "geometry": {
            "type": "Point",
            "coordinates": [-73.94391387988864, 40.8944276435547]
        }
    },
    config: dict | str | None = None
):
    return pydeck_point(gdf, config)


def pydeck_point(gdf, config):
    import json
    import geopandas as gpd
    import pandas as pd
    import pydeck as pdk

    if config is None or config == "":
        config = DEFAULT_CONFIG
    elif isinstance(config, str):
        config = json.loads(config)

    if isinstance(gdf, dict):
        gdf = gpd.GeoDataFrame.from_features([gdf])

    df = pd.DataFrame(gdf.drop(columns="geometry"))
    df["longitude"] = gdf.geometry.x
    df["latitude"] = gdf.geometry.y

    color = config.get("color", DEFAULT_CONFIG["color"])
    radius = config.get("radius", DEFAULT_CONFIG["radius"])
    opacity = config.get("opacity", DEFAULT_CONFIG["opacity"])
    pickable = config.get("pickable", DEFAULT_CONFIG["pickable"])
    # Use a dark-themed basemap for better contrast in dark mode
    basemap = config.get(
        "basemap",
        "https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json"
    )

    center_lat = config.get("center_lat", float(df["latitude"].mean()))
    center_lon = config.get("center_lon", float(df["longitude"].mean()))
    zoom = config.get("zoom", DEFAULT_CONFIG["zoom"])
    pitch = config.get("pitch", DEFAULT_CONFIG["pitch"])
    bearing = config.get("bearing", DEFAULT_CONFIG["bearing"])

    tooltip_template = config.get("tooltip", None)
    if not tooltip_template:
        cols = [c for c in df.columns if c not in ["longitude", "latitude"]]
        if len(cols) == 0:
            tooltip_template = "lon: {longitude}\nlat: {latitude}"
        else:
            tooltip_template = "\n".join([f"{c}: {{{c}}}" for c in cols])

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
