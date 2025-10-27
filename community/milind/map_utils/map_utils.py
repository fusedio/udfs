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

DEFAULT_H3_CONFIG = {
    "hex_field": "hex",
    "fill_color": "[0, 144, 255, 180]",
    "pickable": True,
    "stroked": True,
    "filled": True,
    "extruded": False,
    "line_color": [255, 255, 255],
    "line_width_min_pixels": 2,
    "center_lat": 37.7749295,
    "center_lon": -122.4194155,
    "zoom": 14,
    "bearing": 0,
    "pitch": 30,
    "tooltip": "{__hex__}"
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


@fused.udf(cache_max_age=0)
def pydeck_hex(df=None, config: dict | str | None = None):
    import pandas as pd
    import pydeck as pdk
    import h3
    import json

    if config is None or config == "":
        config = DEFAULT_H3_CONFIG
    elif isinstance(config, str):
        config = json.loads(config)

    if df is None:
        H3_HEX_DATA = "https://raw.githubusercontent.com/visgl/deck.gl-data/master/website/sf.h3cells.json"
        df = pd.read_json(H3_HEX_DATA)

    hex_field = config.get("hex_field", DEFAULT_H3_CONFIG["hex_field"])
    if hex_field not in df.columns:
        raise ValueError(f"DataFrame must have a '{hex_field}' column")

    df = df.copy()
    df["__hex__"] = df[hex_field]

    if not pd.api.types.is_string_dtype(df["__hex__"]):
        df["__hex__"] = df["__hex__"].apply(
            lambda h: h3.int_to_str(int(h)) if pd.notna(h) else None
        )

    layer = pdk.Layer(
        "H3HexagonLayer",
        df,
        pickable=config.get("pickable", DEFAULT_H3_CONFIG["pickable"]),
        stroked=config.get("stroked", DEFAULT_H3_CONFIG["stroked"]),
        filled=config.get("filled", DEFAULT_H3_CONFIG["filled"]),
        extruded=config.get("extruded", DEFAULT_H3_CONFIG["extruded"]),
        get_hexagon="__hex__",
        get_fill_color=config.get("fill_color", DEFAULT_H3_CONFIG["fill_color"]),
        get_line_color=config.get("line_color", DEFAULT_H3_CONFIG["line_color"]),
        line_width_min_pixels=config.get(
            "line_width_min_pixels",
            DEFAULT_H3_CONFIG["line_width_min_pixels"]
        ),
    )

    view_state = pdk.ViewState(
        latitude=config.get("center_lat", DEFAULT_H3_CONFIG["center_lat"]),
        longitude=config.get("center_lon", DEFAULT_H3_CONFIG["center_lon"]),
        zoom=config.get("zoom", DEFAULT_H3_CONFIG["zoom"]),
        bearing=config.get("bearing", DEFAULT_H3_CONFIG["bearing"]),
        pitch=config.get("pitch", DEFAULT_H3_CONFIG["pitch"]),
    )

    deck = pdk.Deck(
        layers=[layer],
        initial_view_state=view_state,
        tooltip={"text": config.get("tooltip", DEFAULT_H3_CONFIG["tooltip"])},
    )

    return deck.to_html(as_string=True)
