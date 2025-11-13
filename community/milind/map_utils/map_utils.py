import json
import geopandas as gpd
import pandas as pd
import pydeck as pdk
from shapely.geometry import mapping
import folium
import numpy as np
import typing  # added for Union typing
from difflib import get_close_matches
from jinja2 import Template
from copy import deepcopy

DEFAULT_CONFIG = {
    # visual
    "get_fill_color": [0, 144, 255, 200],  # can be array OR JS expr string OR column name
    "get_radius": 200,
    "opacity": 1.0,
    "pickable": True,
    # map camera
    "zoom": 11,
    "pitch": 0,
    "bearing": 0,
    # layer/tooltip
    "tooltip": None,
    "basemap": "https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json",
}

DEFAULT_H3_CONFIG = {
        "hex_field": "hex",
        "fill_color": "[0, 144, 255, 180]",
        "pickable": True,
        "stroked": True,
        "filled": True,
        "extruded": False,
        "line_color": [255, 255, 255],
        "line_width_min_pixels": 0.5,
        "center_lat": None,
        "center_lon": None,
        "zoom": 14,
        "bearing": 0,
        "pitch": 0,
        "tooltip": "{__hex__}"
    }

DEFAULT_POLYGON_CONFIG = {
    "get_fill_color": [0, 144, 255, 120],
    "get_line_color": [255, 255, 255],
    "line_width_min_pixels": 1,
    "pickable": True,
    "stroked": True,
    "filled": True,
    "zoom": 11,
    "pitch": 0,
    "bearing": 0,
    "tooltip": "Polygon {id}",
    "basemap": "https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json",
}
DEFAULT_DECK_HEX_CONFIG = {
    "initialViewState": {
        "longitude": None,
        "latitude": None,
        "zoom": 8,
        "pitch": 0,
        "bearing": 0
  },
  "hexLayer": {
    "@@type": "H3HexagonLayer",
        "filled": True,
        "pickable": True,
        "extruded": False,
    "getHexagon": "@@=properties.hex",
    "getFillColor": {
      "@@function": "colorContinuous",
            "attr": "cnt",
            "domain": [5000, 0],
      "steps": 20,
      "colors": "Magenta"
    }
  } 
}

KNOWN_CARTOCOLOR_PALETTES = {
    "Antique", "ArmyRose", "BluGrn", "BluYl", "Bold", "BrwnYl", "Burg", "BurgYl", 
    "DarkMint", "Earth", "Emrld", "Fall", "Geyser", "Magenta", "Mint", "OrYel", 
    "Pastel", "Peach", "PinkYl", "Prism", "Purp", "PurpOr", "RedOr", "Safe", 
    "Sunset", "SunsetDark", "Teal", "TealGrn", "TealRose", "Temps", "Tropic", 
    "Vivid", "ag_GrnYl", "ag_Sunset", "cb_Accent", "cb_Blues", "cb_BrBG", 
    "cb_BuGn", "cb_BuPu", "cb_Dark2", "cb_GnBu", "cb_Greens", "cb_Greys", 
    "cb_OrRd", "cb_Oranges", "cb_PRGn", "cb_Paired", "cb_Pastel1", "cb_Pastel2", 
    "cb_PiYG", "cb_PuBu", "cb_PuBuGn", "cb_PuOr", "cb_PuRd", "cb_Purples", 
    "cb_RdBu", "cb_RdGy", "cb_RdPu", "cb_RdYlBu", "cb_RdYlGn", "cb_Reds", 
    "cb_Set1", "cb_Set2", "cb_Set3", "cb_Spectral", "cb_YlGn", "cb_YlGnBu", 
    "cb_YlOrBr", "cb_YlOrRd"
}

VALID_HEX_LAYER_PROPS = {
    "@@type",
    "aggregationMode",
    "autoHighlight",
    "beforeId",
    "colorAggregation",
    "colorRange",
    "colorScaleType",
    "coverage",
    "data",
    "elevationAggregation",
    "elevationRange",
    "elevationScale",
    "elevationScaleType",
    "extruded",
    "extensions",
    "filled",
    "filterRange",
    "getColorWeight",
    "getDashArray",
    "getElevation",
    "getElevationWeight",
    "getFillColor",
    "getFilterValue",
    "getHexagon",
    "getLineColor",
    "getLineWidth",
    "getTooltip",
    "gpuAggregation",
    "highlightColor",
    "id",
    "lineWidthMaxPixels",
    "lineWidthMinPixels",
    "lineWidthRange",
    "lineWidthScale",
    "lineWidthUnits",
    "lowerPercentile",
    "material",
    "opacity",
    "parameters",
    "pickable",
    "stroked",
    "tooltipAttrs",
    "tooltipColumns",
    "transitions",
    "updateTriggers",
    "upperPercentile",
    "visible",
    "wireframe",
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
    config: typing.Union[dict, str, None] = None  # changed UnionType to typing.Union
):
    return pydeck_point(gdf, config)


def _compute_center_from_points(df):
    if len(df) == 0:
        return None, None
    return float(df["latitude"].mean()), float(df["longitude"].mean())


def _compute_center_from_hex(df):
    if len(df) == 0:
        return None, None
    if "latitude" in df.columns and "longitude" in df.columns:
        return float(df["latitude"].mean()), float(df["longitude"].mean())
    if "__hex__" in df.columns:
        import h3
        centers = df["__hex__"].dropna().map(lambda h: h3.cell_to_latlng(h))
        if len(centers) == 0:
            return None, None
        lats = [lat for lat, lon in centers]
        lons = [lon for lat, lon in centers]
        return float(sum(lats) / len(lats)), float(sum(lons) / len(lons))
    return None, None


def _compute_center_from_polygons(df):
    if len(df) == 0:
        return None, None
    centroid = df["geometry"].unary_union.centroid
    return float(centroid.y), float(centroid.x)


def pydeck_point(gdf, config=None):
    """
    Pydeck based maps. Use this to render HTML interactive maps from data

    Takes a config dict based on:
    'config = {
        "fill_color": '[255, 100 + cnt, 0]' # dynamically sets the colors of the fill based on the `cnt` values col
    '
    """
    if config is None or config == "":
        cfg = DEFAULT_CONFIG.copy()
    elif isinstance(config, str):
        cfg = DEFAULT_CONFIG.copy()
        cfg.update(json.loads(config))
    else:
        cfg = DEFAULT_CONFIG.copy()
        cfg.update(config)

    if isinstance(gdf, dict):
        gdf = gpd.GeoDataFrame.from_features([gdf])

    df = pd.DataFrame(gdf.drop(columns="geometry"))
    df["longitude"] = gdf.geometry.x
    df["latitude"] = gdf.geometry.y

    auto_lat, auto_lon = _compute_center_from_points(df)

    center_lat = cfg.get("center_lat", auto_lat)
    center_lon = cfg.get("center_lon", auto_lon)

    if center_lat is None or center_lon is None:
        raise ValueError("No valid coordinates to center map (no points in gdf)")

    tooltip_template = cfg.get("tooltip")
    if not tooltip_template:
        cols = [c for c in df.columns if c not in ["longitude", "latitude"]]
        if len(cols) == 0:
            tooltip_template = "lon: {longitude}\nlat: {latitude}"
        else:
            tooltip_template = "\n".join([f"{c}: {{{c}}}" for c in cols])

    fill_accessor = cfg.get("get_fill_color", DEFAULT_CONFIG["get_fill_color"])
    radius_accessor = cfg.get("get_radius", DEFAULT_CONFIG["get_radius"])

    layer = pdk.Layer(
        "ScatterplotLayer",
        data=df,
        get_position=["longitude", "latitude"],
        get_fill_color=fill_accessor,
        get_radius=radius_accessor,
        opacity=cfg.get("opacity", DEFAULT_CONFIG["opacity"]),
        pickable=cfg.get("pickable", DEFAULT_CONFIG["pickable"]),
    )

    view_state = pdk.ViewState(
        latitude=center_lat,
        longitude=center_lon,
        zoom=cfg.get("zoom", DEFAULT_CONFIG["zoom"]),
        pitch=cfg.get("pitch", DEFAULT_CONFIG["pitch"]),
        bearing=cfg.get("bearing", DEFAULT_CONFIG["bearing"]),
    )

    deck = pdk.Deck(
        layers=[layer],
        initial_view_state=view_state,
        map_style=cfg.get("basemap", DEFAULT_CONFIG["basemap"]),
        tooltip={"text": tooltip_template},
    )

    return deck.to_html(as_string=True)


@fused.udf(cache_max_age=0)
def pydeck_hex(df=None, config: typing.Union[dict, str, None] = None):  # changed UnionType
    """
    Pydeck based maps. Use this to render HTML interactive maps from data

    Takes a config dict based on:
    'config = {
        "hex_field": "hex",
        "fill_color": '[255, 100 + cnt, 0]' # dynamically sets the colors of the fill based on the `cnt` values col
    ''
    """
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

    auto_lat, auto_lon = _compute_center_from_hex(df)
    center_lat = config.get("center_lat", None)
    center_lon = config.get("center_lon", None)
    if center_lat is None:
        center_lat = auto_lat
    if center_lon is None:
        center_lon = auto_lon

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
        latitude=center_lat,
        longitude=center_lon,
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


def pydeck_polygon(df, config=None):
    """
    Pydeck based maps. Use this to render HTML interactive maps from data

    Takes a config dict based on:
    'config = {
        "fill_color": '[255, 100 + cnt, 0]' # dynamically sets the colors of the fill based on the `cnt` values col
    '
    """
    if config is None or config == "":
        cfg = DEFAULT_POLYGON_CONFIG.copy()
    elif isinstance(config, str):
        cfg = DEFAULT_POLYGON_CONFIG.copy()
        cfg.update(json.loads(config))
    else:
        cfg = DEFAULT_POLYGON_CONFIG.copy()
        cfg.update(config)

    if "geometry" not in df.columns or len(df) == 0:
        raise ValueError("GeoDataFrame must include a non-empty 'geometry' column")

    auto_lat, auto_lon = _compute_center_from_polygons(df)
    center_lat = cfg.get("center_lat", auto_lat)
    center_lon = cfg.get("center_lon", auto_lon)
    if center_lat is None or center_lon is None:
        center_lat, center_lon = 0.0, 0.0

    df = df.copy()
    df["__polygon__"] = df["geometry"].apply(lambda geom: mapping(geom)["coordinates"])

    tooltip_template = cfg.get("tooltip", DEFAULT_POLYGON_CONFIG["tooltip"])
    if not tooltip_template:
        cols = [c for c in df.columns if c not in ["geometry", "__polygon__"]]
        tooltip_template = "\n".join([f"{c}: {{{c}}}" for c in cols]) if cols else "polygon"

    layer = pdk.Layer(
        "PolygonLayer",
        df,
        get_polygon="__polygon__",
        get_fill_color=cfg.get("get_fill_color", DEFAULT_POLYGON_CONFIG["get_fill_color"]),
        get_line_color=cfg.get("get_line_color", DEFAULT_POLYGON_CONFIG["get_line_color"]),
        line_width_min_pixels=cfg.get("line_width_min_pixels", DEFAULT_POLYGON_CONFIG["line_width_min_pixels"]),
        stroked=cfg.get("stroked", DEFAULT_POLYGON_CONFIG["stroked"]),
        filled=cfg.get("filled", DEFAULT_POLYGON_CONFIG["filled"]),
        pickable=cfg.get("pickable", DEFAULT_POLYGON_CONFIG["pickable"]),
    )

    view_state = pdk.ViewState(
        latitude=center_lat,
        longitude=center_lon,
        zoom=cfg.get("zoom", DEFAULT_POLYGON_CONFIG["zoom"]),
        pitch=cfg.get("pitch", DEFAULT_POLYGON_CONFIG["pitch"]),
        bearing=cfg.get("bearing", DEFAULT_POLYGON_CONFIG["bearing"]),
    )

    deck = pdk.Deck(
        layers=[layer],
        initial_view_state=view_state,
        map_style=cfg.get("basemap", DEFAULT_POLYGON_CONFIG["basemap"]),
        tooltip={"text": tooltip_template},
    )

    return deck.to_html(as_string=True)


def folium_raster(data, bounds, opacity=0.7, tiles="CartoDB dark_matter"):
    """
    Minimal Folium raster overlay utility.
    Works with rasterio arrays (H, W) or (bands, H, W).
    """
    west, south, east, north = bounds

    # shape handling (no normalization)
    if data.ndim == 2:
        rgb = np.stack([data, data, data], axis=-1)
    elif data.ndim == 3:
        if data.shape[0] >= 3:
            rgb = np.transpose(data[:3], (1, 2, 0))
        elif data.shape[0] == 1:
            rgb = np.stack([data[0], data[0], data[0]], axis=-1)
        else:
            h, w = data.shape[1], data.shape[2]
            rgb = np.zeros((h, w, 3), dtype=np.uint8)
            for i in range(data.shape[0]):
                rgb[:, :, i] = data[i]
    else:
        raise ValueError(f"Unsupported raster shape: {data.shape}")

    # clip just to be safe
    rgb = np.clip(rgb, 0, 255).astype(np.uint8)

    # center map
    center_lat = (south + north) / 2
    center_lon = (west + east) / 2

    m = folium.Map(location=[center_lat, center_lon], zoom_start=9, tiles=tiles)

    folium.raster_layers.ImageOverlay(
        image=rgb,
        bounds=[[south, west], [north, east]],
        opacity=opacity,
    ).add_to(m)

    return m.get_root().render()

DEFAULT_DECK_CONFIG = {
    "initialViewState": {
        "zoom": 12
    },
    "vectorLayer": {
        "@@type": "GeoJsonLayer",
        "pointRadiusMinPixels": 10,
        "pickable": True,
        "lineWidthMinPixels": 0,  # Default: no lines unless getLineColor is specified
        "getFillColor": {
            "@@function": "colorContinuous",
            "attr": "house_age",
            "colors": "TealGrn",
            "domain": [0, 50],
            "steps": 7,
            "nullColor": [200, 200, 200, 180]
        },
        "tooltipAttrs": ["house_age", "mrt_distance", "price"]
    }
}


def _deep_merge_dict(base: dict, extra: dict) -> dict:
    for key, value in extra.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            _deep_merge_dict(base[key], value)
        else:
            base[key] = value
    return base


def _load_deckgl_config(raw_config, default_config, component_name: str, errors: list):
    merged = deepcopy(default_config)
    if raw_config in (None, ""):
        return merged

    if isinstance(raw_config, str):
        try:
            user_config = json.loads(raw_config)
        except json.JSONDecodeError as exc:
            errors.append(f"[{component_name}] Failed to parse config JSON: {exc}")
            return merged
    elif isinstance(raw_config, dict):
        user_config = raw_config
    else:
        errors.append(f"[{component_name}] Config must be a dict or JSON string. Using defaults.")
        return merged

    if not isinstance(user_config, dict):
        errors.append(f"[{component_name}] Config must be a dict. Using defaults.")
        return merged

    try:
        merged = _deep_merge_dict(merged, deepcopy(user_config))
    except Exception as exc:
        errors.append(f"[{component_name}] Failed to merge config overrides: {exc}. Using defaults.")
        merged = deepcopy(default_config)
    return merged


def _validate_initial_view_state(config: dict, default_view_state: dict, component_name: str, errors: list):
    view_state = config.get("initialViewState")
    if not isinstance(view_state, dict):
        errors.append(f"[{component_name}] initialViewState must be an object. Using defaults.")
        config["initialViewState"] = deepcopy(default_view_state)
        view_state = config["initialViewState"]
    else:
        zoom = view_state.get("zoom")
        if zoom is not None and not isinstance(zoom, (int, float)):
            errors.append(f"[{component_name}] initialViewState.zoom must be numeric. Using default zoom.")
            view_state["zoom"] = default_view_state.get("zoom")
    return view_state


def _ensure_layer_config(config: dict, layer_key: str, default_layer: dict, component_name: str, errors: list):
    layer = config.get(layer_key)
    if not isinstance(layer, dict):
        errors.append(f"[{component_name}] {layer_key} must be an object. Using defaults.")
        config[layer_key] = deepcopy(default_layer)
        layer = config[layer_key]
    return layer


def _validate_palette_name(color_name: str, component_name: str, field_path: str, errors: list):
    if not color_name:
        return
    palette = color_name.strip()
    if palette and palette not in KNOWN_CARTOCOLOR_PALETTES:
        suggestion = get_close_matches(palette, list(KNOWN_CARTOCOLOR_PALETTES), n=1, cutoff=0.7)
        if suggestion:
            errors.append(f"[{component_name}] {field_path} '{palette}' is not a known CartoColor palette. Did you mean '{suggestion[0]}'?")
        else:
            valid_list = ", ".join(sorted(KNOWN_CARTOCOLOR_PALETTES))
            errors.append(f"[{component_name}] {field_path} '{palette}' is not a recognized CartoColor palette. Valid palettes include: {valid_list}.")


def _validate_color_accessor(
    layer: dict,
    field_name: str,
    *,
    component_name: str,
    errors: list,
    allow_array: bool,
    require_color_continuous: bool,
    fallback_value=None,
):
    value = layer.get(field_name)
    if value is None:
        return

    def _reset_to_fallback():
        if fallback_value is None:
            layer.pop(field_name, None)
        else:
            layer[field_name] = deepcopy(fallback_value)

    if isinstance(value, (list, tuple)):
        if not allow_array:
            errors.append(f"[{component_name}] {field_name} cannot be an array. Removing value.")
            _reset_to_fallback()
            return
        if len(value) not in (3, 4) or not all(isinstance(v, (int, float)) for v in value):
            errors.append(f"[{component_name}] {field_name} array must contain 3 or 4 numeric values. Removing value.")
            _reset_to_fallback()
        return

    if isinstance(value, dict):
        fn = value.get("@@function")
        if require_color_continuous and fn != "colorContinuous":
            errors.append(f"[{component_name}] {field_name}.@@function must be 'colorContinuous'.")
            _reset_to_fallback()
            return

        if fn == "colorContinuous":
            if not isinstance(value.get("attr"), str):
                errors.append(f"[{component_name}] {field_name}.attr must be a string column. Resetting attr.")
                if fallback_value:
                    value["attr"] = fallback_value.get("attr")

            domain = value.get("domain")
            if not (isinstance(domain, (list, tuple)) and len(domain) == 2):
                errors.append(f"[{component_name}] {field_name}.domain must be [min, max]. Resetting domain.")
                if fallback_value:
                    value["domain"] = fallback_value.get("domain")

            steps = value.get("steps")
            if steps is not None and (not isinstance(steps, int) or steps <= 0):
                errors.append(f"[{component_name}] {field_name}.steps must be a positive integer. Resetting steps.")
                if fallback_value:
                    value["steps"] = fallback_value.get("steps")

            colors = value.get("colors")
            if colors is not None:
                if not isinstance(colors, str):
                    errors.append(f"[{component_name}] {field_name}.colors must be a palette string. Resetting colors.")
                    if fallback_value:
                        value["colors"] = fallback_value.get("colors")
                else:
                    _validate_palette_name(colors, component_name, f"{field_name}.colors", errors)
        return

    errors.append(f"[{component_name}] {field_name} must be an array or colorContinuous object. Removing value.")
    _reset_to_fallback()


def _extract_tooltip_columns(config_sources, available_columns=None):
    tooltip_config = None
    for source in config_sources:
        if not isinstance(source, dict):
            continue
        for key in ("tooltipColumns", "tooltip_columns", "tooltipAttrs"):
            candidate = source.get(key)
            if candidate is not None:
                tooltip_config = candidate
                break
        if tooltip_config is not None:
            break

    if tooltip_config is None:
        return []

    if isinstance(tooltip_config, str):
        columns = [tooltip_config.strip()] if tooltip_config.strip() else []
    elif isinstance(tooltip_config, (list, tuple, set)):
        columns = [str(item).strip() for item in tooltip_config if isinstance(item, str) and item.strip()]
    else:
        return []

    if available_columns is not None:
        columns = [col for col in columns if col in available_columns]
    return columns

def deckgl_map(
    gdf,
    config: typing.Union[dict, str, None] = None,
    mapbox_token: str = "pk.eyJ1IjoiaXNhYWNmdXNlZGxhYnMiLCJhIjoiY2xicGdwdHljMHQ1bzN4cWhtNThvbzdqcSJ9.73fb6zHMeO_c8eAXpZVNrA",
):
    """
    Custom DeckGL based HTML Map. Use this to visualize vector data like points & polygons
    Uses a DeckGL compatible config JSON file to edit color palette, starting lat / lon, etc.
    
    Default config:
    {
        "initialViewState": {
            "zoom": 12
        },
        "vectorLayer": {
            "@@type": "GeoJsonLayer",
            "pointRadiusMinPixels": 10,
            "pickable": True,
            "getFillColor": {
                "@@function": "colorContinuous",
                "attr": "house_age",
                "colors": "TealGrn",
                "domain": [0, 50],
                "steps": 7,
                "nullColor": [200, 200, 200, 180]
            },
            "tooltipColumns": ["house_age", "mrt_distance", "price"]
        }
    }
    """
    config_errors = []

    if hasattr(gdf, "crs"):
        try:
            if gdf.crs and getattr(gdf.crs, "to_epsg", lambda: None)() != 4326:
                gdf = gdf.to_crs(epsg=4326)
        except Exception as exc:  # pragma: no cover - best-effort fallback
            print(f"[deckgl_map] Warning: failed to reproject to EPSG:4326 ({exc})")

    try:
        geojson_obj = json.loads(gdf.to_json())
    except Exception:
        geojson_obj = {"type": "FeatureCollection", "features": []}

    auto_center = (0.0, 0.0)
    if hasattr(gdf, "total_bounds"):
        try:
            minx, miny, maxx, maxy = gdf.total_bounds
            auto_center = ((minx + maxx) / 2.0, (miny + maxy) / 2.0)
        except Exception:
            pass

    merged_config = _load_deckgl_config(config, DEFAULT_DECK_CONFIG, "deckgl_map", config_errors)
    initial_view_state = _validate_initial_view_state(
        merged_config,
        DEFAULT_DECK_CONFIG["initialViewState"],
        "deckgl_map",
        config_errors,
    )
    vector_layer = _ensure_layer_config(
        merged_config,
        "vectorLayer",
        DEFAULT_DECK_CONFIG["vectorLayer"],
        "deckgl_map",
        config_errors,
    )

    _validate_color_accessor(
        vector_layer,
        "getFillColor",
        component_name="deckgl_map",
        errors=config_errors,
        allow_array=True,
        require_color_continuous=True,
        fallback_value=DEFAULT_DECK_CONFIG["vectorLayer"]["getFillColor"],
    )
    _validate_color_accessor(
        vector_layer,
        "getLineColor",
        component_name="deckgl_map",
        errors=config_errors,
        allow_array=True,
        require_color_continuous=True,
        fallback_value=None,
    )

    tooltip_attrs = vector_layer.get("tooltipAttrs")
    if tooltip_attrs is not None and not isinstance(tooltip_attrs, (list, tuple)):
        config_errors.append("[deckgl_map] vectorLayer.tooltipAttrs must be an array of column names.")
        vector_layer["tooltipAttrs"] = DEFAULT_DECK_CONFIG["vectorLayer"]["tooltipAttrs"]
    
    # Print config errors to console for debugging
    if config_errors:
        print(f"\n[deckgl_map] Config validation found {len(config_errors)} issue(s):")
        for i, msg in enumerate(config_errors, 1):
            print(f"  {i}. {msg}")
        print()

    auto_state = {
        "longitude": float(auto_center[0]) if auto_center else 0.0,
        "latitude": float(auto_center[1]) if auto_center else 0.0,
        "zoom": initial_view_state.get("zoom", 11),
    }

    # Extract fill color config
    fill_color_config = vector_layer.get("getFillColor", {})
    vector_layer_config = vector_layer.copy()
    data_columns = [col for col in gdf.columns if col not in ("geometry",)]
    tooltip_columns = _extract_tooltip_columns((merged_config, vector_layer), data_columns)

    html = Template(r"""
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="initial-scale=1, maximum-scale=1, user-scalable=no" />
  <link href="https://api.mapbox.com/mapbox-gl-js/v2.15.0/mapbox-gl.css" rel="stylesheet" />
  <script src="https://api.mapbox.com/mapbox-gl-js/v2.15.0/mapbox-gl.js"></script>
  <script src="https://unpkg.com/deck.gl@9.2.2/dist.min.js"></script>
  <script src="https://unpkg.com/@deck.gl/carto@9.2.2/dist.min.js"></script>
  <script type="module">
    import * as cartocolor from 'https://esm.sh/cartocolor@5.0.2';
    window.cartocolor = cartocolor;
  </script>
  <style>
    html, body, #map { margin: 0; height: 100%; width: 100%; background: #000; }
    .mapboxgl-popup-content { background: rgba(0,0,0,0.8); color: #fff; font-family: monospace; font-size: 11px; }
    #tooltip {
      position: absolute;
      pointer-events: none;
      background: rgba(0,0,0,0.75);
      color: #fff;
      padding: 4px 8px;
      border-radius: 4px;
      font-size: 12px;
      line-height: 1.4;
      display: none;
      z-index: 12;
      max-width: 260px;
      box-shadow: 0 2px 6px rgba(0,0,0,0.35);
      white-space: nowrap;
    }
    .config-error {
      position: fixed;
      right: 12px;
      bottom: 12px;
      background: rgba(180, 30, 30, 0.92);
      color: #fff;
      padding: 6px 10px;
      border-radius: 4px;
      font-size: 11px;
      max-width: 260px;
      line-height: 1.4;
      display: none;
      z-index: 10;
      box-shadow: 0 2px 6px rgba(0,0,0,0.35);
    }
    .color-legend {
      position: fixed;
      left: 12px;
      bottom: 12px;
      background: rgba(15, 15, 15, 0.9);
      color: #fff;
      padding: 8px 10px;
      border-radius: 4px;
      font-size: 11px;
      z-index: 10;
      box-shadow: 0 2px 6px rgba(0,0,0,0.35);
      border: 1px solid rgba(255,255,255,0.1);
      min-width: 140px;
      display: none;
    }
    .color-legend .legend-title {
      margin-bottom: 6px;
      font-weight: 500;
      color: #fff;
    }
    .color-legend .legend-gradient {
      width: 100%;
      height: 12px;
      border-radius: 2px;
      margin-bottom: 4px;
      border: 1px solid rgba(255,255,255,0.2);
    }
    .color-legend .legend-labels {
      display: flex;
      justify-content: space-between;
      font-size: 10px;
      color: #ccc;
    }
  </style>
</head>
<body>
<div id="map"></div>
<div id="tooltip"></div>
<div id="config-error" class="config-error"></div>
<div id="color-legend" class="color-legend">
  <div class="legend-title"></div>
  <div class="legend-gradient"></div>
  <div class="legend-labels">
    <span class="legend-min"></span>
    <span class="legend-max"></span>
  </div>
</div>
<script>
const MAPBOX_TOKEN = {{ mapbox_token | tojson }};
const GEOJSON = {{ geojson_obj | tojson }};
const AUTO_STATE = {{ auto_state | tojson }};
const FILL_COLOR_CONFIG = {{ fill_color_config | tojson }};
const VECTOR_LAYER_CONFIG = {{ vector_layer_config | tojson }};
const TOOLTIP_COLUMNS = {{ tooltip_columns | tojson }};
const CONFIG_ERROR = {{ config_error | tojson }};

const { MapboxOverlay } = deck;
const { GeoJsonLayer } = deck;
const { colorContinuous } = deck.carto;

let configErrors = [];
if (CONFIG_ERROR) {
  configErrors = configErrors.concat(
    Array.isArray(CONFIG_ERROR)
      ? CONFIG_ERROR
      : String(CONFIG_ERROR)
          .split(" • ")
          .map(s => s.trim())
          .filter(Boolean)
  );
}

// Calculate bounds from all geometry types
let initialBounds = null;
if (GEOJSON.features && GEOJSON.features.length > 0) {
  try {
    const bounds = new mapboxgl.LngLatBounds();
    
    function extendBoundsFromCoordinates(coords, depth = 0) {
      if (depth === 0 && typeof coords[0] === 'number') {
        // Single coordinate [lng, lat]
        bounds.extend(coords);
      } else if (Array.isArray(coords)) {
        coords.forEach(c => extendBoundsFromCoordinates(c, depth + 1));
      }
    }
    
    GEOJSON.features.forEach(feature => {
      if (!feature.geometry) return;
      
      const geom = feature.geometry;
      switch (geom.type) {
        case 'Point':
          bounds.extend(geom.coordinates);
          break;
        case 'MultiPoint':
        case 'LineString':
          geom.coordinates.forEach(coord => bounds.extend(coord));
          break;
        case 'MultiLineString':
        case 'Polygon':
          geom.coordinates.forEach(ring => {
            ring.forEach(coord => bounds.extend(coord));
          });
          break;
        case 'MultiPolygon':
          geom.coordinates.forEach(poly => {
            poly.forEach(ring => {
              ring.forEach(coord => bounds.extend(coord));
            });
          });
          break;
        case 'GeometryCollection':
          // Recursively handle geometry collections
          geom.geometries.forEach(g => {
            extendBoundsFromCoordinates(g.coordinates);
          });
          break;
      }
    });
    
    // Only use bounds if we successfully extended it
    if (!bounds.isEmpty()) {
      initialBounds = bounds;
    }
  } catch (err) {
    console.warn('[deckgl_map] Error calculating bounds:', err);
  }
}

mapboxgl.accessToken = MAPBOX_TOKEN;

const map = new mapboxgl.Map({
  container: 'map',
  style: 'mapbox://styles/mapbox/dark-v10',
  center: [AUTO_STATE.longitude, AUTO_STATE.latitude],
  zoom: AUTO_STATE.zoom
});

// Process colorContinuous config
function processColorContinuous(cfg) {
  let domain = cfg.domain;
  if (domain && domain.length === 2) {
    const [min, max] = domain;
    const steps = cfg.steps ?? 20;
    const stepSize = (max - min) / (steps - 1);
    domain = Array.from({ length: steps }, (_, i) => min + stepSize * i);
  }
  return {
    attr: cfg.attr,
    domain: domain,
    colors: cfg.colors || 'TealGrn',
    nullColor: cfg.nullColor || [184,184,184]
  };
}

// Process layer config outside map.on('load') so it's accessible everywhere
let getFillColor = [0, 144, 255, 200];
let getRadius = VECTOR_LAYER_CONFIG?.getRadius || 200;
let pointRadiusMinPixels = VECTOR_LAYER_CONFIG?.pointRadiusMinPixels || 10;
let lineWidthMinPixels = VECTOR_LAYER_CONFIG?.lineWidthMinPixels ?? 0;
let getLineColor = VECTOR_LAYER_CONFIG?.getLineColor || [0, 0, 0, 0]; // Transparent by default

if (FILL_COLOR_CONFIG && FILL_COLOR_CONFIG['@@function'] === 'colorContinuous') {
  try {
    getFillColor = colorContinuous(processColorContinuous(FILL_COLOR_CONFIG));
  } catch (err) {
    console.warn('Failed to create colorContinuous function:', err);
    configErrors.push('Failed to apply colorContinuous config. Using default colors.');
  }
}

function createGeoJsonLayer(data) {
  return new GeoJsonLayer({
    id: 'gdf-layer',
    data,
    pickable: true,
    stroked: true,
    filled: true,
    lineWidthMinPixels: lineWidthMinPixels,
    getFillColor: getFillColor,
    getLineColor: getLineColor,
    getRadius: getRadius,
    pointRadiusMinPixels: pointRadiusMinPixels,
    opacity: 0.8
  });
}

const overlay = new MapboxOverlay({
  interleaved: false,
  layers: [createGeoJsonLayer({ type: 'FeatureCollection', features: [] })],
  glOptions: {
    preserveDrawingBuffer: true  // Prevents WebGL context loss when iframe loses focus
  }
});

map.addControl(overlay);

requestAnimationFrame(() => {
  overlay.setProps({
    layers: [createGeoJsonLayer(GEOJSON)]
  });
});

map.on('load', () => {
  if (initialBounds && !initialBounds.isEmpty()) {
    try {
      map.fitBounds(initialBounds, {
        padding: 50,
        maxZoom: 15,
        duration: 0
      });
    } catch (err) {
      console.warn('[deckgl_map] fitBounds failed:', err);
    }
  }

  const tooltipEl = document.getElementById('tooltip');

  function buildTooltipLines(props) {
    const keys = Array.isArray(TOOLTIP_COLUMNS) && TOOLTIP_COLUMNS.length
      ? TOOLTIP_COLUMNS
      : Object.keys(props || {});
    const lines = [];
    keys.forEach(key => {
      if (props && props[key] !== undefined) {
        const val = props[key];
        let display = val;
        if (typeof val === 'number' && Number.isFinite(val)) {
          display = val.toFixed(2);
        }
        lines.push(`${key}: ${String(display)}`);
      }
    });
    return lines;
  }

  map.on('mousemove', (e) => {
    if (!overlay) return;
    const info = overlay.pickObject({ x: e.point.x, y: e.point.y, radius: 4 });
    if (info?.object) {
      map.getCanvas().style.cursor = 'pointer';
      const props = info.object.properties || info.object;
      const lines = buildTooltipLines(props);
      if (!lines.length) {
        tooltipEl.style.display = 'none';
        return;
      }
      tooltipEl.innerHTML = lines.join(' • ');
      tooltipEl.style.left = `${e.point.x + 10}px`;
      tooltipEl.style.top = `${e.point.y + 10}px`;
      tooltipEl.style.display = 'block';
    } else {
      map.getCanvas().style.cursor = '';
      tooltipEl.style.display = 'none';
    }
  });

  map.on('mouseleave', () => {
    map.getCanvas().style.cursor = '';
    tooltipEl.style.display = 'none';
  });

  // Generate color legend if colorContinuous config exists
  function generateColorLegend() {
    const cfg = FILL_COLOR_CONFIG;
    if (!cfg || cfg['@@function'] !== 'colorContinuous') {
      console.log('[legend] No colorContinuous config found');
      return;
    }
    
    const attr = cfg.attr || 'value';
    const domain = cfg.domain || [0, 1];
    const steps = cfg.steps || 20;
    const colors = cfg.colors || 'TealGrn';
    
    if (!window.cartocolor) {
      console.log('[legend] Waiting for cartocolor to load...');
      setTimeout(generateColorLegend, 100);
      return;
    }
    
    try {
      const palette = window.cartocolor[colors];
      if (!palette) {
        console.warn('[legend] Palette not found:', colors);
        return;
      }
      
      // Cartocolor palettes are objects with numbered keys (3, 4, 5, etc.)
      // Find the closest available step count
      let colorsArray = null;
      if (palette[steps]) {
        colorsArray = palette[steps];
      } else {
        // Find the highest available step count <= steps, or the max available
        const availableSteps = Object.keys(palette).map(Number).filter(n => !isNaN(n)).sort((a, b) => b - a);
        const closestStep = availableSteps.find(s => s <= steps) || availableSteps[availableSteps.length - 1];
        colorsArray = palette[closestStep];
      }
      
      if (!colorsArray || !Array.isArray(colorsArray)) {
        console.warn('[legend] Invalid color array for palette:', colors);
        return;
      }
      
      const gradient = colorsArray.map((c, i) => {
        const pct = (i / (colorsArray.length - 1)) * 100;
        return `${c} ${pct}%`;
      }).join(', ');
      
      const legendEl = document.getElementById('color-legend');
      if (legendEl) {
        legendEl.querySelector('.legend-title').textContent = attr;
        legendEl.querySelector('.legend-gradient').style.background = `linear-gradient(to right, ${gradient})`;
        legendEl.querySelector('.legend-min').textContent = domain[0].toFixed(1);
        legendEl.querySelector('.legend-max').textContent = domain[1].toFixed(1);
        legendEl.style.display = 'block';
        console.log('[legend] Legend generated successfully');
      } else {
        console.warn('[legend] Legend element not found');
      }
    } catch (err) {
      console.warn('[legend] Failed to generate color legend:', err);
    }
  }
  
  // Wait for cartocolor to load
  if (window.cartocolor) {
    generateColorLegend();
  } else {
    setTimeout(generateColorLegend, 500);
  }
});

const errorBox = document.getElementById('config-error');
if (errorBox && configErrors.length) {
  errorBox.innerHTML = configErrors.map(msg => `<div>${msg}</div>`).join('');
  errorBox.style.display = 'block';
}
</script>
</body>
</html>
""").render(
        mapbox_token=mapbox_token,
        geojson_obj=geojson_obj,
        auto_state=auto_state,
        fill_color_config=fill_color_config,
        vector_layer_config=vector_layer_config,
        tooltip_columns=tooltip_columns,
        config_error=config_errors,
    )

    common = fused.load("https://github.com/fusedio/udfs/tree/351515e/public/common/")
    return common.html_to_obj(html)



def deckgl_hex(
    df,
    config = None,  # Can be dict, JSON string, or None
    mapbox_token: str = "pk.eyJ1IjoiaXNhYWNmdXNlZGxhYnMiLCJhIjoiY2xicGdwdHljMHQ1bzN4cWhtNThvbzdqcSJ9.73fb6zHMeO_c8eAXpZVNrA",
):
    """
    Custom DeckGL based HTML Map. Use this to visualize hex data (dataframe containing a hex column)
    Uses a DeckGL compatible config JSON file to edit color palette, starting lat / lon, tooltip columns, etc.
    
    Default config:
    {
        "initialViewState": {
            "longitude": None,
            "latitude": None,
            "zoom": 8,
            "pitch": 0,
            "bearing": 0
        },
        "hexLayer": {
            "@@type": "H3HexagonLayer",
            "filled": True,
            "pickable": True,
            "extruded": False,
            "getHexagon": "@@=properties.hex",
            "getFillColor": {
                "@@function": "colorContinuous",
                "attr": "cnt",
                "domain": [5000, 0],
                "steps": 20,
                "colors": "Magenta"
            }
        }
    }
    """
    config_errors = []

    config = _load_deckgl_config(config, DEFAULT_DECK_HEX_CONFIG, "deckgl_hex", config_errors)
    _validate_initial_view_state(config, DEFAULT_DECK_HEX_CONFIG["initialViewState"], "deckgl_hex", config_errors)
    hex_layer = _ensure_layer_config(
        config,
        "hexLayer",
        DEFAULT_DECK_HEX_CONFIG["hexLayer"],
        "deckgl_hex",
        config_errors,
    )

    if not isinstance(hex_layer.get("getHexagon"), str) or not hex_layer.get("getHexagon"):
        config_errors.append("[deckgl_hex] hexLayer.getHexagon must be a string expression (e.g. '@@=properties.hex'). Falling back to default accessor.")
        hex_layer["getHexagon"] = DEFAULT_DECK_HEX_CONFIG["hexLayer"]["getHexagon"]

    _validate_color_accessor(
        hex_layer,
        "getFillColor",
        component_name="deckgl_hex",
        errors=config_errors,
        allow_array=False,
        require_color_continuous=True,
        fallback_value=DEFAULT_DECK_HEX_CONFIG["hexLayer"]["getFillColor"],
    )
    _validate_color_accessor(
        hex_layer,
        "getLineColor",
        component_name="deckgl_hex",
        errors=config_errors,
        allow_array=False,
        require_color_continuous=True,
        fallback_value=None,
    )

    invalid_props = [key for key in list(hex_layer.keys()) if key not in VALID_HEX_LAYER_PROPS]
    for prop in invalid_props:
        suggestion = get_close_matches(prop, list(VALID_HEX_LAYER_PROPS), n=1, cutoff=0.7)
        if suggestion:
            config_errors.append(
                f"[deckgl_hex] hexLayer property '{prop}' is not recognized. Did you mean '{suggestion[0]}'?"
            )
        else:
            valid_props_preview = ", ".join(sorted(list(VALID_HEX_LAYER_PROPS))[:8])
            config_errors.append(
                f"[deckgl_hex] hexLayer property '{prop}' is not recognized by Deck.GL. Sample valid properties: {valid_props_preview}, ..."
            )
        hex_layer.pop(prop, None)

    configured_tooltip_columns = _extract_tooltip_columns((config, hex_layer))
    tooltip_columns = []

    if config_errors:
        print(f"\n[deckgl_hex] Config validation found {len(config_errors)} issue(s):")
        for i, msg in enumerate(config_errors, 1):
            print(f"  {i}. {msg}")
        print()

    config_error_messages = config_errors
    
    # Convert dataframe to list of records
    if hasattr(df, 'to_dict'):
        # Handle both pandas and geopandas dataframes
        data_records = df.to_dict('records')
        
        # Convert hex IDs to hex strings to avoid precision loss in JavaScript
        conversion_count = 0
        for i, record in enumerate(data_records):
            hex_val = record.get('hex') or record.get('h3') or record.get('index') or record.get('id')
            if hex_val is not None:
                try:
                    # Convert integer to hex string
                    if isinstance(hex_val, (int, float)):
                        hex_int = int(hex_val)
                        hex_str = format(hex_int, 'x')  # Convert to hex string
                        record['hex'] = hex_str
                        conversion_count += 1
                        # Debug: print first conversion
                        if i == 0:
                            print(f"[deckgl_hex] Converted hex: {hex_int} -> {hex_str}")
                    elif isinstance(hex_val, str) and hex_val.isdigit():
                        # String that looks like a number
                        hex_int = int(hex_val)
                        hex_str = format(hex_int, 'x')
                        record['hex'] = hex_str
                        conversion_count += 1
                    else:
                        # Already a hex string
                        record['hex'] = hex_val
                        if i == 0:
                            print(f"[deckgl_hex] Hex already string: {hex_val}")
                except (ValueError, OverflowError) as e:
                    print(f"Error converting hex: {hex_val}, {e}")
                    record['hex'] = None
        
        if conversion_count > 0:
            print(f"[deckgl_hex] Converted {conversion_count} hex IDs from int to hex string")
    else:
        data_records = []
    
    # Auto-calculate center from lat/lng columns if available
    auto_center = (-119.4179, 36.7783)  # Default to California
    auto_zoom = 5
    
    if len(data_records) > 0:
        available_keys = set(data_records[0].keys())
        if configured_tooltip_columns:
            missing_tooltips = [col for col in configured_tooltip_columns if col not in available_keys]
            if missing_tooltips:
                print(f"[deckgl_hex] Warning: tooltip columns not found in data: {missing_tooltips}")
            tooltip_columns = [col for col in configured_tooltip_columns if col in available_keys]
        else:
            tooltip_columns = []

        if 'lat' in data_records[0] and 'lng' in data_records[0]:
            lats = [r['lat'] for r in data_records if 'lat' in r]
            lngs = [r['lng'] for r in data_records if 'lng' in r]
            if lats and lngs:
                auto_center = (sum(lngs)/len(lngs), sum(lats)/len(lats))
                auto_zoom = 8
    else:
        tooltip_columns = []

    # Get initialViewState from config, use auto-calculated values as fallback
    initial_view_state = config.get('initialViewState', {})
    center_lng = initial_view_state.get('longitude')
    center_lat = initial_view_state.get('latitude')
    zoom = initial_view_state.get('zoom')
    pitch = initial_view_state.get('pitch', 0)
    bearing = initial_view_state.get('bearing', 0)
    
    # Use auto values if not specified in config
    if center_lng is None:
        center_lng = auto_center[0]
    if center_lat is None:
        center_lat = auto_center[1]
    if zoom is None:
        zoom = auto_zoom

    # Infer tooltip columns from data
    if not tooltip_columns and data_records:
        tooltip_columns = [k for k in data_records[0].keys() if k not in ['hex', 'lat', 'lng']]

    html = Template(r"""
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8" />
  <title>H3 Hexagon Viewer</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />

  <!-- Mapbox GL -->
  <link href="https://api.mapbox.com/mapbox-gl-js/v2.15.0/mapbox-gl.css" rel="stylesheet" />
  <script src="https://api.mapbox.com/mapbox-gl-js/v2.15.0/mapbox-gl.js"></script>

  <!-- Load h3-js FIRST, then deck.gl + geo-layers (+ carto for color ramps) -->
  <script src="https://unpkg.com/h3-js@4.1.0/dist/h3-js.umd.js"></script>
  <script src="https://unpkg.com/deck.gl@9.2.2/dist.min.js"></script>
  <script src="https://unpkg.com/@deck.gl/geo-layers@9.2.2/dist.min.js"></script>
  <script src="https://unpkg.com/@deck.gl/carto@9.2.2/dist.min.js"></script>
  <script type="module">
    import * as cartocolor from 'https://esm.sh/cartocolor@5.0.2';
    window.cartocolor = cartocolor;
  </script>

  <style>
    html, body, #map { margin: 0; height: 100%; width: 100%; }
    #tooltip {
      position: absolute;
      pointer-events: none;
      background: rgba(0,0,0,0.7);
      color: #fff;
      padding: 4px 8px;
      border-radius: 4px;
      font-size: 12px;
      display: none;
      z-index: 6;
    }
    #config-error {
      position: fixed;
      right: 14px;
      bottom: 14px;
      background: rgba(180, 30, 30, 0.92);
      color: #fff;
      padding: 6px 10px;
      border-radius: 4px;
      font-size: 11px;
      max-width: 260px;
      line-height: 1.4;
      display: none;
      z-index: 8;
      box-shadow: 0 2px 6px rgba(0,0,0,0.35);
    }
    .color-legend {
      position: fixed;
      left: 12px;
      bottom: 12px;
      background: rgba(15, 15, 15, 0.9);
      color: #fff;
      padding: 8px 10px;
      border-radius: 4px;
      font-size: 11px;
      z-index: 10;
      box-shadow: 0 2px 6px rgba(0,0,0,0.35);
      border: 1px solid rgba(255,255,255,0.1);
      min-width: 140px;
      display: none;
    }
    .color-legend .legend-title {
      margin-bottom: 6px;
      font-weight: 500;
      color: #fff;
    }
    .color-legend .legend-gradient {
      width: 100%;
      height: 12px;
      border-radius: 2px;
      margin-bottom: 4px;
      border: 1px solid rgba(255,255,255,0.2);
    }
    .color-legend .legend-labels {
      display: flex;
      justify-content: space-between;
      font-size: 10px;
      color: #ccc;
    }
  </style>
</head>
<body>
  <div id="map"></div>
  <div id="tooltip"></div>
  <div id="config-error"></div>
  <div id="color-legend" class="color-legend">
    <div class="legend-title"></div>
    <div class="legend-gradient"></div>
    <div class="legend-labels">
      <span class="legend-min"></span>
      <span class="legend-max"></span>
    </div>
  </div>

  <script>
    const MAPBOX_TOKEN = {{ mapbox_token | tojson }};
    const STYLE_URL = "mapbox://styles/mapbox/dark-v10";
    const DATA = {{ data_records | tojson }};
    const CONFIG = {{ config | tojson }};
    const TOOLTIP_COLUMNS = {{ tooltip_columns | tojson }};
    const DEFAULT_FILL_CONFIG = {{ default_fill_config | tojson }};
    const CONFIG_ERROR = {{ config_error | tojson }};

    const { MapboxOverlay } = deck;
    const H3HexagonLayer = deck.H3HexagonLayer || (deck.GeoLayers && deck.GeoLayers.H3HexagonLayer);
    const { colorContinuous } = deck.carto;

    const $tooltip = () => document.getElementById('tooltip');

    // ----- H3 ID safety (handles string/number/bigint) -----
    function toH3String(hex) {
      try {
        if (hex == null) return null;
        if (typeof hex === 'string') {
          // Remove 0x prefix if present
          const s = hex.startsWith('0x') ? hex.slice(2) : hex;
          // If it's already a hex string (contains a-f), just return lowercase
          if (/[a-f]/i.test(s)) {
            return s.toLowerCase();
          }
          // If all digits, might be decimal string - convert to hex
          if (/^\d+$/.test(s)) {
            return BigInt(s).toString(16);
          }
          // Otherwise return as-is
          return s.toLowerCase();
        }
        // Fallback for numbers (shouldn't happen with pre-conversion)
        if (typeof hex === 'number') return BigInt(Math.trunc(hex)).toString(16);
        if (typeof hex === 'bigint') return hex.toString(16);
      } catch(e) {
        console.error('toH3String error:', hex, e);
      }
      return null;
    }

    // Process color continuous config
    function processColorContinuous(cfg) {
      let domain = cfg.domain;
      if (domain && domain.length === 2) {
        const [min, max] = domain;
        const steps = cfg.steps ?? 20;
        const stepSize = (max - min) / (steps - 1);
        domain = Array.from({ length: steps }, (_, i) => min + stepSize * i);
      }

      return {
        attr: cfg.attr,
        domain: domain,
        colors: cfg.colors || 'Magenta',
        nullColor: cfg.nullColor || [184,184,184]
      };
    }

    // Parse hex layer config
    let configErrors = [];
    if (CONFIG_ERROR) {
      configErrors = configErrors.concat(
        Array.isArray(CONFIG_ERROR)
          ? CONFIG_ERROR
          : String(CONFIG_ERROR)
              .split(" • ")
              .map(s => s.trim())
              .filter(Boolean)
      );
    }

    function parseHexLayerConfig(config) {
      const out = {};
      for (const [k, v] of Object.entries(config || {})) {
        if (k === '@@type') continue;
        if (v && typeof v === 'object' && !Array.isArray(v)) {
          if (v['@@function'] === 'colorContinuous') {
            try {
              out[k] = colorContinuous(processColorContinuous(v));
            } catch (err) {
              console.error('colorContinuous parse error:', err);
              configErrors.push(`hexLayer.${k}: ${err?.message || 'Invalid color configuration (check palette name)'}`);
              try {
                out[k] = colorContinuous(processColorContinuous(DEFAULT_FILL_CONFIG));
              } catch {
                out[k] = colorContinuous(processColorContinuous({
                  attr: DEFAULT_FILL_CONFIG.attr || 'cnt',
                  domain: DEFAULT_FILL_CONFIG.domain || [0, 1],
                  colors: DEFAULT_FILL_CONFIG.colors || 'Magenta',
                  nullColor: DEFAULT_FILL_CONFIG.nullColor || [184, 184, 184]
                }));
              }
            }
          } else {
            out[k] = v;
          }
        } else if (typeof v === 'string' && v.startsWith('@@=')) {
          // Handle @@= expressions
          const code = v.slice(3);
          out[k] = (obj) => {
            try {
              const properties = obj?.properties || obj || {};
              return eval(code);
            } catch (e) { 
              console.error('@@= eval error:', v, e); 
              return null; 
            }
          };
        } else {
          out[k] = v;
        }
      }
      return out;
    }

    // Normalize data - convert hex IDs to H3 strings
    const normalizedData = DATA.map((d, i) => {
      const hexRaw = d.hex ?? d.h3 ?? d.index ?? d.id;
      const hex = toH3String(hexRaw);
      if (!hex) {
        if (i < 3) console.warn('Null hex for record:', d);
      return null;
    }
      return { ...d, hex, properties: { ...d, hex } };
      }).filter(Boolean);

    console.log('Loaded hexagons:', normalizedData.length);
    if (normalizedData.length > 0) {
      console.log('Sample hex (raw):', DATA[0].hex);
      console.log('Sample hex (converted):', normalizedData[0].hex);
      console.log('Sample record:', normalizedData[0]);
      // Verify with h3-js
      if (typeof h3 !== 'undefined' && h3.isValidCell) {
        console.log('Is valid H3 cell?', h3.isValidCell(normalizedData[0].hex));
      }
    }

    // Mapbox init
    mapboxgl.accessToken = MAPBOX_TOKEN;
    const map = new mapboxgl.Map({ 
      container: 'map', 
      style: STYLE_URL, 
      center: [{{ center_lng }}, {{ center_lat }}], 
      zoom: {{ zoom }} 
    });

    function createHexLayer(data) {
      return new H3HexagonLayer({
        id: 'h3-hexagon-layer',
        data,
        pickable: true,
        wireframe: false,
        filled: true,
        extruded: false,
        coverage: 0.9,
        getHexagon: d => d.hex,
        ...parseHexLayerConfig(CONFIG.hexLayer || {})
      });
    }

    const overlay = new MapboxOverlay({
      interleaved: false,
      layers: [createHexLayer([])],
      glOptions: {
        preserveDrawingBuffer: true  // Prevents WebGL context loss when iframe loses focus
      }
    });

    map.addControl(overlay);

    requestAnimationFrame(() => {
      overlay.setProps({
        layers: [createHexLayer(normalizedData)]
      });
    });

    // Tooltip on hover
    map.on('mousemove', (e) => { 
      const info = overlay.pickObject({x: e.point.x, y: e.point.y, radius: 4});
      if (info?.object) {
        map.getCanvas().style.cursor = 'pointer';
        const p = info.object;
        const lines = [];
        if (!TOOLTIP_COLUMNS.length && p.hex) {
          lines.push(`hex: ${p.hex.substring(0, 10)}...`);
        }
        TOOLTIP_COLUMNS.forEach(col => {
          if (p[col] !== undefined) {
            const val = p[col];
            let display = val;
            if (typeof val === 'number' && Number.isFinite(val)) {
              display = val.toFixed(2);
            }
            lines.push(`${col}: ${String(display)}`);
          }
        });

        const tt = $tooltip();
        if (!lines.length) {
          tt.style.display = 'none';
          return;
        }

        const tooltipText = lines.join(' • ');
        tt.innerHTML = tooltipText;
        tt.style.left = `${e.point.x + 10}px`;
        tt.style.top = `${e.point.y + 10}px`;
        tt.style.display = 'block';
      } else {
        map.getCanvas().style.cursor = '';
        $tooltip().style.display = 'none';
      }
    });

    // Generate color legend if colorContinuous config exists
    function generateColorLegend() {
      const hexLayer = CONFIG.hexLayer || {};
      // Check getFillColor first, then getLineColor
      let colorCfg = hexLayer.getFillColor;
      if (!colorCfg || colorCfg['@@function'] !== 'colorContinuous') {
        colorCfg = hexLayer.getLineColor;
        if (!colorCfg || colorCfg['@@function'] !== 'colorContinuous') {
          console.log('[legend] No colorContinuous config found in getFillColor or getLineColor');
          return;
        }
      }
      
      const attr = colorCfg.attr || 'cnt';
      const domain = colorCfg.domain || [0, 1];
      const steps = colorCfg.steps || 20;
      const colors = colorCfg.colors || 'Magenta';
      
      if (!window.cartocolor) {
        console.log('[legend] Waiting for cartocolor to load...');
        setTimeout(generateColorLegend, 100);
        return;
      }
      
      try {
        const palette = window.cartocolor[colors];
        if (!palette) {
          console.warn('[legend] Palette not found:', colors);
          return;
        }
        
        // Cartocolor palettes are objects with numbered keys (3, 4, 5, etc.)
        // Find the closest available step count
        let colorsArray = null;
        if (palette[steps]) {
          colorsArray = palette[steps];
        } else {
          // Find the highest available step count <= steps, or the max available
          const availableSteps = Object.keys(palette).map(Number).filter(n => !isNaN(n)).sort((a, b) => b - a);
          const closestStep = availableSteps.find(s => s <= steps) || availableSteps[availableSteps.length - 1];
          colorsArray = palette[closestStep];
        }
        
        if (!colorsArray || !Array.isArray(colorsArray)) {
          console.warn('[legend] Invalid color array for palette:', colors);
          return;
        }
        
        const gradient = colorsArray.map((c, i) => {
          const pct = (i / (colorsArray.length - 1)) * 100;
          return `${c} ${pct}%`;
        }).join(', ');
        
        const legendEl = document.getElementById('color-legend');
        if (legendEl) {
          legendEl.querySelector('.legend-title').textContent = attr;
          legendEl.querySelector('.legend-gradient').style.background = `linear-gradient(to right, ${gradient})`;
          legendEl.querySelector('.legend-min').textContent = domain[0].toFixed(1);
          legendEl.querySelector('.legend-max').textContent = domain[1].toFixed(1);
          legendEl.style.display = 'block';
          console.log('[legend] Legend generated successfully');
        } else {
          console.warn('[legend] Legend element not found');
        }
      } catch (err) {
        console.warn('[legend] Failed to generate color legend:', err);
      }
    }
    
    // Wait for cartocolor to load
    if (window.cartocolor) {
      generateColorLegend();
    } else {
      setTimeout(generateColorLegend, 500);
    }

    if (configErrors.length) {
      const box = document.getElementById('config-error');
      if (box) {
        box.innerHTML = configErrors.map(msg => `<div>${msg}</div>`).join('');
        box.style.display = 'block';
      }
    }
  </script>
</body>
</html>
""").render(
        mapbox_token=mapbox_token,
        data_records=data_records,
        config=config,
        tooltip_columns=tooltip_columns,
        center_lng=center_lng,
        center_lat=center_lat,
        zoom=zoom,
        config_error=config_error_messages,
        default_fill_config=DEFAULT_DECK_HEX_CONFIG["hexLayer"]["getFillColor"],
    )

    common = fused.load("https://github.com/fusedio/udfs/tree/f430c25/public/common/")
    return common.html_to_obj(html)


def deckgl_raster(
    image_data,  # numpy array or image URL string
    bounds,  # [west, south, east, north]
    config: dict = None,
    mapbox_token: str = "pk.eyJ1IjoiaXNhYWNmdXNlZGxhYnMiLCJhIjoiY2xicGdwdHljMHQ1bzN4cWhtNThvbzdqcSJ9.73fb6zHMeO_c8eAXpZVNrA",
):
    """
    Render a georeferenced raster image using Deck.gl BitmapLayer.
    
    Args:
        image_data: numpy array (H, W, 3 or 4) or image URL string
        bounds: [west, south, east, north] geographic bounds
        config: Optional dict with initialViewState and rasterLayer config
        mapbox_token: Mapbox access token
    
    Returns:
        HTML object for rendering
    """
    from jinja2 import Template
    import json
    import numpy as np
    import base64
    from io import BytesIO
    from copy import deepcopy
    
    # Default config
    DEFAULT_RASTER_CONFIG = {
        "initialViewState": {
            "longitude": None,
            "latitude": None,
            "zoom": 10,
            "pitch": 0,
            "bearing": 0
        },
        "rasterLayer": {
            "opacity": 1.0,
            "tintColor": [255, 255, 255]
        }
    }
    
    if config is None or config == "":
        config = deepcopy(DEFAULT_RASTER_CONFIG)
    elif isinstance(config, str):
        config = json.loads(config)
    else:
        # Merge with defaults
        merged = deepcopy(DEFAULT_RASTER_CONFIG)
        merged.update(config)
        config = merged
    
    # Convert numpy array to base64 image data URL
    image_url = None
    if isinstance(image_data, str):
        # Already a URL
        image_url = image_data
    else:
        # Convert numpy array to PNG base64
        try:
            from PIL import Image
            
            # Handle different array formats
            if image_data.ndim == 3:
                # Check if it's (C, H, W) format (rasterio/GDAL convention)
                if image_data.shape[0] in [1, 3, 4] and image_data.shape[0] < image_data.shape[1] and image_data.shape[0] < image_data.shape[2]:
                    # Transpose from (C, H, W) to (H, W, C)
                    image_data = np.transpose(image_data, (1, 2, 0))
                    print(f"[deckgl_raster] Transposed from (C,H,W) to (H,W,C): {image_data.shape}")
            
            # Ensure it's uint8
            if image_data.dtype != np.uint8:
                # Normalize to 0-255
                img_min, img_max = image_data.min(), image_data.max()
                if img_max > img_min:
                    image_data = ((image_data - img_min) / (img_max - img_min) * 255).astype(np.uint8)
                else:
                    image_data = np.zeros_like(image_data, dtype=np.uint8)
            
            # Handle different shapes
            if image_data.ndim == 2:
                # Grayscale - convert to RGB
                img = Image.fromarray(image_data, mode='L').convert('RGB')
            elif image_data.ndim == 3:
                if image_data.shape[2] == 1:
                    # Single channel - convert to grayscale then RGB
                    img = Image.fromarray(image_data[:, :, 0], mode='L').convert('RGB')
                elif image_data.shape[2] == 3:
                    img = Image.fromarray(image_data, mode='RGB')
                elif image_data.shape[2] == 4:
                    img = Image.fromarray(image_data, mode='RGBA')
                else:
                    raise ValueError(f"Unsupported number of channels: {image_data.shape[2]}. Expected 1, 3, or 4. Shape: {image_data.shape}")
            else:
                raise ValueError(f"Unsupported image dimensionality: {image_data.ndim}D. Expected 2D or 3D array. Shape: {image_data.shape}")
            
            # Convert to base64
            buffer = BytesIO()
            img.save(buffer, format='PNG')
            img_base64 = base64.b64encode(buffer.getvalue()).decode('utf-8')
            image_url = f"data:image/png;base64,{img_base64}"
            
        except Exception as e:
            print(f"[deckgl_raster] Error converting image: {e}")
            raise
    
    # Calculate center from bounds
    west, south, east, north = bounds
    auto_center_lng = (west + east) / 2
    auto_center_lat = (south + north) / 2
    
    # Get initialViewState from config
    initial_view_state = config.get('initialViewState', {})
    center_lng = initial_view_state.get('longitude')
    center_lat = initial_view_state.get('latitude')
    zoom = initial_view_state.get('zoom')
    pitch = initial_view_state.get('pitch', 0)
    bearing = initial_view_state.get('bearing', 0)
    
    # Use auto values if not specified
    if center_lng is None:
        center_lng = auto_center_lng
    if center_lat is None:
        center_lat = auto_center_lat
    if zoom is None:
        zoom = 10
    
    # Get raster layer config
    raster_config = config.get('rasterLayer', {})
    opacity = raster_config.get('opacity', 1.0)
    tint_color = raster_config.get('tintColor', [255, 255, 255])
    
    html = Template(r"""
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="initial-scale=1,maximum-scale=1,user-scalable=no"/>
  <link href="https://api.mapbox.com/mapbox-gl-js/v3.2.0/mapbox-gl.css" rel="stylesheet"/>
  <script src="https://api.mapbox.com/mapbox-gl-js/v3.2.0/mapbox-gl.js"></script>
  <script src="https://unpkg.com/deck.gl@9.2.2/dist.min.js"></script>
  <style>
    html,body{margin:0;height:100%;background:#000}
    #map{position:absolute;inset:0}
  </style>
</head>
<body>
<div id="map"></div>
<script>
const MAPBOX_TOKEN = {{ mapbox_token | tojson }};
const IMAGE_URL = {{ image_url | tojson }};
const BOUNDS = {{ bounds | tojson }};
const OPACITY = {{ opacity | tojson }};
const TINT_COLOR = {{ tint_color | tojson }};

mapboxgl.accessToken = MAPBOX_TOKEN;
const map = new mapboxgl.Map({
  container: 'map',
  style: 'mapbox://styles/mapbox/dark-v10',
  center: [{{ center_lng }}, {{ center_lat }}],
  zoom: {{ zoom }},
  pitch: {{ pitch }},
  bearing: {{ bearing }}
});

const layer = new deck.BitmapLayer({
  id: 'raster-layer',
  bounds: BOUNDS,
  image: IMAGE_URL,
  opacity: OPACITY,
  tintColor: TINT_COLOR,
  pickable: false
});

const overlay = new deck.MapboxOverlay({
  interleaved: false,
  layers: [layer]
});

map.addControl(overlay);

// Fit map to raster bounds on load
map.on('load', () => {
  const [west, south, east, north] = BOUNDS;
  map.fitBounds(
    [[west, south], [east, north]],
    {
      padding: 20,
      pitch: {{ pitch }},
      bearing: {{ bearing }},
      duration: 1000
    }
  );
});
</script>
</body>
</html>
""").render(
        mapbox_token=mapbox_token,
        image_url=image_url,
        bounds=bounds,
        opacity=opacity,
        tint_color=tint_color,
        center_lng=center_lng,
        center_lat=center_lat,
        zoom=zoom,
        pitch=pitch,
        bearing=bearing,
    )

    common = fused.load("https://github.com/fusedio/udfs/tree/f430c25/public/common/")
    return common.html_to_obj(html)