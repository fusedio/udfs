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
    Mapbox-GL-native implementation with robust native Popup tooltips and recovery.
    Diagnostic message for "no features at cursor" has been removed.
    """
    config_errors = []

    if hasattr(gdf, "crs"):
        try:
            if gdf.crs and getattr(gdf.crs, "to_epsg", lambda: None)() != 4326:
                gdf = gdf.to_crs(epsg=4326)
        except Exception as exc:
            print(f"[deckgl_map] Warning: failed to reproject to EPSG:4326 ({exc})")

    import math
    import numpy as _np

    try:
        geojson_obj = json.loads(gdf.to_json())
    except Exception:
        geojson_obj = {"type": "FeatureCollection", "features": []}

    # --- SANITIZE PROPERTIES: convert non-serializable values to plain strings/numbers ---
    def sanitize_value(v):
        # simple primitives
        if isinstance(v, (float, int, str, bool)) or v is None:
            if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                return None
            return v
        # numpy scalars
        if isinstance(v, (_np.floating, _np.integer, _np.bool_)):
            try:
                return v.item()
            except Exception:
                return str(v)
        # lists/tuples/sets -> comma-joined string
        if isinstance(v, (list, tuple, set)):
            try:
                return ", ".join(str(s) for s in v)
            except Exception:
                return str(v)
        # dicts or other objects -> string fallback
        try:
            return str(v)
        except Exception:
            return None

    if isinstance(geojson_obj, dict) and "features" in geojson_obj:
        for feat in geojson_obj["features"]:
            props = feat.get("properties") or {}
            new_props = {}
            if isinstance(props, dict):
                for k, val in props.items():
                    new_props[k] = sanitize_value(val)
            feat["properties"] = new_props

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

    tooltip_attrs = vector_layer.get("tooltipAttrs")
    if tooltip_attrs is not None and not isinstance(tooltip_attrs, (list, tuple)):
        config_errors.append("[deckgl_map] vectorLayer.tooltipAttrs must be an array of column names.")
        vector_layer["tooltipAttrs"] = DEFAULT_DECK_CONFIG["vectorLayer"]["tooltipAttrs"]
    
    # Only use fill color config if user explicitly provided it (not from defaults)
    # Check the original config to see if user specified getFillColor
    user_specified_fill_color = False
    if isinstance(config, dict):
        user_vector_layer = config.get("vectorLayer", {})
        if isinstance(user_vector_layer, dict) and "getFillColor" in user_vector_layer:
            user_specified_fill_color = True
    
    fill_color_config = vector_layer.get("getFillColor", {}) or {} if user_specified_fill_color else {}
    color_attr = fill_color_config.get("attr", None) if isinstance(fill_color_config, dict) and user_specified_fill_color else None
    
    # Extract line color configuration
    line_color_value = vector_layer.get("getLineColor")
    line_color_rgba = None
    if isinstance(line_color_value, (list, tuple)) and len(line_color_value) >= 3:
        # Convert [r, g, b, a] to rgba string
        r, g, b = int(line_color_value[0]), int(line_color_value[1]), int(line_color_value[2])
        a = line_color_value[3] / 255.0 if len(line_color_value) > 3 else 1.0
        line_color_rgba = f"rgba({r},{g},{b},{a})"
    
    # Extract line width and point radius from vector layer config
    line_width = vector_layer.get("lineWidthMinPixels", 3)
    # Support both pointRadius and pointRadiusMinPixels for consistency with deck.gl naming
    point_radius = vector_layer.get("pointRadiusMinPixels")
    if point_radius is None:
        point_radius = vector_layer.get("pointRadius", 6)

    auto_state = {
        "longitude": float(auto_center[0]) if auto_center else 0.0,
        "latitude": float(auto_center[1]) if auto_center else 0.0,
        "zoom": initial_view_state.get("zoom", 11),
    }

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
  <script type="module">
    import * as cartocolor from 'https://esm.sh/cartocolor@5.0.2';
    window.cartocolor = cartocolor;
  </script>
  <style>
    html, body, #map { margin:0; height:100%; width:100%; background:#000; }
    .mapboxgl-popup-content { font-family: monospace; font-size: 12px; background: rgba(0,0,0,0.85); color:#fff; padding:6px 8px; border-radius:6px; }
    .config-error { position:fixed; right:12px; bottom:12px; background:rgba(180,30,30,0.92); color:#fff; padding:6px 10px; border-radius:4px; font-size:11px; display:none; z-index:30; }
    .color-legend { position:fixed; left:12px; bottom:12px; background:rgba(15,15,15,0.9); color:#fff; padding:8px; border-radius:4px; font-size:11px; display:none; z-index:30; min-width:140px; border:1px solid rgba(255,255,255,0.08); }
    .color-legend .legend-gradient { height:12px; border-radius:3px; border:1px solid rgba(255,255,255,0.06); margin-bottom:6px; }
    .color-legend .legend-labels { display:flex; justify-content:space-between; color:#ccc; font-size:10px;}
  </style>
</head>
<body>
<div id="map"></div>
<div id="config-error" class="config-error"></div>
<div id="color-legend" class="color-legend"><div class="legend-title"></div><div class="legend-gradient"></div><div class="legend-labels"><span class="legend-min"></span><span class="legend-max"></span></div></div>

<script>
const MAPBOX_TOKEN = {{ mapbox_token | tojson }};
const GEOJSON = {{ geojson_obj | tojson }};
const AUTO_STATE = {{ auto_state | tojson }};
const FILL_COLOR_CONFIG = {{ fill_color_config | tojson }};
const COLOR_ATTR = {{ color_attr | tojson }};
const TOOLTIP_COLUMNS = {{ tooltip_columns | tojson }};
const CONFIG_ERROR = {{ config_error | tojson }};
const LINE_WIDTH = {{ line_width | tojson }};
const POINT_RADIUS = {{ point_radius | tojson }};
const LINE_COLOR = {{ line_color_rgba | tojson }};

mapboxgl.accessToken = MAPBOX_TOKEN;
const map = new mapboxgl.Map({
  container: 'map',
  style: 'mapbox://styles/mapbox/dark-v10',
  center: [AUTO_STATE.longitude, AUTO_STATE.latitude],
  zoom: AUTO_STATE.zoom
});

// Build color stops (returns {stops: [[v,color],...], domain: [min,max]} or null)
function makeColorStops(cfg) {
  if (!cfg || cfg['@@function'] !== 'colorContinuous' || !cfg.domain || cfg.domain.length !== 2) {
    return null;
  }
  const domain = cfg.domain;
  const steps = cfg.steps || 7;
  const paletteName = cfg.colors || 'TealGrn';
  const palette = (window.cartocolor && window.cartocolor[paletteName]) || null;
  let colorsArray;
  if (palette) {
    const available = Object.keys(palette).map(Number).filter(n=>!isNaN(n)).sort((a,b)=>a-b);
    const best = available.find(n => n >= steps) || available[available.length-1];
    colorsArray = palette[best];
  } else {
    colorsArray = [];
    for (let i=0;i<steps;i++){
      const t = i/(steps-1);
      const r = Math.round(30 + (0-30)*t);
      const g = Math.round(120 + (200-120)*t);
      const b = Math.round(220 + (150-220)*t);
      colorsArray.push(`rgb(${r},${g},${b})`);
    }
  }
  const stops = [];
  for (let i=0;i<colorsArray.length;i++){
    const v = domain[0] + ((domain[1]-domain[0]) * (i/(colorsArray.length-1)));
    stops.push([v, colorsArray[i]]);
  }
  return {stops, domain};
}

// Create expression array for Mapbox interpolate: ['interpolate',['linear'],['get', attr], v1,c1, v2,c2, ...]
function makeInterpolateExpression(attr, colorSpec) {
  if (!colorSpec || !attr) return null;
  const expr = ['interpolate', ['linear'], ['get', attr]];
  colorSpec.stops.forEach(s => {
    expr.push(s[0]);
    expr.push(s[1]);
  });
  return expr;
}

  function removeLayerIfExists(id) {
    if (!map || typeof map.getLayer !== 'function') return;
    try {
      const layer = map.getLayer(id);
      if (layer) {
        map.removeLayer(id);
      }
    } catch(e) {
      console.warn('[deckgl_map] Failed to remove layer', id, e);
    }
  }

  function ensureSource() {
    if (!map || typeof map.getSource !== 'function' || typeof map.addSource !== 'function') {
      console.warn('[deckgl_map] Map not ready for source operations');
      return null;
    }
    
    const existing = map.getSource('gdf-source');
    if (!existing) {
      try {
        map.addSource('gdf-source', { type: 'geojson', data: GEOJSON });
        return map.getSource('gdf-source');
      } catch(e) {
        console.warn('[deckgl_map] Failed to add source', e);
        return null;
      }
    }
    try {
      if (typeof existing.setData === 'function') {
        existing.setData(GEOJSON);
      }
      return existing;
  } catch (err) {
      console.warn('[deckgl_map] Source update failed, recreating source', err);
      try { 
        if (typeof map.removeSource === 'function') {
          map.removeSource('gdf-source'); 
        }
      } catch(e) {}
      try {
        map.addSource('gdf-source', { type: 'geojson', data: GEOJSON });
        return map.getSource('gdf-source');
      } catch(e) {
        console.warn('[deckgl_map] Failed to recreate source', e);
        return null;
      }
    }
  }

  function addGeoJsonSourceAndLayers() {
    if (!map || typeof map.addLayer !== 'function') {
      console.warn('[deckgl_map] Map not ready for layer operations');
      return;
    }
    
    removeLayerIfExists('gdf-fill');
    removeLayerIfExists('gdf-line');
    removeLayerIfExists('gdf-line-only');
    removeLayerIfExists('gdf-circle');

    const source = ensureSource();
    if (!source) {
      console.warn('[deckgl_map] Failed to ensure source, skipping layer creation');
      return;
    }

    let hasPolygon=false, hasPoint=false, hasLine=false;
  for (const f of (GEOJSON.features || [])) {
    const t = f.geometry && f.geometry.type;
    if (!t) continue;
    if (t === 'Point' || t === 'MultiPoint') hasPoint = true;
    if (t === 'Polygon' || t === 'MultiPolygon') hasPolygon = true;
      if (t === 'LineString' || t === 'MultiLineString') hasLine = true;
      if (hasPoint && hasPolygon && hasLine) break;
  }

  const colorSpec = makeColorStops(FILL_COLOR_CONFIG);
  const colorExpr = makeInterpolateExpression(COLOR_ATTR, colorSpec);

  if (hasPolygon) {
    map.addLayer({
      id: 'gdf-fill',
      type: 'fill',
      source: 'gdf-source',
      paint: {
        'fill-color': colorExpr || 'rgba(0,144,255,0.6)',
        'fill-opacity': 0.8
      },
      filter: ['any',
        ['==', ['geometry-type'], 'Polygon'],
        ['==', ['geometry-type'], 'MultiPolygon']
      ]
    });

    map.addLayer({
      id: 'gdf-line',
      type: 'line',
      source: 'gdf-source',
      paint: {
        'line-color': 'rgba(0,0,0,0.5)',
        'line-width': 1
      },
      filter: ['any',
        ['==', ['geometry-type'], 'Polygon'],
        ['==', ['geometry-type'], 'MultiPolygon']
      ]
    });
  }

  if (hasLine) {
    map.addLayer({
      id: 'gdf-line-only',
      type: 'line',
      source: 'gdf-source',
      paint: {
        'line-color': LINE_COLOR || colorExpr || 'rgba(255,0,100,0.9)',
        'line-width': LINE_WIDTH || 3,
        'line-opacity': 1.0
      },
      filter: ['any',
        ['==', ['geometry-type'], 'LineString'],
        ['==', ['geometry-type'], 'MultiLineString']
      ]
    });
  }

  if (hasPoint) {
    const radius = POINT_RADIUS || 6;
    // For very small circles, reduce or remove stroke to make size difference visible
    const strokeWidth = radius < 2 ? 0 : 1;
    
    map.addLayer({
      id: 'gdf-circle',
      type: 'circle',
      source: 'gdf-source',
      paint: {
        'circle-radius': radius,
        'circle-color': colorExpr || 'rgba(0,144,255,0.9)',
        'circle-stroke-color': 'rgba(0,0,0,0.5)',
        'circle-stroke-width': strokeWidth,
        'circle-opacity': 0.9
      },
      filter: ['any',
        ['==', ['geometry-type'], 'Point'],
        ['==', ['geometry-type'], 'MultiPoint']
      ]
    });
  }
}

map.on('load', () => {
  addGeoJsonSourceAndLayers();

  // fit bounds if turf available
  try {
    if (window.turf) {
      const bbox = turf.bbox(GEOJSON);
      if (bbox && bbox.length === 4) {
        map.fitBounds([[bbox[0], bbox[1]],[bbox[2], bbox[3]]], { padding: 40, maxZoom: 15, duration: 500 });
      }
    }
  } catch(e){}

  // legend - only show if we have a continuous color scale with an attribute
  const colorSpec = makeColorStops(FILL_COLOR_CONFIG);
  if (colorSpec && COLOR_ATTR) {
    const legend = document.getElementById('color-legend');
    legend.querySelector('.legend-title').textContent = COLOR_ATTR || 'value';
    const gEl = legend.querySelector('.legend-gradient');
    const cs = colorSpec.stops.map((s,i)=> {
      const pct = Math.round((i/(colorSpec.stops.length-1))*100);
      return `${s[1]} ${pct}%`;
    }).join(', ');
    gEl.style.background = `linear-gradient(to right, ${cs})`;
    legend.querySelector('.legend-min').textContent = Number(colorSpec.domain[0]).toFixed(1);
    legend.querySelector('.legend-max').textContent = Number(colorSpec.domain[1]).toFixed(1);
    legend.style.display = 'block';
  }
});

// --- NATIVE MAPBOX POPUP TOOLTIP (robust, recovers from missing layers) ---
let hoverPopup = null;
let lastFeatureKey = null;

function safeStringify(val) {
  if (val === null || val === undefined) return '';
  if (typeof val === 'number') {
    if (!isFinite(val)) return '';
    return Number(val).toFixed(2);
  }
  if (typeof val === 'string') return val;
  try {
    if (Array.isArray(val)) return val.join(', ');
    return String(val);
  } catch(e) {
    return '';
  }
}

function makeTooltipHTML(props) {
  const keys = Array.isArray(TOOLTIP_COLUMNS) && TOOLTIP_COLUMNS.length ? TOOLTIP_COLUMNS : Object.keys(props || {});
  const lines = [];
  keys.forEach(k => {
    if (props && Object.prototype.hasOwnProperty.call(props, k) && props[k] !== null && props[k] !== undefined && String(props[k]) !== 'null') {
      const v = safeStringify(props[k]);
      if (v !== '') lines.push(`${k}: ${v}`);
    }
  });
  return lines.join(' • ');
}

function getActiveLayerIds() {
  const layers = [];
  if (!map || typeof map.getLayer !== 'function') return layers;
  try {
    if (map.getLayer('gdf-circle')) layers.push('gdf-circle');
    if (map.getLayer('gdf-fill')) layers.push('gdf-fill');
    if (map.getLayer('gdf-line')) layers.push('gdf-line');
    if (map.getLayer('gdf-line-only')) layers.push('gdf-line-only');
  } catch(e) {
    console.warn('[deckgl_map] Error checking active layers', e);
  }
  return layers;
}

map.on('mousemove', (e) => {
  try {
    const activeLayers = getActiveLayerIds();
    if (activeLayers.length === 0) return;

    let features = null;
    try {
      features = map.queryRenderedFeatures(e.point, { layers: activeLayers });
    } catch (qerr) {
      console.warn('[deckgl_map] queryRenderedFeatures failed, attempting recovery:', qerr && qerr.message ? qerr.message : qerr);
      try {
        // try to re-add source/layers if a layer was missing
        addGeoJsonSourceAndLayers();
        // bail out for this event; next mousemove should find layers
      return;
      } catch (readdErr) {
        console.warn('[deckgl_map] addGeoJsonSourceAndLayers recovery also failed:', readdErr);
      }
      // final fallback: query without layers
      try {
        features = map.queryRenderedFeatures(e.point);
      } catch (qerr2) {
        console.warn('[deckgl_map] fallback queryRenderedFeatures also failed:', qerr2);
        return;
      }
    }

    if (features && features.length) {
      const f = features[0];
      const key = f.id !== undefined ? f.id : JSON.stringify(f.properties || {});
      if (key === lastFeatureKey && hoverPopup) {
        try { hoverPopup.setLngLat(e.lngLat); } catch(e){}
        map.getCanvas().style.cursor = 'pointer';
        return;
      }
      lastFeatureKey = key;

      const html = makeTooltipHTML(f.properties || {});
      if (!html) {
        if (hoverPopup) { hoverPopup.remove(); hoverPopup = null; lastFeatureKey = null; }
        map.getCanvas().style.cursor = '';
        return;
      }
      if (!hoverPopup) {
        hoverPopup = new mapboxgl.Popup({ closeButton: false, closeOnClick: false, offset: 10, className: 'gdf-hover-popup' });
      }
      hoverPopup.setLngLat(e.lngLat).setHTML(html).addTo(map);
      map.getCanvas().style.cursor = 'pointer';
      } else {
      if (hoverPopup) { hoverPopup.remove(); hoverPopup = null; }
      lastFeatureKey = null;
      map.getCanvas().style.cursor = '';
      }
    } catch (err) {
    console.warn('[deckgl_map] tooltip mousemove handler error', err);
  }
});

map.on('mouseleave', () => {
  if (hoverPopup) { hoverPopup.remove(); hoverPopup = null; }
  lastFeatureKey = null;
  map.getCanvas().style.cursor = '';
});

// lightweight recovery/resize handlers
const container = document.getElementById('map');
const resizeObserver = new ResizeObserver(() => { try { map.resize(); } catch(e) {} });
resizeObserver.observe(container);
document.addEventListener('visibilitychange', () => { if (!document.hidden) { try { map.resize(); } catch(e) {} } });
window.addEventListener('focus', () => { try { map.triggerRepaint && map.triggerRepaint(); } catch(e) {} });
window.addEventListener('focusin', () => { try { map.triggerRepaint && map.triggerRepaint(); } catch(e) {} });

// styledata recovery
map.on('styledata', () => {
  try {
    if (!map || typeof map.getSource !== 'function' || typeof map.getLayer !== 'function') {
      console.warn('[deckgl_map] Map not ready in styledata handler');
      return;
    }
    const hasSource = map.getSource('gdf-source');
    const hasFill = map.getLayer('gdf-fill');
    const hasCircle = map.getLayer('gdf-circle');
    const hasLine = map.getLayer('gdf-line-only');
    
    if (!hasSource || (!hasFill && !hasCircle && !hasLine)) {
      addGeoJsonSourceAndLayers();
    } else if (hasSource && typeof hasSource.setData === 'function') {
      hasSource.setData(GEOJSON);
    }
  } catch (err) {
    console.warn('[deckgl_map] styledata recovery failed', err);
  }
});

const errorBox = document.getElementById('config-error');
if (errorBox && CONFIG_ERROR && CONFIG_ERROR.length) {
  errorBox.innerHTML = CONFIG_ERROR.map(m => `<div>${m}</div>`).join('');
  errorBox.style.display = 'block';
}
</script>

<!-- turf for bbox (optional) -->
<script src="https://cdn.jsdelivr.net/npm/@turf/turf@6.5.0/turf.min.js"></script>
</body>
</html>
    """).render(
        mapbox_token=mapbox_token,
        geojson_obj=geojson_obj,
        auto_state=auto_state,
        fill_color_config=fill_color_config,
        color_attr=color_attr,
        tooltip_columns=tooltip_columns,
        config_error=config_errors,
        line_width=line_width,
        point_radius=point_radius,
        line_color_rgba=line_color_rgba,
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
    
    # Save original config to check what user actually provided
    original_config = config

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
    # Check the original config parameter to see if user provided getFillColor
    user_provided_fill_color = False
    if isinstance(original_config, dict):
        user_hex_layer = original_config.get("hexLayer", {})
        if isinstance(user_hex_layer, dict) and "getFillColor" in user_hex_layer:
            user_provided_fill_color = True
    
    # If user didn't provide getFillColor, remove it from merged config to prevent wrong defaults
    if not user_provided_fill_color and "getFillColor" in hex_layer:
        hex_layer.pop("getFillColor", None)
        print("[deckgl_hex] Removed default getFillColor since user didn't specify it")

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

    // Calculate and fit bounds from H3 hexagons
    map.on('load', () => {
      if (normalizedData.length > 0 && typeof h3 !== 'undefined' && h3.cellToBoundary) {
        try {
          const bounds = new mapboxgl.LngLatBounds();
          
          // Sample up to 100 hexagons to calculate bounds (for performance)
          const sampleSize = Math.min(100, normalizedData.length);
          const step = Math.max(1, Math.floor(normalizedData.length / sampleSize));
          
          for (let i = 0; i < normalizedData.length; i += step) {
            const hex = normalizedData[i].hex;
            if (hex && h3.isValidCell(hex)) {
              const boundary = h3.cellToBoundary(hex);
              boundary.forEach(([lat, lng]) => {
                bounds.extend([lng, lat]);
              });
            }
          }
          
          if (!bounds.isEmpty()) {
            requestAnimationFrame(() => {
              map.fitBounds(bounds, {
                padding: 50,
                maxZoom: 15,
                duration: 500
              });
            });
            console.log('[deckgl_hex] Fitted map to hexagon bounds');
          }
        } catch (err) {
          console.warn('[deckgl_hex] Failed to fit bounds:', err);
        }
      }
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
      
      // Use values from config, no fallbacks - if missing, don't show legend
      const attr = colorCfg.attr;
      const domain = colorCfg.domain;
      const steps = colorCfg.steps || 7;
      const colors = colorCfg.colors || 'TealGrn';
      
      if (!attr || !domain || !Array.isArray(domain) || domain.length !== 2) {
        console.log('[legend] Invalid color config - missing attr or domain');
        return;
      }
      
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


def enable_map_sync(html_input, channel: str = "default"):
    """
    Inject BroadcastChannel-based map sync into any Mapbox GL map HTML.
    
    Args:
        html_string: HTML string containing a Mapbox GL map
        channel: Sync channel name (maps with same channel will sync)
    
    Returns:
        Modified HTML with sync capability
    
    Usage:
        html = deckgl_hex(df, config)
        map1 = enable_map_sync(html, channel="channel1")
        return map1
    """
    channel = channel if "::" in channel else f"map-sync::{channel}"

    # Normalize input to HTML string
    response_mode = not isinstance(html_input, str)
    if isinstance(html_input, str):
        html_string = html_input
    else:
        html_string = None
        text_value = getattr(html_input, "text", None)
        if isinstance(text_value, str):
            html_string = text_value
        else:
            data_value = getattr(html_input, "data", None)
            if isinstance(data_value, (bytes, bytearray)):
                html_string = data_value.decode("utf-8", errors="ignore")
            elif isinstance(data_value, str):
                html_string = data_value
            else:
                body_value = getattr(html_input, "body", None)
                if isinstance(body_value, (bytes, bytearray)):
                    html_string = body_value.decode("utf-8", errors="ignore")
                elif isinstance(body_value, str):
                    html_string = body_value
        if html_string is None:
            raise TypeError(
                "html_input must be a str or response-like object with 'text', 'data', or 'body' attribute"
            )
    
    sync_script = f"""
<script>
(function() {{
  if (!window.mapboxgl || !map) return;
  
  const CHANNEL = {json.dumps(channel)};
  const bc = new BroadcastChannel(CHANNEL);
  const mapId = Math.random().toString(36).substr(2, 9);
  
  let isSyncing = false;
  let userInteracting = false;
  let lastBroadcast = {{x: NaN, y: NaN, z: NaN}};
  
  const eq = (a, b, e) => Math.abs(a - b) < e;
  
  function getState() {{
    const c = map.getCenter();
    return {{
      center: [+c.lng.toFixed(6), +c.lat.toFixed(6)],
      zoom: +map.getZoom().toFixed(3),
      bearing: +map.getBearing().toFixed(1),
      pitch: +map.getPitch().toFixed(1),
      id: mapId,
      ts: Date.now()
    }};
  }}
  
  function broadcast() {{
    if (isSyncing) return;
    const state = getState();
    const [x, y] = state.center;
    const z = state.zoom;
    
    if (eq(x, lastBroadcast.x, 1e-6) && 
        eq(y, lastBroadcast.y, 1e-6) && 
        eq(z, lastBroadcast.z, 1e-3)) return;
    
    lastBroadcast = {{x, y, z}};
    bc.postMessage(state);
  }}
  
  function applySync(state) {{
    if (!state || state.id === mapId) return;
    if (isSyncing || userInteracting) return;
    
    isSyncing = true;
    map.jumpTo({{
      center: state.center,
      zoom: state.zoom,
      bearing: state.bearing,
      pitch: state.pitch,
      animate: false
    }});
    
    requestAnimationFrame(() => {{ isSyncing = false; }});
  }}
  
  let moveTimeout = null;
  function scheduleBroadcast() {{
    if (moveTimeout) clearTimeout(moveTimeout);
    moveTimeout = setTimeout(broadcast, 0);
  }}
  
  const startInteraction = () => {{
    userInteracting = true;
  }};
  const finishInteraction = () => {{
    if (!userInteracting) return;
    userInteracting = false;
    broadcast();
  }};
  
  map.on('dragstart', startInteraction);
  map.on('zoomstart', startInteraction);
  map.on('rotatestart', startInteraction);
  map.on('pitchstart', startInteraction);
  map.on('moveend', finishInteraction);
  
  map.on('move', () => {{
    if (userInteracting && !isSyncing) scheduleBroadcast();
  }});
  
  bc.onmessage = (e) => applySync(e.data);
  
  map.on('load', () => {{
    setTimeout(() => bc.postMessage(getState()), 100 + Math.random() * 100);
  }});
  
  document.addEventListener('visibilitychange', () => {{
    if (document.hidden) {{
      isSyncing = false;
      userInteracting = false;
    }}
  }});
}})();
</script>
"""
    
    # Inject before closing </body> tag
    if "</body>" in html_string:
        injected_html = html_string.replace("</body>", f"{sync_script}\n</body>")
    elif "</html>" in html_string:
        injected_html = html_string.replace("</html>", f"{sync_script}\n</html>")
    else:
        injected_html = html_string + sync_script

    if response_mode:
        common = fused.load("https://github.com/fusedio/udfs/tree/f430c25/public/common/")
        return common.html_to_obj(injected_html)
    return injected_html