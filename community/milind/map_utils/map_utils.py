import json
import geopandas as gpd
import pandas as pd
import pydeck as pdk
from shapely.geometry import mapping
import folium
import numpy as np
import typing  # added for Union typing
from jinja2 import Template
from copy import deepcopy

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
        "stroked": True,
        "pickable": True,
        "extruded": False,
        "opacity": 1,
    "getHexagon": "@@=properties.hex",
    "getFillColor": {
      "@@function": "colorContinuous",
            "attr": "cnt",
      "steps": 20,
      "colors": "ArmyRose",
      "nullColor": [184, 184, 184]
    },
        "getLineColor": [255, 255, 255],
        "lineWidthMinPixels": 1
  } 
}

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
            "colors": "ArmyRose",
            "domain": [0, 50],
            "steps": 7,
            "nullColor": [200, 200, 200, 180]
        },
        "tooltipAttrs": ["house_age", "mrt_distance", "price"]
  } 
}

KNOWN_CARTOCOLOR_PALETTES = {
    "ArmyRose","Antique", "BluGrn", "BluYl", "Bold", "BrwnYl", "Burg", "BurgYl", 
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
    "sql",
    "stroked",
    "tooltipAttrs",
    "tooltipColumns",
    "transitions",
    "upperPercentile",
    "visible",
    "wireframe",
}

@fused.udf(cache_max_age=0)
def udf(
    config: typing.Union[dict, str, None] = None
):
    """Example UDF using deckgl_map with DEFAULT_DECK_CONFIG."""
    import geopandas as gpd
    import pandas as pd
    from shapely.geometry import Point
    import random
    
    # Create sample point data around New York City
    # Fields match DEFAULT_DECK_CONFIG expectations
    data = []
    base_lat, base_lng = 40.7128, -74.0060
    
    for i in range(50):
        lat = base_lat + (i % 10 - 5) * 0.01
        lng = base_lng + (i // 10 - 2) * 0.01
        data.append({
            'geometry': Point(lng, lat),
            'house_age': (i % 50),  # 0-49 years, matches domain [0, 50]
            'mrt_distance': random.randint(100, 5000),  # meters
            'price': random.randint(200000, 800000),  # dollars
            'index': i
        })
    
    gdf = gpd.GeoDataFrame(data, crs='EPSG:4326')
    
    # Use DEFAULT_DECK_CONFIG if none provided (for points/vectors)
    if config is None:
        config = DEFAULT_DECK_CONFIG
    
    # Ensure the map centers on New York City
    if config is None:
        config = {}
    # Set initial view state to NYC coordinates if not already specified
    view_state = config.get("initialViewState", {})
    view_state.setdefault("longitude", -74.0060)
    view_state.setdefault("latitude", 40.7128)
    view_state.setdefault("zoom", 12)
    config["initialViewState"] = view_state

    layers = [{"type": "vector", "data": gdf, "config": config, "name": "Sample Points"}]
    return deckgl_layers(layers=layers, debug=True)



def deckgl_hex(
    df=None,
    config=None,
    mapbox_token: str = "pk.eyJ1IjoiaXNhYWNmdXNlZGxhYnMiLCJhIjoiY2xicGdwdHljMHQ1bzN4cWhtNThvbzdqcSJ9.73fb6zHMeO_c8eAXpZVNrA",
    basemap: str = "dark",
    tile_url: str = None,
    layers: list = None,
    highlight_on_click: bool = True,
    on_click: dict = None,
    debug: bool = False,
):

    if layers is None:
        if df is not None:
            layers = [{"type": "hex", "data": df, "config": config, "name": "Layer 1"}]
        elif tile_url is not None:
            layers = [{"type": "hex", "tile_url": tile_url, "config": config, "name": "Tile Layer"}]
        else:
            raise ValueError("Provide df, tile_url, or layers parameter")
    else:
        # Ensure all layers have type="hex"
        layers = [{"type": "hex", **layer_def} for layer_def in layers]
    
    # Use unified deckgl_layers implementation
    return deckgl_layers(
        layers=layers,
        mapbox_token=mapbox_token,
        basemap=basemap,
        highlight_on_click=highlight_on_click,
        on_click=on_click,
        debug=debug,
    )


def deckgl_layers(
    layers: list,
    mapbox_token: str = "pk.eyJ1IjoiaXNhYWNmdXNlZGxhYnMiLCJhIjoiY2xicGdwdHljMHQ1bzN4cWhtNThvbzdqcSJ9.73fb6zHMeO_c8eAXpZVNrA",
    basemap: str = "dark",
    highlight_on_click: bool = True,
    on_click: dict = None,
    debug: bool = False,
):
    """
    Render mixed hex and vector layers on a single interactive map.
    
    This function combines the capabilities of deckgl_hex and deckgl_map,
    allowing you to render H3 hexagons and GeoJSON vectors together.
    
    Args:
        layers: List of layer dicts, each with:
            - "type": "hex" or "vector" (required)
            - "data": DataFrame with hex column (for hex) or GeoDataFrame (for vector)
            - "tile_url": XYZ tile URL template (for hex tile layers, use instead of data)
            - "config": Layer config dict (hexLayer for hex, vectorLayer for vector)
            - "name": Display name for layer toggle (optional)
        mapbox_token: Mapbox access token.
        basemap: 'dark', 'satellite', or custom Mapbox style URL.
        on_click: Dict to configure click broadcasting. Keys:
            - "properties": List of property names to include (default: all)
            - "channel": BroadcastChannel name (default: "fused-bus")
            - "message_type": Message type string (default: "feature_click")
            - "include_coords": Include click lat/lng (default: True)
            - "include_layer": Include layer name (default: True)
    
    Examples:
        # Mix hex and vector layers
        deckgl_layers(layers=[
            {
                "type": "hex",
                "data": hex_df,
                "config": {
                    "hexLayer": {
                        "filled": True,
                        "getFillColor": {"@@function": "colorContinuous", "attr": "value", "domain": [0, 100], "colors": "Mint"}
                    }
                },
                "name": "Elevation"
            },
            {
                "type": "vector",
                "data": boundaries_gdf,
                "config": {
                    "vectorLayer": {
                        "filled": False,
                        "stroked": True,
                        "getLineColor": [255, 255, 0],
                        "lineWidthMinPixels": 2
                    }
                },
                "name": "Farm Boundaries"
            },
        ], basemap="dark")
    
    Returns:
        HTML object for rendering in Fused Workbench
    """
    import math
    
    # Basemap setup (config can override function arg)
    basemap_styles = {
        "dark": "mapbox://styles/mapbox/dark-v11",
        "satellite": "mapbox://styles/mapbox/satellite-streets-v12",
        "light": "mapbox://styles/mapbox/light-v11",
        "streets": "mapbox://styles/mapbox/streets-v12"
    }
    basemap_from_config = None
    try:
        if layers and isinstance(layers, list):
            for ld in layers:
                cfg0 = (ld or {}).get("config", {})
                if isinstance(cfg0, dict) and cfg0.get("basemap"):
                    basemap_from_config = cfg0.get("basemap")
                    break
    except Exception:
        basemap_from_config = None

    basemap_value = basemap_from_config or basemap or "dark"
    basemap_key = {"sat": "satellite", "satellite-streets": "satellite"}.get(
        (basemap_value or "dark").lower(),
        (basemap_value or "dark").lower(),
    )
    style_url = basemap_styles.get(basemap_key, basemap_value or basemap_styles["dark"])

    # Process each layer
    processed_layers = []
    has_tile_layers = False
    has_mvt_layers = False
    has_sql_layers = False
    
    for i, layer_def in enumerate(layers):
        layer_type = layer_def.get("type", "hex").lower()
        df = layer_def.get("data")
        tile_url = layer_def.get("tile_url")
        config = layer_def.get("config", {})
        name = layer_def.get("name", f"Layer {i + 1}")

        # Track whether the user explicitly provided a fill domain in the input config.
        # (Merged defaults may include a domain, but we only want to treat it as "user set" when provided.)
        user_config = {}
        if isinstance(config, str):
            try:
                user_config = json.loads(config)
            except Exception:
                user_config = {}
        elif isinstance(config, dict):
            user_config = config
        else:
            user_config = {}
        
        if layer_type == "hex":
            # Process hex layer
            fill_domain_from_user = False
            try:
                user_fill = (((user_config or {}).get("hexLayer") or {}).get("getFillColor") or {})
                fill_domain_from_user = (
                    isinstance(user_fill, dict)
                    and isinstance(user_fill.get("domain"), (list, tuple))
                    and len(user_fill.get("domain")) >= 2
                )
            except Exception:
                fill_domain_from_user = False

            merged_config = _load_deckgl_config(config, DEFAULT_DECK_HEX_CONFIG)
            hex_layer = merged_config.get("hexLayer", {})
            tile_layer_config = merged_config.get("tileLayer", {})
            
            # Remove invalid hexLayer properties silently
            for key in list(hex_layer.keys()):
                if key not in VALID_HEX_LAYER_PROPS:
                    hex_layer.pop(key, None)
            
            is_tile_layer = tile_url is not None
            if is_tile_layer:
                has_tile_layers = True
            
            # Convert dataframe to records (for static layers)
            data_records = []
            df_clean = None
            if not is_tile_layer and df is not None and hasattr(df, 'to_dict'):
                import numpy as np
                import json
                
                # Drop geometry column - hex layers use hex ID, not geometry
                df_clean = df.drop(columns=['geometry'], errors='ignore') if hasattr(df, 'drop') else df
                df_clean = df_clean.copy()
                
                # Vectorized hex ID conversion FIRST (converts BigInt hex to string)
                hex_col = next((c for c in ['hex', 'h3', 'index', 'id'] if c in df_clean.columns), None)
                if hex_col:
                    def _to_hex_str(val):
                        if val is None: return None
                        try:
                            if isinstance(val, (int, float, np.integer)): return format(int(val), 'x')
                            if isinstance(val, str): return format(int(val), 'x') if val.isdigit() else val
                        except (ValueError, OverflowError): pass
                        return str(val) if val is not None else None
                    df_clean['hex'] = df_clean[hex_col].apply(_to_hex_str)
                
                # Convert all int64/uint64 to regular Python types for JSON serialization
                def _convert_value(val):
                    if val is None or (isinstance(val, float) and np.isnan(val)):
                        return None
                    if isinstance(val, (np.integer, np.int64, np.uint64)):
                        return int(val)
                    if isinstance(val, np.floating):
                        return float(val)
                    return val
                
                # Convert to records and sanitize each value
                raw_records = df_clean.to_dict('records')
                data_records = [
                    {k: _convert_value(v) for k, v in row.items()}
                    for row in raw_records
                ]
            
            tooltip_columns = _extract_tooltip_columns((config, merged_config, hex_layer))
            
            # Enable DuckDB SQL only when we can serialize Parquet (known-good input path).
            layer_sql = hex_layer.get("sql", "SELECT * FROM data")  # Default SQL (only used if parquetData exists)
            parquet_base64 = None
            if not is_tile_layer and df_clean is not None and len(data_records) > 0:
                # Serialize to Parquet for efficient DuckDB loading
                import io
                import base64
                try:
                    buf = io.BytesIO()
                    df_clean.to_parquet(buf, index=False)
                    parquet_base64 = base64.b64encode(buf.getvalue()).decode('ascii')
                except Exception:
                    parquet_base64 = None
                
                if parquet_base64:
                    has_sql_layers = True
                else:
                    # No fallback: skip SQL for this layer if Parquet serialization fails.
                    layer_sql = None
            
            processed_layers.append({
                "id": f"layer-{i}",
                "name": name,
                "layerType": "hex",
                "data": data_records,
                "tileUrl": tile_url,
                "isTileLayer": is_tile_layer,
                "tileLayerConfig": tile_layer_config,
                "config": merged_config,
                "hexLayer": hex_layer,
                "fillDomainFromUser": fill_domain_from_user,
                "tooltipColumns": tooltip_columns,
                "visible": True,
                "sql": layer_sql if (not is_tile_layer and len(data_records) > 0 and parquet_base64) else None,
                "parquetData": parquet_base64,
            })
            
        elif layer_type == "vector":
            # Process vector layer (similar to deckgl_map)
            fill_domain_from_user = False
            try:
                user_fill = (((user_config or {}).get("vectorLayer") or {}).get("getFillColor") or {})
                fill_domain_from_user = (
                    isinstance(user_fill, dict)
                    and isinstance(user_fill.get("domain"), (list, tuple))
                    and len(user_fill.get("domain")) >= 2
                )
            except Exception:
                fill_domain_from_user = False

            merged_config = _load_deckgl_config(config, DEFAULT_DECK_CONFIG)
            vector_layer = merged_config.get("vectorLayer", {})
            tile_url = layer_def.get("tile_url")
            
            # Check if this is an MVT tile layer (auto-detect from tile_url)
            is_mvt = tile_url is not None and "mvt" in tile_url.lower()
            
            if is_mvt:
                # MVT vector tile layer
                has_mvt_layers = True
                source_layer = layer_def.get("source_layer", "udf")
                
                # Extract styling with dynamic coloring support
                fill_opacity = vector_layer.get("opacity", 0.8)
                line_width = vector_layer.get("lineWidthMinPixels") or vector_layer.get("getLineWidth", 1)
                is_filled = vector_layer.get("filled", True)
                
                # Process fill color (supports colorContinuous, colorCategories, hasProp, arrays, and static)
                fill_color_raw = vector_layer.get("getFillColor", [255, 245, 204])
                fill_color = "#FFF5CC"
                fill_color_config = None
                if isinstance(fill_color_raw, dict) and fill_color_raw.get("@@function") in ("colorContinuous", "colorCategories", "hasProp"):
                    fill_color_config = fill_color_raw
                elif isinstance(fill_color_raw, (list, tuple)) and len(fill_color_raw) >= 3:
                    r, g, b = int(fill_color_raw[0]), int(fill_color_raw[1]), int(fill_color_raw[2])
                    fill_color = f"rgb({r},{g},{b})"
                elif isinstance(fill_color_raw, str):
                    fill_color = fill_color_raw
                
                # Process line color (supports colorContinuous, colorCategories, hasProp, arrays, and static)
                line_color_raw = vector_layer.get("getLineColor", [255, 255, 255])
                line_color = "#FFFFFF"
                line_color_config = None
                if isinstance(line_color_raw, dict) and line_color_raw.get("@@function") in ("colorContinuous", "colorCategories", "hasProp"):
                    line_color_config = line_color_raw
                elif isinstance(line_color_raw, (list, tuple)) and len(line_color_raw) >= 3:
                    r, g, b = int(line_color_raw[0]), int(line_color_raw[1]), int(line_color_raw[2])
                    line_color = f"rgb({r},{g},{b})"
                elif isinstance(line_color_raw, str):
                    line_color = line_color_raw
                
                # Extrusion settings
                is_extruded = vector_layer.get("extruded", False)
                height_property = vector_layer.get("heightProperty", "height")
                height_multiplier = vector_layer.get("heightMultiplier", 1)
                extrusion_opacity = vector_layer.get("extrusionOpacity", 0.9)
                
                minzoom = vector_layer.get("minzoom", 0)
                maxzoom = vector_layer.get("maxzoom", 22)
                
                # Tooltip columns
                tooltip_cols = layer_def.get("tooltip_columns", [])
                
                processed_layers.append({
                    "id": f"layer-{i}",
                    "name": name,
                    "layerType": "mvt",
                    "tileUrl": tile_url,
                    "sourceLayer": source_layer,
                    "config": merged_config,
                    "fillDomainFromUser": fill_domain_from_user,
                    "minzoom": minzoom,
                    "maxzoom": maxzoom,
                    "fillColor": fill_color,
                    "fillColorConfig": fill_color_config,
                    "fillOpacity": fill_opacity,
                    "isFilled": is_filled,
                    "lineColor": line_color,
                    "lineColorConfig": line_color_config,
                    "lineWidth": line_width,
                    "isExtruded": is_extruded,
                    "extrusionOpacity": extrusion_opacity,
                    "heightProperty": height_property,
                    "heightMultiplier": height_multiplier,
                    "tooltipColumns": tooltip_cols,
                    "visible": True,
                })
                continue  # Skip to next layer
            
            # Standard GeoJSON vector layer
            geojson_obj = {"type": "FeatureCollection", "features": []}
            if df is not None:
                # Reproject to EPSG:4326 if needed
                if hasattr(df, "crs") and df.crs and getattr(df.crs, "to_epsg", lambda: None)() != 4326:
                    try:
                        df = df.to_crs(epsg=4326)
                    except Exception:
                        pass
                
                # Robust GeoJSON conversion (fixed):
                # 1) prefer GeoPandas to_json()
                # 2) fallback to __geo_interface__ if to_json fails
                if hasattr(df, "to_json"):
                    try:
                        geojson_obj = json.loads(df.to_json())
                    except Exception:
                        geojson_obj = {"type": "FeatureCollection", "features": []}

                if (not geojson_obj.get("features")) and hasattr(df, "__geo_interface__"):
                    try:
                        gi = df.__geo_interface__
                        if isinstance(gi, dict) and gi.get("type") == "FeatureCollection":
                            geojson_obj = gi
                    except Exception:
                        pass
                
                # Add unique index to each feature for unclipped geometry lookup on click
                for idx, feat in enumerate(geojson_obj.get("features", []) or []):
                    feat["properties"] = {k: _sanitize_geojson_value(v) for k, v in (feat.get("properties") or {}).items()}
                    feat["properties"]["_fused_idx"] = idx
            
            # Extract color config - only if layer is filled
            fill_color_config = {}
            fill_color_rgba = None
            color_attr = None
            
            # Check if filled is explicitly False in original config
            original_vector = (config or {}).get("vectorLayer", {})
            is_filled = original_vector.get("filled", True) and vector_layer.get("filled", True)
            
            if is_filled:
                fill_color_raw = vector_layer.get("getFillColor")
                if isinstance(fill_color_raw, dict) and fill_color_raw.get("@@function") in ("colorContinuous", "colorCategories"):
                    fill_color_config = fill_color_raw
                    color_attr = fill_color_raw.get("attr")
                elif isinstance(fill_color_raw, (list, tuple)) and len(fill_color_raw) >= 3:
                    r, g, b = int(fill_color_raw[0]), int(fill_color_raw[1]), int(fill_color_raw[2])
                    a = fill_color_raw[3] / 255.0 if len(fill_color_raw) > 3 else 0.6
                    fill_color_rgba = f"rgba({r},{g},{b},{a})"
            
            line_color_raw = vector_layer.get("getLineColor")
            line_color_rgba = None
            line_color_config = {}
            line_color_attr = None
            
            if isinstance(line_color_raw, dict) and line_color_raw.get("@@function") in ("colorContinuous", "colorCategories"):
                line_color_config = line_color_raw
                line_color_attr = line_color_raw.get("attr")
            elif isinstance(line_color_raw, (list, tuple)) and len(line_color_raw) >= 3:
                r, g, b = int(line_color_raw[0]), int(line_color_raw[1]), int(line_color_raw[2])
                a = line_color_raw[3] / 255.0 if len(line_color_raw) > 3 else 1.0
                line_color_rgba = f"rgba({r},{g},{b},{a})"
            
            data_columns = [col for col in (df.columns if df is not None and hasattr(df, 'columns') else []) if col != "geometry"]
            tooltip_columns = _extract_tooltip_columns((config, merged_config, vector_layer), data_columns)
            
            processed_layers.append({
                "id": f"layer-{i}",
                "name": name,
                "layerType": "vector",
                "geojson": geojson_obj,
                "config": merged_config,
                "vectorLayer": vector_layer,
                "fillDomainFromUser": fill_domain_from_user,
                "fillColorConfig": fill_color_config,
                "fillColorRgba": fill_color_rgba,
                "colorAttr": color_attr,
                "lineColorConfig": line_color_config,
                "lineColorRgba": line_color_rgba,
                "lineColorAttr": line_color_attr,
                "lineWidth": vector_layer.get("lineWidthMinPixels") or vector_layer.get("getLineWidth", 1),
                "pointRadius": vector_layer.get("pointRadiusMinPixels") or vector_layer.get("pointRadius", 6),
                "isFilled": is_filled,
                "isStroked": vector_layer.get("stroked", True),
                "opacity": vector_layer.get("opacity", 0.8),
                "tooltipColumns": tooltip_columns,
                "visible": True,
            })
    
    # Auto-center from all layers' data
    auto_center, auto_zoom = (-119.4179, 36.7783), 5
    
    # Check first layer for custom view state
    first_config = layers[0].get("config", {}) if layers else {}
    user_initial_state = first_config.get('initialViewState', {}) or {}
    has_custom_view = any(
        user_initial_state.get(key) is not None
        for key in ('longitude', 'latitude', 'zoom')
    )
    
    ivs = processed_layers[0]["config"].get('initialViewState', {}) if processed_layers else {}
    center_lng = ivs.get('longitude') or auto_center[0]
    center_lat = ivs.get('latitude') or auto_center[1]
    zoom = ivs.get('zoom') or auto_zoom
    pitch = ivs.get('pitch', 0)
    bearing = ivs.get('bearing', 0)

    html = Template(r"""
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <link href="https://api.mapbox.com/mapbox-gl-js/v3.2.0/mapbox-gl.css" rel="stylesheet" />
  <script src="https://api.mapbox.com/mapbox-gl-js/v3.2.0/mapbox-gl.js"></script>
  <script src="https://unpkg.com/h3-js@4.1.0/dist/h3-js.umd.js"></script>
  {% if has_tile_layers %}
  <script src="https://unpkg.com/deck.gl@9.1.3/dist.min.js"></script>
  <script src="https://unpkg.com/@deck.gl/geo-layers@9.1.3/dist.min.js"></script>
  <script src="https://unpkg.com/@deck.gl/carto@9.1.3/dist.min.js"></script>
  {% endif %}
  {% if has_sql_layers %}
  <script src="https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.1-dev132.0/dist/duckdb-wasm.js"></script>
  <script type="module">
    import * as duckdb_wasm from "https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.1-dev132.0/+esm";
    window.__DUCKDB_WASM = duckdb_wasm;
  </script>
  {% endif %}
  <script type="module">
    import * as cartocolor from 'https://esm.sh/cartocolor@5.0.2';
    window.cartocolor = cartocolor;
  </script>
  <style>
    html, body { margin:0; height:100%; width:100%; display:flex; overflow:hidden; }
    #map { flex:1; height:100%; }
    #tooltip { position:absolute; pointer-events:none; background:rgba(15,15,15,0.95); color:#fff; padding:10px 14px; border-radius:6px; font-size:12px; display:none; z-index:6; max-width:320px; border:1px solid rgba(255,255,255,0.1); box-shadow:0 4px 16px rgba(0,0,0,0.4); font-family:Inter,'SF Pro Display','Segoe UI',sans-serif; }
    #tooltip .tt-title { display:block; margin-bottom:8px; padding-bottom:6px; border-bottom:1px solid rgba(255,255,255,0.15); font-size:11px; letter-spacing:0.3px; text-transform:uppercase; color: rgba(255,255,255,0.72); }
    #tooltip .tt-row { display:flex; justify-content:space-between; padding:3px 0; gap:12px; }
    #tooltip .tt-key { color:rgba(255,255,255,0.6); font-size:11px; }
    #tooltip .tt-val { color:#fff; font-weight:500; text-align:right; max-width:180px; word-break:break-word; }
    
    /* Layer Toggle Panel */
    #layer-panel {
      position: fixed;
      top: 12px;
      right: 12px;
      background: rgba(26, 26, 26, 0.95);
      border: 1px solid #424242;
      border-radius: 8px;
      padding: 0; /* no padding so gutter is flush */
      font-family: Inter, 'SF Pro Display', 'Segoe UI', sans-serif;
      color: #f5f5f5;
      min-width: 160px;
      z-index: 100;
      box-shadow: 0 4px 12px rgba(0,0,0,0.3);
      overflow: hidden; /* clip gutter to rounded corners */
    }
    .layer-item {
      display: flex;
      align-items: center;
      gap: 8px;
      padding: 8px 10px 8px 16px; /* text padding; gutter overlaps left 6px */
      border-bottom: 1px solid #333;
      position: relative;
      cursor: pointer;
    }
    .layer-item:last-child { border-bottom: none; }
    /* Palette gradient gutter on the left edge - fully flush */
    .layer-item::before {
      content: '';
      position: absolute;
      left: 0;
      top: 0;
      bottom: 0;
      width: 6px;
      background: var(--layer-strip, rgba(255,255,255,0.22));
    }
    .layer-item .layer-eye {
      width: 18px;
      height: 18px;
      cursor: pointer;
      display: flex;
      align-items: center;
      justify-content: center;
      color: #aaa;
      font-size: 14px;
      transition: color 0.15s;
      user-select: none;
      margin-left: auto; /* push eye to the far right */
    }
    .layer-item .layer-eye:hover { color: #fff; }
    .layer-item.disabled .layer-eye { color: #555; }
    .layer-item .layer-name {
      flex: 1;
      font-size: 12px;
      color: #ccc;
      transition: color 0.15s;
    }
    .layer-item.disabled .layer-name { color: #555; }
    .legend-layer { margin-bottom: 16px; padding-bottom: 12px; border-bottom: 1px solid rgba(255,255,255,0.1); }
    .legend-layer:last-child { margin-bottom: 0; padding-bottom: 0; border-bottom: none; letter-spacing: 0.3px; }
    
    /* Legend */
    .color-legend {
      position: fixed;
      right: 12px;
      bottom: 12px;
      background: rgba(26, 26, 26, 0.95);
      border: 1px solid #424242;
      border-radius: 8px;
      padding: 8px 10px;
      font-family: Inter, 'SF Pro Display', 'Segoe UI', sans-serif;
      color: #f5f5f5;
      font-size: 11px;
      z-index: 10;
      min-width: 140px;
      box-shadow: 0 4px 12px rgba(0,0,0,0.3);
    }

    /* Mapbox scale (bottom-left) */
    .legend-layer { margin-bottom: 16px; padding-bottom: 12px; border-bottom: 1px solid rgba(255,255,255,0.1); }
    .legend-layer:last-child { margin-bottom: 0; padding-bottom: 0; border-bottom: none; }
    .legend-layer .legend-title { margin-bottom: 6px; font-weight: 500; }
    .legend-layer .legend-gradient { height: 10px; border-radius: 2px; margin-bottom: 4px; border: 1px solid rgba(255,255,255,0.2); }
    .legend-layer .legend-labels { display: flex; justify-content: space-between; font-size: 10px; color: #ccc; }
    .legend-layer .legend-categories { display: flex; flex-direction: column; gap: 4px; margin-top: 4px; max-height: 440px; overflow-y: scroll; padding-right: 4px; }
    .legend-layer .legend-categories::-webkit-scrollbar { width: 4px; }
    .legend-layer .legend-categories::-webkit-scrollbar-track { background: rgba(255,255,255,0.05); border-radius: 2px; }
    .legend-layer .legend-categories::-webkit-scrollbar-thumb { background: rgba(255,255,255,0.2); border-radius: 2px; }
    .legend-layer .legend-categories::-webkit-scrollbar-thumb:hover { background: rgba(255,255,255,0.3); }
    .legend-layer .legend-cat-item { display: flex; align-items: center; gap: 8px; font-size: 11px; flex-shrink: 0; }
    .legend-layer .legend-cat-swatch { width: 14px; height: 14px; border-radius: 3px; border: 1px solid rgba(255,255,255,0.2); flex-shrink: 0; }
    .legend-layer .legend-cat-label { color: #ddd; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; max-width: 120px; }
    .legend-layer .legend-cat-more { font-size: 10px; color: #888; font-style: italic; margin-top: 2px; }
    
    /* Tile Loading Indicator */
    #tile-loader {
      position: fixed;
      bottom: 12px;
      right: 12px;
      background: rgba(26, 26, 26, 0.9);
      border: 1px solid #424242;
      border-radius: 6px;
      padding: 8px 12px;
      display: none;
      align-items: center;
      gap: 8px;
      font-family: Inter, 'SF Pro Display', 'Segoe UI', sans-serif;
      font-size: 11px;
      color: #aaa;
      z-index: 90;
      box-shadow: 0 2px 8px rgba(0,0,0,0.3);
    }
    #tile-loader.visible { display: flex; }
    #tile-loader .loader-spinner {
      width: 14px;
      height: 14px;
      border: 2px solid rgba(255,255,255,0.1);
      border-top-color: #E8FF59;
      border-radius: 50%;
      animation: spin 0.8s linear infinite;
    }
    @keyframes spin { to { transform: rotate(360deg); } }
    
    /* Debug Panel - Minimal */
    #debug-shell { position: fixed; left: 0; top: 0; height: 100%; z-index: 200; --debug-panel-w: 280px; }
    #debug-panel { position: fixed; left: 0; top: 0; width: 280px; min-width: 240px; max-width: 520px; height: 100%; background: rgba(24,24,24,0.98); border-right: 1px solid #333; transform: translateX(0); transition: transform 0.2s ease; z-index: 199; display: flex; flex-direction: column; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; overflow: hidden; }
    #debug-panel, #debug-panel * { box-sizing: border-box; }
    #debug-resize-handle { position: absolute; top: 0; right: 0; width: 8px; height: 100%; cursor: col-resize; background: transparent; }
    /* Use the old tooltip accent only when hovering the resize handle */
    #debug-resize-handle:hover { background: linear-gradient(to left, rgba(232,255,89,0.95) 0 2px, rgba(255,255,255,0.04) 2px 100%); }
    #debug-panel.collapsed { transform: translateX(-100%); }
    #debug-toggle { position: fixed; top: 12px; width: 24px; height: 24px; background: rgba(30,30,30,0.9); border: 1px solid #333; border-left: none; border-radius: 0 6px 6px 0; cursor: pointer; display: flex; align-items: center; justify-content: center; color: #888; font-size: 14px; z-index: 200; transition: background 0.15s, color 0.15s; left: var(--debug-panel-w, 280px); }
    #debug-toggle:hover { background: rgba(50,50,50,0.95); color: #ccc; }
    #debug-content { flex: 1; overflow-y: auto; padding: 12px; display: flex; flex-direction: column; gap: 12px; }
    /* Minimal grey scrollbars for debug panel + dropdown menus */
    #debug-content { scrollbar-width: thin; scrollbar-color: rgba(255,255,255,0.22) rgba(255,255,255,0.06); }
    #debug-content::-webkit-scrollbar { width: 6px; }
    #debug-content::-webkit-scrollbar-track { background: rgba(255,255,255,0.06); border-radius: 6px; }
    #debug-content::-webkit-scrollbar-thumb { background: rgba(255,255,255,0.22); border-radius: 6px; }
    #debug-content::-webkit-scrollbar-thumb:hover { background: rgba(255,255,255,0.30); }

    .pal-menu, .debug-checklist { scrollbar-width: thin; scrollbar-color: rgba(255,255,255,0.22) rgba(255,255,255,0.06); }
    .pal-menu::-webkit-scrollbar, .debug-checklist::-webkit-scrollbar { width: 6px; }
    .pal-menu::-webkit-scrollbar-track, .debug-checklist::-webkit-scrollbar-track { background: rgba(255,255,255,0.06); border-radius: 6px; }
    .pal-menu::-webkit-scrollbar-thumb, .debug-checklist::-webkit-scrollbar-thumb { background: rgba(255,255,255,0.22); border-radius: 6px; }
    .pal-menu::-webkit-scrollbar-thumb:hover, .debug-checklist::-webkit-scrollbar-thumb:hover { background: rgba(255,255,255,0.30); }
    .debug-section { background: rgba(40,40,40,0.6); border: 1px solid #333; border-radius: 6px; padding: 10px; }
    .debug-section-title { font-size: 10px; text-transform: uppercase; letter-spacing: 0.5px; color: #666; margin-bottom: 8px; }
    .debug-row { display: flex; align-items: center; gap: 8px; margin-bottom: 6px; flex-wrap: nowrap; }
    .debug-row:last-child { margin-bottom: 0; }
    /* Make single-field rows align perfectly */
    .debug-label { font-size: 11px; color: #999; width: 84px; flex: 0 0 84px; }
    /* Let the browser render native number steppers, but keep dark-mode + subtle */
    #debug-panel { color-scheme: dark; }
    .debug-input { flex: 1 1 auto; min-width: 0; width: auto; background: #1a1a1a; border: 1px solid #333; border-radius: 4px; padding: 5px 8px; font-size: 11px; color: #ddd; outline: none; }
    .debug-input:focus { border-color: #555; }
    .debug-input-sm { flex: 0 0 56px; width: 56px; min-width: 56px; text-align: center; }
    /* Tighten right inset for native number steppers */
    .debug-input[type="number"] { padding-right: 4px; }
    /* Subtle native spin buttons (WebKit) — mostly visible on focus */
    .debug-input[type="number"]::-webkit-inner-spin-button,
    .debug-input[type="number"]::-webkit-outer-spin-button {
      opacity: 0.18;
      filter: grayscale(1) brightness(0.9);
      margin: 0;
    }
    .debug-input[type="number"]:focus::-webkit-inner-spin-button,
    .debug-input[type="number"]:focus::-webkit-outer-spin-button {
      opacity: 0.45;
    }
    .debug-select { flex: 1 1 auto; min-width: 0; width: auto; background: #1a1a1a; border: 1px solid #333; border-radius: 4px; padding: 5px 8px; font-size: 11px; color: #ddd; outline: none; cursor: pointer; }
    .debug-select:focus { border-color: #555; }
    .pal-hidden { display: none; }
    /* Palette dropdown should take full row width like other selects */
    .pal-dd { flex: 1 1 auto; min-width: 0; width: 100%; position: relative; }
    .pal-trigger { width: 100%; display: flex; align-items: center; justify-content: flex-start; background: #1a1a1a; border: 1px solid #333; border-radius: 4px; padding: 0; color: #ddd; font-size: 11px; cursor: pointer; }
    .pal-trigger:hover { border-color: #444; }
    /* Trigger should be swatch-only; name is exposed via hover tooltip (title) */
    .pal-name { display: none; }
    .pal-swatch { width: 100%; height: 12px; border-radius: 3px; border: 1px solid #333; background: linear-gradient(90deg, #555, #999); }
    /* Palette menu: swatch-only, keep it tight (no empty leading space) */
    .pal-menu { position: absolute; left: 0; right: 0; top: calc(100% + 6px); background: #111; border: 1px solid #333; border-radius: 6px; max-height: 220px; overflow: auto; z-index: 9999; box-shadow: 0 8px 24px rgba(0,0,0,0.5); width: 100%; }
    /* Palette menu items: swatch-only (name via hover tooltip) */
    .pal-item { display: flex; align-items: center; justify-content: center; padding: 6px 0; cursor: pointer; }
    .pal-item:hover { background: rgba(255,255,255,0.06); }
    .pal-item-name { display: none; }
    .pal-item-swatch { width: 100%; height: 12px; border-radius: 3px; border: 1px solid #333; }
    .debug-toggles { display: flex; flex-wrap: wrap; gap: 8px 12px; }
    .debug-checkbox { display: flex; align-items: center; gap: 6px; font-size: 11px; color: #999; cursor: pointer; }
    .debug-checkbox input { width: 14px; height: 14px; cursor: pointer; accent-color: #666; }
    .debug-slider { flex: 1 1 auto; min-width: 0; height: 4px; background: #333; border-radius: 2px; -webkit-appearance: none; cursor: pointer; }
    .debug-slider::-webkit-slider-thumb { -webkit-appearance: none; width: 12px; height: 12px; background: #888; border-radius: 50%; cursor: pointer; }
    .debug-slider::-webkit-slider-thumb:hover { background: #aaa; }
    /* Dual-thumb range (two overlaid sliders) */
    .debug-dual-range { position: relative; flex: 1 1 auto; min-width: 0; height: 18px; }
    /* Single shared track so one range input's track can't cover the other's thumb */
    .debug-dual-range::before {
      content: "";
      position: absolute;
      left: 0;
      right: 0;
      top: 7px;
      height: 4px;
      background: #333;
      border-radius: 2px;
    }
    .debug-dual-range input[type="range"] {
      position: absolute;
      left: 0;
      top: 7px;
      width: 100%;
      height: 4px;
      margin: 0;
      background: transparent;
      pointer-events: none;
      -webkit-appearance: none;
      z-index: 2;
    }
    .debug-dual-range input[type="range"]::-webkit-slider-runnable-track { height: 4px; background: transparent; border-radius: 2px; }
    .debug-dual-range input[type="range"]::-webkit-slider-thumb {
      -webkit-appearance: none;
      pointer-events: auto;
      width: 12px;
      height: 12px;
      margin-top: -4px;
      background: #888;
      border-radius: 50%;
      cursor: pointer;
      position: relative;
      z-index: 3;
    }
    .debug-dual-range input[type="range"]::-webkit-slider-thumb:hover { background: #aaa; }
    .debug-dual-range input[type="range"]::-moz-range-track { height: 4px; background: transparent; border-radius: 2px; }
    .debug-dual-range input[type="range"]::-moz-range-thumb {
      pointer-events: auto;
      width: 12px;
      height: 12px;
      background: #888;
      border-radius: 50%;
      cursor: pointer;
    }
    .debug-color { width: 28px; height: 28px; border: 1px solid #333; border-radius: 4px; cursor: pointer; padding: 0; background: none; }
    .debug-color::-webkit-color-swatch-wrapper { padding: 2px; }
    .debug-color::-webkit-color-swatch { border-radius: 2px; border: none; }
    .debug-color-label { font-size: 10px; color: #666; font-family: 'SF Mono', Consolas, monospace; }
    /* Copy button removed (clipboard API is unreliable across some browsers/contexts) */
    .debug-checklist { display: flex; flex-direction: column; gap: 6px; max-height: 140px; overflow: auto; padding: 2px 0; }
    .debug-check { display: flex; align-items: center; gap: 8px; font-size: 11px; color: #bbb; }
    .debug-check input { width: 14px; height: 14px; accent-color: #666; }
    .debug-check code { font-family: 'SF Mono', Consolas, monospace; font-size: 10px; color: #aaa; }
    .debug-output { width: 100%; min-height: 120px; resize: vertical; background: #111; color: #aaa; border: 1px solid #333; border-radius: 4px; padding: 8px; font-family: 'SF Mono', Consolas, monospace; font-size: 10px; line-height: 1.4; }
  </style>
</head>
<body>
  <div id="map"></div>
  <div id="tooltip"></div>
  
  <!-- Layer Toggle Panel -->
  <div id="layer-panel">
    <div id="layer-list"></div>
  </div>
  
  <!-- Legend -->
  <div id="color-legend" class="color-legend" style="display:none;"></div>
  
  <!-- Tile Loading Indicator -->
  {% if has_tile_layers or has_mvt_layers %}
  <div id="tile-loader">
    <div class="loader-spinner"></div>
    <span id="loader-text">Loading tiles...</span>
  </div>
  {% endif %}

  {% if debug %}
  <!-- Debug Panel -->
  <div id="debug-shell">
  <div id="debug-panel">
    <div id="debug-content">
      <!-- Editing Layer -->
      <div class="debug-section">
        <div class="debug-section-title">Editing Layer</div>
        <div class="debug-row">
          <span class="debug-label">Layer</span>
          <select class="debug-select" id="dbg-layer-select"></select>
        </div>
      </div>
      <!-- Hex Layer Settings -->
      <div class="debug-section">
        <div class="debug-section-title">Hex Layer</div>
        <div class="debug-toggles">
        <label class="debug-checkbox"><input type="checkbox" id="dbg-filled" checked /> Filled</label>
          <label class="debug-checkbox"><input type="checkbox" id="dbg-stroked" checked /> Stroked</label>
        <label class="debug-checkbox"><input type="checkbox" id="dbg-extruded" /> Extruded</label>
        </div>
        <div class="debug-row" style="margin-top:8px;">
          <span class="debug-label">Opacity</span>
          <input type="range" class="debug-slider" id="dbg-opacity-slider" min="0" max="1" step="0.05" value="1" />
          <input type="number" class="debug-input debug-input-sm" id="dbg-opacity" step="0.1" min="0" max="1" value="1" />
        </div>
      </div>
      
      <!-- Fill Color -->
      <div class="debug-section" id="fill-color-section">
        <div class="debug-section-title">Fill Color</div>
        <div class="debug-row">
          <span class="debug-label">Function</span>
          <select class="debug-select" id="dbg-fill-fn">
            <option value="colorContinuous">colorContinuous</option>
            <option value="colorCategories">colorCategories</option>
            <option value="static">Static Color</option>
          </select>
        </div>
        <div id="fill-fn-options">
        <div class="debug-row">
          <span class="debug-label">Attribute</span>
          <select class="debug-select" id="dbg-attr"></select>
        </div>
        <div class="debug-row">
          <span class="debug-label">Reverse</span>
          <label class="debug-checkbox" style="margin-left:auto;">
            <input type="checkbox" id="dbg-reverse-colors" />
            Reverse colors
          </label>
        </div>
        <div class="debug-row">
          <span class="debug-label">Palette</span>
          <select class="debug-select pal-hidden" id="dbg-palette">
            {% for pal in palettes %}<option value="{{ pal }}">{{ pal }}</option>{% endfor %}
          </select>
          <div class="pal-dd" id="dbg-palette-dd">
            <button type="button" class="pal-trigger" id="dbg-palette-trigger" title="Palette">
              <span class="pal-name" id="dbg-palette-name">Palette</span>
              <span class="pal-swatch" id="dbg-palette-swatch"></span>
            </button>
            <div class="pal-menu" id="dbg-palette-menu" style="display:none;"></div>
        </div>
        </div>
        <div class="debug-row">
            <span class="debug-label">Domain</span>
            <input type="number" class="debug-input debug-input-sm" id="dbg-domain-min" step="0.1" placeholder="min" />
            <span style="color:#666;">–</span>
            <input type="number" class="debug-input debug-input-sm" id="dbg-domain-max" step="0.1" placeholder="max" />
        </div>
        <div class="debug-row">
          <span class="debug-label"></span>
          <div class="debug-dual-range" aria-label="Domain range">
            <input type="range" class="debug-range-min" id="dbg-domain-range-min" min="0" max="100" step="0.1" value="0" />
            <input type="range" class="debug-range-max" id="dbg-domain-range-max" min="0" max="100" step="0.1" value="100" />
      </div>
        </div>
        <div class="debug-row">
          <span class="debug-label">Steps</span>
            <input type="number" class="debug-input debug-input-sm" id="dbg-steps" step="1" min="2" max="20" value="7" />
        </div>
        <div class="debug-row">
            <span class="debug-label">Null Color</span>
            <input type="color" class="debug-color" id="dbg-null-color" value="#b8b8b8" />
            <span class="debug-color-label" id="dbg-null-color-label">#b8b8b8</span>
      </div>
      </div>
        <div id="fill-static-options" style="display:none;">
          <div class="debug-row">
            <span class="debug-label">Color</span>
            <input type="color" class="debug-color" id="dbg-fill-static" value="#0090ff" />
            <span class="debug-color-label" id="dbg-fill-static-label">#0090ff</span>
          </div>
        </div>
      </div>
      
      <!-- Line Color (when stroked) -->
      <div class="debug-section" id="line-color-section">
        <div class="debug-section-title">Line Color</div>
        <div class="debug-row">
          <span class="debug-label">Function</span>
          <select class="debug-select" id="dbg-line-fn">
            <option value="colorContinuous">colorContinuous</option>
            <option value="colorCategories">colorCategories</option>
            <option value="static" selected>Static Color</option>
          </select>
        </div>
        <div id="line-fn-options" style="display:none;">
        <div class="debug-row">
          <span class="debug-label">Attribute</span>
            <select class="debug-select" id="dbg-line-attr"></select>
        </div>
        <div class="debug-row">
          <span class="debug-label">Palette</span>
            <select class="debug-select pal-hidden" id="dbg-line-palette">
            {% for pal in palettes %}<option value="{{ pal }}">{{ pal }}</option>{% endfor %}
          </select>
            <div class="pal-dd" id="dbg-line-palette-dd">
              <button type="button" class="pal-trigger" id="dbg-line-palette-trigger" title="Palette">
                <span class="pal-name" id="dbg-line-palette-name">Palette</span>
                <span class="pal-swatch" id="dbg-line-palette-swatch"></span>
              </button>
              <div class="pal-menu" id="dbg-line-palette-menu" style="display:none;"></div>
            </div>
        </div>
        <div class="debug-row">
            <span class="debug-label">Domain</span>
            <input type="number" class="debug-input debug-input-sm" id="dbg-line-domain-min" step="0.1" placeholder="min" />
            <span style="color:#666;">–</span>
            <input type="number" class="debug-input debug-input-sm" id="dbg-line-domain-max" step="0.1" placeholder="max" />
        </div>
        </div>
        <div id="line-static-options">
        <div class="debug-row">
            <span class="debug-label">Color</span>
            <input type="color" class="debug-color" id="dbg-line-static" value="#ffffff" />
            <span class="debug-color-label" id="dbg-line-static-label">#ffffff</span>
          </div>
        </div>
        <div class="debug-row">
          <span class="debug-label">Line Width</span>
          <input type="range" class="debug-slider" id="dbg-line-width-slider" min="0" max="5" step="0.5" value="1" />
          <input type="number" class="debug-input debug-input-sm" id="dbg-line-width" step="0.5" min="0" max="10" value="1" />
        </div>
      </div>
      
      <!-- Elevation (when extruded) -->
      <div class="debug-section" id="elevation-section" style="display:none;">
        <div class="debug-section-title">Elevation</div>
        <div class="debug-row">
          <span class="debug-label">Height Attr</span>
          <select class="debug-select" id="dbg-height-attr"></select>
        </div>
        <div class="debug-row">
          <span class="debug-label">Elev. Scale</span>
          <input type="number" class="debug-input" id="dbg-height-scale" step="1" min="0" max="100000" value="10" />
        </div>
      </div>

      <!-- Tooltip -->
      <div class="debug-section" id="tooltip-section">
        <div class="debug-section-title">Tooltip Columns</div>
        <div id="dbg-tooltip-cols" class="debug-checklist"></div>
      </div>

      <!-- View State -->
      <div class="debug-section">
        <div class="debug-section-title">View State</div>
        <div class="debug-row">
          <span class="debug-label">Longitude</span>
          <input type="number" class="debug-input" id="dbg-lng" step="0.0001" />
        </div>
        <div class="debug-row">
          <span class="debug-label">Latitude</span>
          <input type="number" class="debug-input" id="dbg-lat" step="0.0001" />
        </div>
        <div class="debug-row">
          <span class="debug-label">Zoom</span>
          <input type="number" class="debug-input" id="dbg-zoom" step="0.1" min="0" max="22" />
        </div>
        <div class="debug-row">
          <span class="debug-label">Pitch</span>
          <input type="number" class="debug-input" id="dbg-pitch" step="1" min="0" max="85" />
        </div>
        <div class="debug-row">
          <span class="debug-label">Bearing</span>
          <input type="number" class="debug-input" id="dbg-bearing" step="1" />
        </div>
        <div class="debug-row">
          <span class="debug-label">Basemap</span>
          <select class="debug-select" id="dbg-basemap"></select>
        </div>
      </div>
      
      {% if has_sql_layers %}
      <!-- SQL Filter -->
      <div class="debug-section" id="sql-section">
        <div class="debug-section-title">SQL Filter <span id="sql-status" style="float:right;font-weight:normal;color:#888;"></span></div>
        <textarea id="dbg-sql" class="debug-output" style="height:60px;font-family:monospace;font-size:11px;resize:vertical;" placeholder="SELECT * FROM data"></textarea>
      </div>
      {% endif %}
      
      <!-- Config Output -->
      <div class="debug-section">
        <div class="debug-section-title">Config Output</div>
        <textarea id="dbg-output" class="debug-output" readonly></textarea>
      </div>
    </div>
    <div id="debug-resize-handle" title="Drag to resize"></div>
  </div>
  <div id="debug-toggle" onclick="toggleDebugPanel()">&#x2039;</div>
  </div>
  {% endif %}

  <script>
    const MAPBOX_TOKEN = {{ mapbox_token | tojson }};
    const LAYERS_DATA = {{ layers_data | tojson }};
    const HAS_CUSTOM_VIEW = {{ has_custom_view | tojson }};
    const INITIAL_BASEMAP = {{ basemap | tojson }};
    const HAS_SQL_LAYERS = {{ has_sql_layers | tojson }};
    // Defaults used for minimal snippet generation (we emit only overrides vs these).
    const DEFAULT_HEX_CONFIG = {{ default_hex_config | tojson }};
    const DEFAULT_VECTOR_CONFIG = {{ default_vector_config | tojson }};
    const BASEMAP_STYLES = {
      dark: "mapbox://styles/mapbox/dark-v11",
      satellite: "mapbox://styles/mapbox/satellite-streets-v12",
      light: "mapbox://styles/mapbox/light-v11",
      streets: "mapbox://styles/mapbox/streets-v12"
    };
    
    // Track layer visibility
    const layerVisibility = {};
    LAYERS_DATA.forEach(l => { layerVisibility[l.id] = l.visible; });

    // ========== H3 Hex Utilities ==========
    function toH3(hex) {
      if (hex == null) return null;
      try {
        if (typeof hex === 'string') {
          const s = hex.startsWith('0x') ? hex.slice(2) : hex;
          return /[a-f]/i.test(s) ? s.toLowerCase() : /^\d+$/.test(s) ? BigInt(s).toString(16) : s.toLowerCase();
        }
        if (typeof hex === 'number') return BigInt(Math.trunc(hex)).toString(16);
        if (typeof hex === 'bigint') return hex.toString(16);
      } catch(e) {}
      return null;
    }

    function hexToGeoJSON(data) {
      const features = [];
      for (const d of data) {
        const hexId = toH3(d.hex ?? d.h3 ?? d.index ?? d.id);
        if (!hexId || !h3.isValidCell(hexId)) continue;
        try {
          const boundary = h3.cellToBoundary(hexId);
          const coords = boundary.map(([lat, lng]) => [lng, lat]);
          coords.push(coords[0]);
          features.push({ type: 'Feature', properties: { ...d, hex: hexId }, geometry: { type: 'Polygon', coordinates: [coords] }});
        } catch(e) {}
      }
      return { type: 'FeatureCollection', features };
    }

    // ========== Color Utilities ==========
    function getPaletteColors(name, steps) {
      const pal = window.cartocolor?.[name];
      if (!pal) return null;
      const keys = Object.keys(pal).map(Number).filter(n => !isNaN(n)).sort((a, b) => a - b);
      const best = keys.find(n => n >= steps) || keys[keys.length - 1];
      return pal[best] ? [...pal[best]] : null;
    }

    // Extract unique categories from data for an attribute, with optional label mapping
    function getUniqueCategories(data, attr, labelAttr) {
      const seen = new Map(); // Map category value -> label
      (data || []).forEach(d => {
        const val = d?.[attr] ?? d?.properties?.[attr];
        if (val != null && val !== '' && val !== 'null') {
          if (!seen.has(val)) {
            const label = labelAttr ? (d?.[labelAttr] ?? d?.properties?.[labelAttr] ?? val) : val;
            seen.set(val, label);
          }
        }
      });
      // Sort by category value and return {value, label} pairs
      const sorted = [...seen.entries()].sort((a, b) => {
        if (typeof a[0] === 'number' && typeof b[0] === 'number') return a[0] - b[0];
        return String(a[0]).localeCompare(String(b[0]));
      });
      return sorted.map(([value, label]) => ({ value, label }));
    }
    
    function buildColorExpr(cfg, data) {
      if (!cfg || !cfg.attr) return null;
      
      // Handle colorCategories
      if (cfg['@@function'] === 'colorCategories') {
        // Accept both 'categories' and 'domain' as category sources
        const categorySource = cfg.categories || cfg.domain;
        let catPairs = categorySource 
          ? (Array.isArray(categorySource) 
              ? categorySource.map(c => typeof c === 'object' ? c : { value: c, label: String(c) })
              : [])
          : getUniqueCategories(data, cfg.attr, cfg.labelAttr);
        
        if (!catPairs.length) return 'rgba(128,128,128,0.5)';
        
        const name = cfg.colors || 'Bold';
        const cols = getPaletteColors(name, Math.max(catPairs.length, 3)) || ['#7F3C8D','#11A579','#3969AC','#F2B701','#E73F74','#80BA5A','#E68310','#008695','#CF1C90','#f97b72'];
        const fallback = cfg.nullColor ? `rgb(${cfg.nullColor.slice(0,3).join(',')})` : 'rgba(128,128,128,0.5)';
        const expr = ['match', ['get', cfg.attr]];
        catPairs.forEach((cat, i) => { expr.push(cat.value); expr.push(cols[i % cols.length]); });
        expr.push(fallback);
        // Store detected categories back for legend (with labels)
        cfg._detectedCategories = catPairs;
        return expr;
      }
      
      // Handle colorContinuous
      if (cfg['@@function'] !== 'colorContinuous' || !cfg.domain?.length) return null;
      const [d0, d1] = cfg.domain;
      const domainReversed = d0 > d1;
      const dom = domainReversed ? [d1, d0] : [d0, d1];
      const steps = cfg.steps || 7, name = cfg.colors || 'ArmyRose';
      let cols = getPaletteColors(name, steps);
      if (!cols || !cols.length) {
        return ['interpolate', ['linear'], ['get', cfg.attr], dom[0], 'rgb(237,248,251)', dom[1], 'rgb(0,109,44)'];
      }
      // Reverse logic:
      // - domain reversed already flips direction
      // - cfg.reverse toggles it
      const wantsReverse = !!cfg.reverse;
      const shouldReverse = domainReversed ? !wantsReverse : wantsReverse;
      if (shouldReverse) cols.reverse();
      const expr = ['interpolate', ['linear'], ['get', cfg.attr]];
      cols.forEach((c, i) => { expr.push(dom[0] + (dom[1] - dom[0]) * i / (cols.length - 1)); expr.push(c); });
      return expr;
    }

    function toRgba(arr, defaultAlpha) {
      if (!Array.isArray(arr) || arr.length < 3) return null;
      const [r, g, b] = arr;
      const a = arr.length >= 4 ? arr[3] / 255 : (defaultAlpha ?? 1);
      return `rgba(${r},${g},${b},${a})`;
    }

    // ========== Precompute Data ==========
    const layerGeoJSONs = {};
    LAYERS_DATA.forEach(l => {
      if (l.layerType === 'hex' && !l.isTileLayer) {
        layerGeoJSONs[l.id] = hexToGeoJSON(l.data);
      } else if (l.layerType === 'vector') {
        layerGeoJSONs[l.id] = l.geojson;
      }
    });

    const HAS_TILE_LAYERS = LAYERS_DATA.some(l => l.layerType === 'hex' && l.isTileLayer);
    const TILE_CACHE = new Map();
    const TILE_DATA_STORE = {};  // Track loaded tile data per layer for autoDomain
    
    // Tile loading indicator
    let tilesCurrentlyLoading = 0;
    let loaderHideTimeout = null;
    
    function updateTileLoader(delta) {
      tilesCurrentlyLoading = Math.max(0, tilesCurrentlyLoading + delta);
      const loader = document.getElementById('tile-loader');
      const loaderText = document.getElementById('loader-text');
      if (!loader) return;
      
      if (tilesCurrentlyLoading > 0) {
        clearTimeout(loaderHideTimeout);
        loader.classList.add('visible');
        loaderText.textContent = tilesCurrentlyLoading === 1 
          ? 'Loading tile...' 
          : `Loading ${tilesCurrentlyLoading} tiles...`;
      } else {
        // Small delay before hiding to prevent flicker
        loaderHideTimeout = setTimeout(() => {
          loader.classList.remove('visible');
        }, 300);
      }
    }

    // ========== DuckDB SQL Filtering ==========
    let duckConn = null;
    let duckDbReady = false;
    
    async function initDuckDB() {
      if (!HAS_SQL_LAYERS || !window.__DUCKDB_WASM) return;
      
      try {
        updateSqlStatus('initializing...');
        const duckmod = window.__DUCKDB_WASM;
        const bundle = await duckmod.selectBundle(duckmod.getJsDelivrBundles());
        const worker = new Worker(
          URL.createObjectURL(
            new Blob([await (await fetch(bundle.mainWorker)).text()], { type: 'application/javascript' })
          )
        );
        const db = new duckmod.AsyncDuckDB(new duckmod.ConsoleLogger(), worker);
        await db.instantiate(bundle.mainModule);
        duckConn = await db.connect();
        
        // Try to load H3 extension
        try { await duckConn.query("INSTALL h3 FROM community"); } catch (e) {}
        try { await duckConn.query("LOAD h3"); } catch (e) {}
        
        // Register data tables for each SQL-enabled layer
        for (const l of LAYERS_DATA) {
          if (l.sql && l.data && l.data.length > 0) {
            const tableName = l.id.replace(/-/g, '_'); // layer-0 -> layer_0
            
            // Prefer Parquet (faster, smaller). If missing, skip DuckDB load for this layer.
            if (l.parquetData) {
              // Decode base64 Parquet and register
              const binaryString = atob(l.parquetData);
              const bytes = new Uint8Array(binaryString.length);
              for (let i = 0; i < binaryString.length; i++) {
                bytes[i] = binaryString.charCodeAt(i);
              }
              await db.registerFileBuffer(`${tableName}.parquet`, bytes);
              await duckConn.query(`CREATE OR REPLACE TABLE ${tableName} AS SELECT * FROM read_parquet('${tableName}.parquet')`);
            } else { continue; }
            
            // Also create an alias 'data' for the first SQL layer
            if (!duckDbReady) {
              await duckConn.query(`CREATE OR REPLACE VIEW data AS SELECT * FROM ${tableName}`);
            }
          }
        }
        
        duckDbReady = true;
        updateSqlStatus('ready');
        
        // Calculate slider extent (min/max) from full dataset
        const firstSqlLayer = LAYERS_DATA.find(l => l.sql);
        const colorAttr = firstSqlLayer?.hexLayer?.getFillColor?.attr || debugState?.fillAttr;
        if (colorAttr) {
          try {
            const statsRes = await duckConn.query(`SELECT MIN("${colorAttr}") as min_val, MAX("${colorAttr}") as max_val FROM data`);
            const statsRow = statsRes.toArray()[0];
            if (statsRow) {
              let minVal = statsRow.min_val;
              let maxVal = statsRow.max_val;
              // Handle BigInt
              if (typeof minVal === 'bigint') minVal = Number(minVal);
              if (typeof maxVal === 'bigint') maxVal = Number(maxVal);
              
              if (Number.isFinite(minVal) && Number.isFinite(maxVal)) {
                // Set slider min/max (extent) once
                if (typeof setDomainSliderBounds === 'function') setDomainSliderBounds(minVal, maxVal);

                // If the user already provided a domain in config, DO NOT override it here.
                // Otherwise, initialize the selected domain to the full extent.
                const cfgDomain = firstSqlLayer?.hexLayer?.getFillColor?.domain;
                const hasUserDomain = Array.isArray(cfgDomain) && cfgDomain.length >= 2;

                if (!hasUserDomain) {
                  const minInput = document.getElementById('dbg-domain-min');
                  const maxInput = document.getElementById('dbg-domain-max');
                  if (minInput) { minInput.value = minVal.toFixed(2); debugState.fillDomainMin = minVal; }
                  if (maxInput) { maxInput.value = maxVal.toFixed(2); debugState.fillDomainMax = maxVal; }
                  // This is automatic initialization; keep snippet domain omitted unless user changes it.
                  if (typeof syncDomainSliderFromInputs === 'function') syncDomainSliderFromInputs();
                  if (firstSqlLayer?.hexLayer?.getFillColor) {
                    firstSqlLayer.hexLayer.getFillColor.domain = [minVal, maxVal];
                  }
                  // Apply immediately so the map/styles/snippet reflect the computed extent without user interaction.
                  if (typeof scheduleLayerUpdate === 'function') scheduleLayerUpdate();
                } else {
                  // Just ensure thumbs reflect the user's config domain
                  if (typeof syncDomainSliderFromInputs === 'function') syncDomainSliderFromInputs();
                }

                updateSqlStatus(`extent: ${minVal.toFixed(1)} - ${maxVal.toFixed(1)}`);
              }
            }
          } catch (e) {
            console.warn('Could not calculate domain:', e);
          }
        }
        
        // Initialize SQL input with first layer's SQL or default
        const sqlInput = document.getElementById('dbg-sql');
        if (sqlInput) {
          sqlInput.value = firstSqlLayer?.sql || 'SELECT * FROM data';
        }
        
        // Run the initial SQL query to apply the filter
        await runSqlQuery();
      } catch (e) {
        console.error('DuckDB init error:', e);
        updateSqlStatus('init error');
      }
    }
    
    function updateSqlStatus(text) {
      const el = document.getElementById('sql-status');
      if (el) el.textContent = text;
    }
    
    async function runSqlQuery() {
      if (!duckConn || !duckDbReady) {
        updateSqlStatus('not ready');
        return;
      }
      
      const sqlInput = document.getElementById('dbg-sql');
      const sqlText = (sqlInput?.value || 'SELECT * FROM data').trim();
      if (!sqlText) return;
      
      try {
        updateSqlStatus('running...');
        const res = await duckConn.query(sqlText);
        const hexColumns = new Set(['hex', 'h3', 'index', 'id']);
        const rows = res.toArray().map(row => {
          // Convert Arrow row to plain object, handling BigInt
          const obj = {};
          for (const key of Object.keys(row)) {
            let val = row[key];
            if (typeof val === 'bigint') {
              // Only convert hex columns to hex string, others to number
              if (hexColumns.has(key.toLowerCase())) {
                val = val.toString(16);
              } else {
                // Convert to number if safe, otherwise string
                val = Number(val);
              }
            }
            obj[key] = val;
          }
          return obj;
        });
        
        // Normalize hex IDs using toH3 (handles various formats)
        const features = rows.map(p => {
          const rawHex = p.hex ?? p.h3 ?? p.index ?? p.id;
          const hex = toH3(rawHex);
          if (!hex) return null;
          const props = { ...p, hex };
          return { ...props, properties: props };
        }).filter(Boolean);
        
        updateSqlStatus(`${features.length.toLocaleString()} rows`);
        
        // Update the first SQL-enabled layer with filtered data
        const sqlLayer = LAYERS_DATA.find(l => l.sql);
        if (sqlLayer) {
          sqlLayer.data = features;
          layerGeoJSONs[sqlLayer.id] = hexToGeoJSON(features);
          addAllLayers();
          if (typeof updateLegend === 'function') updateLegend();
          if (typeof updateLayerPanel === 'function') updateLayerPanel();
          if (typeof updateConfigOutput === 'function') updateConfigOutput();
        }
      } catch (e) {
        console.error('SQL error:', e);
        updateSqlStatus('error: ' + (e.message || 'query failed').substring(0, 30));
      }
    }
    
    // Debounced SQL execution
    let sqlTypingTimer = null;
    function scheduleSqlQuery() {
      clearTimeout(sqlTypingTimer);
      sqlTypingTimer = setTimeout(runSqlQuery, 500);
    }

    // ========== Map Setup ==========
    mapboxgl.accessToken = MAPBOX_TOKEN;
    const map = new mapboxgl.Map({ 
      container: 'map', 
      style: {{ style_url | tojson }}, 
      center: [{{ center_lng }}, {{ center_lat }}], 
      zoom: {{ zoom }}, 
      pitch: {{ pitch }}, 
      bearing: {{ bearing }}, 
      projection: 'mercator',
      // Keep pitch behavior enabled; we override the trackpad shortcut to Cmd+drag below.
      pitchWithRotate: true
    });

    // Mapbox scale (km) in bottom-left
    try { map.addControl(new mapboxgl.ScaleControl({ maxWidth: 110, unit: 'metric' }), 'bottom-left'); } catch (e) {}

    // Cmd (⌘) + drag to rotate/pitch (trackpad-friendly "3D orbit").
    // Also blocks Mapbox's default Ctrl+drag rotate so Cmd becomes the shortcut.
    (function enableCmdDragOrbit() {
      const canvas = map.getCanvasContainer?.() || map.getCanvas?.();
      if (!canvas) return;
      let active = false;
      let startX = 0;
      let startY = 0;
      let startBearing = 0;
      let startPitch = 0;
      const clamp = (v, lo, hi) => Math.max(lo, Math.min(hi, v));
      const PITCH_MIN = 0;
      const PITCH_MAX = 85;
      const SPEED_PITCH = 0.25;   // degrees per pixel
      const SPEED_BEARING = 0.35; // degrees per pixel

      function stop() {
        if (!active) return;
        active = false;
        try { map.dragPan.enable(); } catch (e) {}
        try { canvas.style.cursor = ''; } catch (e) {}
        window.removeEventListener('pointermove', onMove, { passive: false });
        window.removeEventListener('pointerup', onUp, { passive: false });
        window.removeEventListener('pointercancel', onUp, { passive: false });
      }

      function onMove(e) {
        if (!active) return;
        if (!e.metaKey) return stop();
        const dx = e.clientX - startX;
        const dy = e.clientY - startY;
        map.setBearing(startBearing + dx * SPEED_BEARING);
        map.setPitch(clamp(startPitch - dy * SPEED_PITCH, PITCH_MIN, PITCH_MAX));
        e.preventDefault();
      }

      function onUp(e) {
        stop();
        if (e) e.preventDefault();
      }

      function onDown(e) {
        // Block Mapbox default ctrl+drag rotate
        if (e.button === 0 && e.ctrlKey && !e.metaKey) {
          e.preventDefault();
          e.stopPropagation();
          return;
        }
        // Only left button + cmd key; ignore if already active
        if (active) return;
        if (e.button !== 0) return;
        if (!e.metaKey) return;
        active = true;
        startX = e.clientX;
        startY = e.clientY;
        startBearing = map.getBearing();
        startPitch = map.getPitch();
        try { map.dragPan.disable(); } catch (e2) {}
        try { canvas.style.cursor = 'grabbing'; } catch (e2) {}
        window.addEventListener('pointermove', onMove, { passive: false });
        window.addEventListener('pointerup', onUp, { passive: false });
        window.addEventListener('pointercancel', onUp, { passive: false });
        e.stopPropagation();
        e.preventDefault();
      }

      // Use capture so we can block ctrl+drag before Mapbox handlers see it.
      canvas.addEventListener('pointerdown', onDown, { passive: false, capture: true });
    })();

    // Deck.gl overlay for tile layers
    let deckOverlay = null;

    {% if has_tile_layers %}
    const { TileLayer, MapboxOverlay } = deck;
    const H3HexagonLayer = deck.H3HexagonLayer || (deck.GeoLayers && deck.GeoLayers.H3HexagonLayer);
    const { colorContinuous } = deck.carto;

    function normalizeTileData(raw) {
      const arr = Array.isArray(raw) ? raw : (Array.isArray(raw?.data) ? raw.data : (Array.isArray(raw?.features) ? raw.features : []));
      const rows = arr.map(d => d?.properties ? { ...d.properties } : { ...d });
      return rows.map(p => {
        const hexRaw = p.hex ?? p.h3 ?? p.index ?? p.id;
        const hex = toH3(hexRaw);
        if (!hex) return null;
        const props = { ...p, hex };
        return { ...props, properties: { ...props } };
      }).filter(Boolean);
    }

    function processColorContinuousCfg(cfg, computedDomain = null) {
      let domain = computedDomain || cfg.domain;
      if (domain && domain.length === 2) {
        const [min, max] = domain;
        const steps = cfg.steps ?? 20;
        const stepSize = (max - min) / (steps - 1);
        domain = Array.from({ length: steps }, (_, i) => min + stepSize * i);
      }
      if (cfg && cfg.reverse && Array.isArray(domain)) domain = [...domain].reverse();
      return { attr: cfg.attr, domain, colors: cfg.colors || 'ArmyRose', nullColor: cfg.nullColor || [184, 184, 184] };
    }

    // Convert tile coordinates to geographic bounds
    function tileToBounds(x, y, z) {
      const n = Math.PI - (2 * Math.PI * y) / Math.pow(2, z);
      const west = (x / Math.pow(2, z)) * 360 - 180;
      const north = (180 / Math.PI) * Math.atan(Math.sinh(n));
      
      const n2 = Math.PI - (2 * Math.PI * (y + 1)) / Math.pow(2, z);
      const east = ((x + 1) / Math.pow(2, z)) * 360 - 180;
      const south = (180 / Math.PI) * Math.atan(Math.sinh(n2));
      
      return { west, south, east, north };
    }
    
    // Check if two bounds intersect
    function boundsIntersect(a, b) {
      return !(a.east < b.west || a.west > b.east || a.north < b.south || a.south > b.north);
    }
    
    // AutoDomain: calculate domain from visible tiles (only when zoom >= 10)
    const AUTO_DOMAIN_MAX_SAMPLES = 5000;
    const AUTO_DOMAIN_MIN_ZOOM = 10;
    
    function calculateDomainFromTiles(layerId, attr) {
      // Only calculate when zoomed in enough
      if (map.getZoom() < AUTO_DOMAIN_MIN_ZOOM) return null;
      
      const tileData = TILE_DATA_STORE[layerId];
      if (!tileData || !Object.keys(tileData).length) return null;
      
      // Get viewport bounds
      const mapBounds = map.getBounds();
      const viewportBounds = {
        west: mapBounds.getWest(),
        east: mapBounds.getEast(),
        south: mapBounds.getSouth(),
        north: mapBounds.getNorth()
      };
      
      // First pass: count total values and collect tile refs
      // Only use tiles within 1 zoom level of current view to avoid mixing aggregation levels
      const currentZoom = Math.round(map.getZoom());
      let totalCount = 0;
      const viewportTiles = [];
      
      for (const [tileKey, data] of Object.entries(tileData)) {
        const [z, x, y] = tileKey.split('/').map(Number);
        
        // Skip tiles from different zoom levels to avoid double-counting
        if (Math.abs(z - currentZoom) > 1) continue;
        
        const tileBounds = tileToBounds(x, y, z);
        
        if (boundsIntersect(tileBounds, viewportBounds)) {
          viewportTiles.push(data);
          totalCount += data.length;
        }
      }
      
      if (totalCount < 10) return null;
      
      // Calculate sampling rate to keep under MAX_SAMPLES
      const sampleRate = Math.min(1, AUTO_DOMAIN_MAX_SAMPLES / totalCount);
      const values = [];
      
      // Collect values with sampling
      for (const data of viewportTiles) {
        for (const item of data) {
          // Sample: skip items based on sample rate
          if (sampleRate < 1 && Math.random() > sampleRate) continue;
          
          const val = item?.[attr] ?? item?.properties?.[attr];
          if (typeof val === 'number' && isFinite(val)) {
            values.push(val);
          }
          
          // Hard cap just in case
          if (values.length >= AUTO_DOMAIN_MAX_SAMPLES) break;
        }
        if (values.length >= AUTO_DOMAIN_MAX_SAMPLES) break;
      }
      
      if (values.length < 10) return null;
      
      // Sort sampled values and use percentiles (2nd-98th)
      values.sort((a, b) => a - b);
      const p2 = Math.floor(values.length * 0.02);
      const p98 = Math.floor(values.length * 0.98);
      const minVal = values[p2];
      const maxVal = values[Math.min(p98, values.length - 1)];
      
      if (minVal >= maxVal) return [minVal - 1, maxVal + 1];
      return [minVal, maxVal];
    }
    
    // Track tile loading for autoDomain
    let autoDomainTilesLoading = 0;
    
    function scheduleAutoDomainUpdate() {
      if (autoDomainTilesLoading > 0) return;  // Still loading
      
      let needsRebuild = false;
      LAYERS_DATA.forEach(l => {
        if (l.isTileLayer && l.hexLayer?.getFillColor?.autoDomain) {
          const newDomain = calculateDomainFromTiles(l.id, l.hexLayer.getFillColor.attr);
          if (newDomain) {
            const old = l.hexLayer.getFillColor._dynamicDomain;
            // Only rebuild if domain changed >5%
            if (!old || Math.abs(newDomain[0] - old[0]) / (old[1] - old[0] || 1) > 0.05 ||
                        Math.abs(newDomain[1] - old[1]) / (old[1] - old[0] || 1) > 0.05) {
              l.hexLayer.getFillColor._dynamicDomain = newDomain;
              needsRebuild = true;
            }
          }
        }
      });
      
      if (needsRebuild) {
        rebuildDeckOverlay();
        updateLegend();
      }
    }

    function parseHexLayerConfigForDeck(config, layerId = null) {
      const out = {};
      for (const [k, v] of Object.entries(config || {})) {
        if (k === '@@type') continue;
        // Skip non-deck.gl properties
        if (['pickable', 'filled', 'stroked', 'extruded', 'opacity', 'coverage', 'lineWidthMinPixels', 'elevationScale'].includes(k)) continue;
        
        if (v && typeof v === 'object' && !Array.isArray(v)) {
          if (v['@@function'] === 'colorContinuous') {
            // For autoDomain: use stored dynamic domain if calculated, otherwise use config default
            let domainToUse = null;
            if (v.autoDomain === true && layerId) {
              // Check if we already calculated a domain for this layer
              const layerData = LAYERS_DATA.find(l => l.id === layerId);
              if (layerData?.hexLayer?.getFillColor?._dynamicDomain) {
                domainToUse = layerData.hexLayer.getFillColor._dynamicDomain;
              }
            }
            out[k] = colorContinuous(processColorContinuousCfg(v, domainToUse));
          } else if (v['@@function'] === 'colorCategories') {
            // colorCategories - create accessor function
            const { attr, categories, colors, nullColor } = v;
            out[k] = (obj) => {
              const val = obj?.properties?.[attr] ?? obj?.[attr];
              if (val == null) return nullColor || [184, 184, 184];
              const idx = categories?.indexOf(val);
              if (idx >= 0 && colors?.[idx]) {
                const c = colors[idx];
                return Array.isArray(c) ? c : [128, 128, 128];
              }
              return nullColor || [184, 184, 184];
            };
          } else {
            out[k] = v;
          }
        } else if (typeof v === 'string' && v.startsWith('@@=')) {
          const code = v.slice(3);
          out[k] = (obj) => {
            try {
              const fn = new Function('object', `const properties = object?.properties || object || {}; return (${code});`);
              return fn(obj);
            } catch (e) { return null; }
          };
        } else if (Array.isArray(v) && (k === 'getFillColor' || k === 'getLineColor')) {
          // Static color array - return as-is for Deck.gl
          out[k] = v;
        } else {
          out[k] = v;
        }
      }
      return out;
    }

    function buildTileLayer(layerDef, beforeId = null) {
      const tileUrl = layerDef.tileUrl;
      const tileCfg = layerDef.tileLayerConfig || {};
      const rawHexLayer = layerDef.hexLayer || {};
      const hexCfg = parseHexLayerConfigForDeck(rawHexLayer, layerDef.id);
      const visible = layerVisibility[layerDef.id];
      const layerOpacity = (typeof rawHexLayer.opacity === 'number') ? rawHexLayer.opacity : 0.8;
      const isExtruded = rawHexLayer.extruded === true;
      const elevationScale = rawHexLayer.elevationScale || 1;
      const coverage = (typeof rawHexLayer.coverage === 'number') ? rawHexLayer.coverage : 0.9;
      const lineWidth = rawHexLayer.lineWidthMinPixels || 1;
      const isStroked = rawHexLayer.stroked !== false;
      const isFilled = rawHexLayer.filled !== false;

      // Check if this layer has autoDomain enabled
      const hasAutoDomain = rawHexLayer.getFillColor?.autoDomain === true;
      
      // Always initialize tile data store for debug panel attribute detection
      if (!TILE_DATA_STORE[layerDef.id]) {
        TILE_DATA_STORE[layerDef.id] = {};
      }
      
      // Include domain in layer ID to force sublayer recreation when domain changes
      // This prevents visible tile boundaries due to different domains per tile
      const dynamicDomain = rawHexLayer.getFillColor?._dynamicDomain;
      const domainHash = dynamicDomain ? `${dynamicDomain[0].toFixed(2)}-${dynamicDomain[1].toFixed(2)}` : 'default';

      const layerProps = {
        id: `${layerDef.id}-tiles-${domainHash}`,
        data: tileUrl,
        tileSize: tileCfg.tileSize ?? 256,
        minZoom: tileCfg.minZoom ?? 0,
        maxZoom: tileCfg.maxZoom ?? 12,
        pickable: true,
        visible: visible,
        maxRequests: 6,
        refinementStrategy: 'best-available',
        getTileData: async ({ index, signal }) => {
          const { x, y, z } = index;
          const url = tileUrl.replace('{z}', z).replace('{x}', x).replace('{y}', y);
          const cacheKey = `${tileUrl}|${z}|${x}|${y}`;
          const tileKey = `${z}/${x}/${y}`;
          
          // Return cached data immediately
          if (TILE_CACHE.has(cacheKey)) {
            const cachedData = TILE_CACHE.get(cacheKey);
            TILE_DATA_STORE[layerDef.id][tileKey] = cachedData;
            return cachedData;
          }
          
          // Track loading for autoDomain and loader UI
          if (hasAutoDomain) autoDomainTilesLoading++;
          updateTileLoader(1);
          
          try {
            const res = await fetch(url, { signal });
            if (!res.ok) {
              if (hasAutoDomain) {
                autoDomainTilesLoading--;
                setTimeout(scheduleAutoDomainUpdate, 100);
              }
              updateTileLoader(-1);
              return [];
            }
            let text = await res.text();
            text = text.replace(/\"(hex|h3|index)\"\s*:\s*(\d+)/gi, (_m, k, d) => `"${k}":"${d}"`);
            const data = JSON.parse(text);
            const normalized = normalizeTileData(data);
            TILE_CACHE.set(cacheKey, normalized);
            TILE_DATA_STORE[layerDef.id][tileKey] = normalized;
            
            // Track tile loaded for autoDomain
            if (hasAutoDomain) {
              autoDomainTilesLoading--;
              setTimeout(scheduleAutoDomainUpdate, 100);
            }
            updateTileLoader(-1);
            return normalized;
          } catch (e) {
            if (hasAutoDomain) {
              autoDomainTilesLoading--;
              setTimeout(scheduleAutoDomainUpdate, 100);
            }
            updateTileLoader(-1);
            return TILE_CACHE.get(cacheKey) || [];
          }
        },
        renderSubLayers: (props) => {
          const data = props.data || [];
          if (!data.length) return null;
          if (H3HexagonLayer) {
            return new H3HexagonLayer({
              id: `${props.id}-h3`,
              data,
              getHexagon: d => d.hex,
              ...hexCfg,
              // Explicit overrides (must come AFTER hexCfg spread)
              pickable: true,
              stroked: isStroked,
              filled: isFilled,
              extruded: isExtruded,
              elevationScale: elevationScale,
              coverage: coverage,
              lineWidthMinPixels: lineWidth,
              opacity: layerOpacity
            });
          }
          return null;
        }
      };
      
      // Add beforeId for z-order control in interleaved mode
      if (beforeId) {
        layerProps.beforeId = beforeId;
      }
      
      return new TileLayer(layerProps);
    }

    function rebuildDeckOverlay() {
      const tileLayers = LAYERS_DATA
        .filter(l => l.layerType === 'hex' && l.isTileLayer && layerVisibility[l.id]);
      
      if (tileLayers.length === 0) {
      if (deckOverlay) {
        try {
          map.removeControl(deckOverlay);
          deckOverlay.finalize?.();
        } catch (e) {}
        deckOverlay = null;
        }
        return;
      }
      
      // Determine the bottom-most Mapbox layer belonging to our static layers
      const allMapboxLayers = map.getStyle()?.layers || [];
      let bottomMostLayerId = null;
      for (const mapLayer of allMapboxLayers) {
        const isOurLayer = LAYERS_DATA.some(l => 
          !l.isTileLayer && (
            mapLayer.id === `${l.id}-fill` ||
            mapLayer.id === `${l.id}-outline` ||
            mapLayer.id === `${l.id}-line` ||
            mapLayer.id === `${l.id}-circle` ||
            mapLayer.id === `${l.id}-extrusion`
          )
        );
        if (isOurLayer) {
          bottomMostLayerId = mapLayer.id;
          break;
        }
      }
      
      // Reverse to keep UI order consistent (top of menu renders on top)
      const deckLayers = tileLayers
        .slice()
        .reverse()
        .map(l => buildTileLayer(l, bottomMostLayerId));
      
      // Fast-path: update overlay in-place to avoid flicker during rapid edits
      if (deckOverlay) {
        try {
          deckOverlay.setProps({ layers: deckLayers });
        } catch (e) {
          // Fallback: recreate if anything goes wrong
          try { map.removeControl(deckOverlay); deckOverlay.finalize?.(); } catch (e2) {}
          deckOverlay = null;
        }
      }
      if (!deckOverlay) {
        deckOverlay = new MapboxOverlay({
          interleaved: true,
          layers: deckLayers
        });
        map.addControl(deckOverlay);
      }

        if (bottomMostLayerId) {
          const moveOverlay = () => {
            const overlayLayerId = deckOverlay?._mapboxLayer?.id;
            if (overlayLayerId) {
              try { map.moveLayer(overlayLayerId, bottomMostLayerId); } catch (e) {}
            }
          };
          setTimeout(moveOverlay, 0);
          map.once('idle', moveOverlay);
      }
    }
    
    // Auto-domain viewport listener: rebuild when viewport changes
    const hasAutoDomainLayers = LAYERS_DATA.some(l => 
      l.layerType === 'hex' && 
      l.isTileLayer && 
      l.hexLayer?.getFillColor?.autoDomain === true
    );
    
    if (hasAutoDomainLayers) {
      let autoDomainTimeout = null;
      map.on('moveend', () => {
        clearTimeout(autoDomainTimeout);
        autoDomainTimeout = setTimeout(scheduleAutoDomainUpdate, 800);
      });
      map.once('idle', () => setTimeout(scheduleAutoDomainUpdate, 2000));
    }
    {% endif %}

    // ========== Add All Layers ==========
    // Fast in-place updates (no remove/re-add) to reduce flicker while typing/dragging controls.
    // Falls back to addAllLayers() if a required layer doesn't exist.
    function updateMapboxLayersFast() {
      let ok = true;
      const safePaint = (layerId, key, val) => {
        try { if (map.getLayer(layerId)) map.setPaintProperty(layerId, key, val); else ok = false; } catch (e) { ok = false; }
      };
      const safeLayout = (layerId, key, val) => {
        try { if (map.getLayer(layerId)) map.setLayoutProperty(layerId, key, val); else ok = false; } catch (e) { ok = false; }
      };
      const safeRemove = (layerId) => { try { if (map.getLayer(layerId)) map.removeLayer(layerId); } catch (e) {} };

      // Match addAllLayers() ordering logic, but only update what's already there.
      const renderOrder = [...LAYERS_DATA].reverse();
      renderOrder.forEach((l) => {
        if (l.layerType === 'hex' && l.isTileLayer) return; // Deck handles tile layers

        const visible = layerVisibility[l.id];
        const vis = visible ? 'visible' : 'none';

        if (l.layerType === 'mvt') {
          // MVT style updates are not currently driven by the debug form; skip.
          return;
        }

        // If source missing, we can't update safely.
        if (!map.getSource(l.id)) { ok = false; return; }

        if (l.layerType === 'hex') {
          const cfg = l.hexLayer || {};
          const fillColor = Array.isArray(cfg.getFillColor) ? toRgba(cfg.getFillColor, 0.8) : buildColorExpr(cfg.getFillColor, l.data) || 'rgba(0,144,255,0.7)';
          const lineColor = cfg.getLineColor ? (Array.isArray(cfg.getLineColor) ? toRgba(cfg.getLineColor, 1) : buildColorExpr(cfg.getLineColor, l.data)) : 'rgba(255,255,255,0.3)';
          const layerOpacity = (typeof cfg.opacity === 'number' && isFinite(cfg.opacity)) ? Math.max(0, Math.min(1, cfg.opacity)) : 0.8;
          const lineW = (typeof cfg.lineWidthMinPixels === 'number' && isFinite(cfg.lineWidthMinPixels)) ? cfg.lineWidthMinPixels : 0.5;

          if (cfg.extruded) {
            // Outline existence is structural; remove if disabled, otherwise update if present.
            if (cfg.stroked === false) {
              safeRemove(`${l.id}-outline`);
            } else {
              safePaint(`${l.id}-outline`, 'line-color', lineColor);
              safePaint(`${l.id}-outline`, 'line-width', lineW);
              safeLayout(`${l.id}-outline`, 'visibility', vis);
            }
            const elev = cfg.elevationScale || 1;
            safePaint(`${l.id}-extrusion`, 'fill-extrusion-color', fillColor);
            safePaint(`${l.id}-extrusion`, 'fill-extrusion-height', cfg.getFillColor?.attr ? ['*', ['get', cfg.getFillColor.attr], elev] : 100);
            safePaint(`${l.id}-extrusion`, 'fill-extrusion-opacity', layerOpacity);
            safeLayout(`${l.id}-extrusion`, 'visibility', vis);
          } else {
            if (cfg.filled === false) {
              safeRemove(`${l.id}-fill`);
            } else {
              safePaint(`${l.id}-fill`, 'fill-color', fillColor);
              safePaint(`${l.id}-fill`, 'fill-opacity', layerOpacity);
              safeLayout(`${l.id}-fill`, 'visibility', vis);
            }
            if (cfg.stroked === false) {
              safeRemove(`${l.id}-outline`);
            } else {
              safePaint(`${l.id}-outline`, 'line-color', lineColor);
              safePaint(`${l.id}-outline`, 'line-width', lineW);
              safeLayout(`${l.id}-outline`, 'visibility', vis);
            }
            // Ensure extrusion layer doesn't linger
            safeRemove(`${l.id}-extrusion`);
          }
          return;
        }

        if (l.layerType === 'vector') {
          const vecData = l.geojson?.features?.map(f => f.properties) || [];
          const fillColorExpr = l.fillColorConfig?.['@@function'] ? buildColorExpr(l.fillColorConfig, vecData) : (l.fillColorRgba || 'rgba(0,144,255,0.6)');
          const lineColorExpr = l.lineColorConfig?.['@@function'] ? buildColorExpr(l.lineColorConfig, vecData) : (l.lineColorRgba || 'rgba(100,100,100,0.8)');
          const lineW = (typeof l.lineWidth === 'number' && isFinite(l.lineWidth)) ? l.lineWidth : 1;
          const layerOpacity = (typeof l.opacity === 'number' && isFinite(l.opacity)) ? Math.max(0, Math.min(1, l.opacity)) : 0.8;

          // Only update layers if they exist; don't try to infer geometry type here.
          if (l.isFilled === false) {
            safeRemove(`${l.id}-fill`);
          } else {
            if (map.getLayer(`${l.id}-fill`)) {
              safePaint(`${l.id}-fill`, 'fill-color', fillColorExpr);
              safePaint(`${l.id}-fill`, 'fill-opacity', layerOpacity);
              safeLayout(`${l.id}-fill`, 'visibility', vis);
            }
          }
          if (l.isStroked === false) {
            safeRemove(`${l.id}-outline`);
            safeRemove(`${l.id}-line`);
          } else {
            if (map.getLayer(`${l.id}-outline`)) {
              safePaint(`${l.id}-outline`, 'line-color', lineColorExpr);
              safePaint(`${l.id}-outline`, 'line-width', lineW);
              safeLayout(`${l.id}-outline`, 'visibility', vis);
            }
            if (map.getLayer(`${l.id}-line`)) {
              safePaint(`${l.id}-line`, 'line-color', lineColorExpr);
              safePaint(`${l.id}-line`, 'line-width', lineW);
              safeLayout(`${l.id}-line`, 'visibility', vis);
            }
          }
          if (map.getLayer(`${l.id}-circle`)) {
            // Keep points consistent with fill settings
            safePaint(`${l.id}-circle`, 'circle-color', fillColorExpr);
            safeLayout(`${l.id}-circle`, 'visibility', vis);
          }
          return;
        }
      });

      return ok;
    }

    function addAllLayers() {
      // Remove existing layers
      LAYERS_DATA.forEach(l => {
        [`${l.id}-fill`, `${l.id}-extrusion`, `${l.id}-outline`, `${l.id}-circle`, `${l.id}-line`].forEach(id => { 
          try { if(map.getLayer(id)) map.removeLayer(id); } catch(e){} 
        });
        try { if(map.getSource(l.id)) map.removeSource(l.id); } catch(e){}
      });
      
      // Add layers in reverse menu order so top of menu renders on top of map
      const renderOrder = [...LAYERS_DATA].reverse();
      renderOrder.forEach((l) => {
        if (l.layerType === 'hex' && l.isTileLayer) return;  // Skip hex tile layers - handled by Deck.gl
        
        const visible = layerVisibility[l.id];
        
        // MVT layers create their own source, skip GeoJSON check
        if (l.layerType === 'mvt') {
          // Handled below in the mvt section
        } else {
        const geojson = layerGeoJSONs[l.id];
        if (!geojson || !geojson.features?.length) return;
        map.addSource(l.id, { type: 'geojson', data: geojson });
        }
        
        if (l.layerType === 'hex') {
          // Hex layer rendering
          const cfg = l.hexLayer || {};
          const fillColor = Array.isArray(cfg.getFillColor) ? toRgba(cfg.getFillColor, 0.8) : buildColorExpr(cfg.getFillColor, l.data) || 'rgba(0,144,255,0.7)';
          const lineColor = cfg.getLineColor ? (Array.isArray(cfg.getLineColor) ? toRgba(cfg.getLineColor, 1) : buildColorExpr(cfg.getLineColor, l.data)) : 'rgba(255,255,255,0.3)';
          const layerOpacity = (typeof cfg.opacity === 'number' && isFinite(cfg.opacity)) ? Math.max(0, Math.min(1, cfg.opacity)) : 0.8;
          
          if (cfg.extruded) {
            // If stroked is enabled, add outline FIRST so it doesn't sit on top of the 3D extrusion.
            if (cfg.stroked !== false) {
              map.addLayer({ 
                id: `${l.id}-outline`, 
                type: 'line', 
                source: l.id, 
                paint: { 'line-color': lineColor, 'line-width': (typeof cfg.lineWidthMinPixels === 'number' && isFinite(cfg.lineWidthMinPixels)) ? cfg.lineWidthMinPixels : 0.5 },
                layout: { 'visibility': visible ? 'visible' : 'none' }
              });
            }
            const elev = cfg.elevationScale || 1;
            map.addLayer({ 
              id: `${l.id}-extrusion`, 
              type: 'fill-extrusion', 
              source: l.id, 
              paint: { 
                'fill-extrusion-color': fillColor, 
                'fill-extrusion-height': cfg.getFillColor?.attr ? ['*', ['get', cfg.getFillColor.attr], elev] : 100,
                'fill-extrusion-base': 0,
                'fill-extrusion-opacity': layerOpacity
              },
              layout: { 'visibility': visible ? 'visible' : 'none' }
            });
          } else {
            if (cfg.filled !== false) {
              map.addLayer({ 
                id: `${l.id}-fill`, 
                type: 'fill', 
                source: l.id, 
                paint: { 'fill-color': fillColor, 'fill-opacity': layerOpacity },
                layout: { 'visibility': visible ? 'visible' : 'none' }
              });
            }
            // Flat mode: outline should be on top of fill
            if (cfg.stroked !== false) {
          map.addLayer({ 
            id: `${l.id}-outline`, 
            type: 'line', 
            source: l.id, 
            paint: { 'line-color': lineColor, 'line-width': (typeof cfg.lineWidthMinPixels === 'number' && isFinite(cfg.lineWidthMinPixels)) ? cfg.lineWidthMinPixels : 0.5 },
            layout: { 'visibility': visible ? 'visible' : 'none' }
          });
            }
          }
          
        } else if (l.layerType === 'vector') {
          // Vector layer rendering
          const vecData = l.geojson?.features?.map(f => f.properties) || [];
          const builtFillColor = l.fillColorConfig?.['@@function'] ? buildColorExpr(l.fillColorConfig, vecData) : null;
          const fillColorExpr = builtFillColor || l.fillColorRgba || 'rgba(0,144,255,0.6)';
          const lineColorExpr = l.lineColorConfig?.['@@function'] ? buildColorExpr(l.lineColorConfig, vecData) : (l.lineColorRgba || 'rgba(100,100,100,0.8)');
          const lineW = (typeof l.lineWidth === 'number' && isFinite(l.lineWidth)) ? l.lineWidth : 1;
          const layerOpacity = (typeof l.opacity === 'number' && isFinite(l.opacity)) ? Math.max(0, Math.min(1, l.opacity)) : 0.8;
          
          // Detect geometry types
          let hasPoly = false, hasPoint = false, hasLine = false;
          for (const f of l.geojson?.features || []) {
            const t = f.geometry?.type;
            if (t === 'Point' || t === 'MultiPoint') hasPoint = true;
            if (t === 'Polygon' || t === 'MultiPolygon') hasPoly = true;
            if (t === 'LineString' || t === 'MultiLineString') hasLine = true;
          }
          
          if (hasPoly) {
            if (l.isFilled !== false) {
              map.addLayer({ 
                id: `${l.id}-fill`, 
                type: 'fill', 
                source: l.id, 
                paint: { 'fill-color': fillColorExpr, 'fill-opacity': layerOpacity },
                filter: ['any', ['==', ['geometry-type'], 'Polygon'], ['==', ['geometry-type'], 'MultiPolygon']],
                layout: { 'visibility': visible ? 'visible' : 'none' }
              });
            }
            if (l.isStroked !== false) {
              map.addLayer({ 
                id: `${l.id}-outline`, 
                type: 'line', 
                source: l.id, 
                layout: { 'line-join': 'round', 'line-cap': 'round', 'visibility': visible ? 'visible' : 'none' },
                paint: { 'line-color': lineColorExpr, 'line-width': lineW },
                filter: ['any', ['==', ['geometry-type'], 'Polygon'], ['==', ['geometry-type'], 'MultiPolygon']]
              });
            }
          }
          if (hasLine) {
            map.addLayer({ 
              id: `${l.id}-line`, 
              type: 'line', 
              source: l.id, 
              layout: { 'line-join': 'round', 'line-cap': 'round', 'visibility': visible ? 'visible' : 'none' },
              paint: { 'line-color': lineColorExpr, 'line-width': lineW, 'line-opacity': 1 },
              filter: ['any', ['==', ['geometry-type'], 'LineString'], ['==', ['geometry-type'], 'MultiLineString']]
            });
          }
          if (hasPoint) {
            map.addLayer({ 
              id: `${l.id}-circle`, 
              type: 'circle', 
              source: l.id, 
              paint: { 
                'circle-radius': l.pointRadius || 6, 
                'circle-color': fillColorExpr, 
                'circle-stroke-color': 'rgba(0,0,0,0.5)', 
                'circle-stroke-width': 1, 
                'circle-opacity': 0.9 
              },
              filter: ['any', ['==', ['geometry-type'], 'Point'], ['==', ['geometry-type'], 'MultiPoint']],
              layout: { 'visibility': visible ? 'visible' : 'none' }
            });
          }
        } else if (l.layerType === 'mvt') {
          // MVT (Mapbox Vector Tile) layer rendering
          map.addSource(l.id, {
            type: 'vector',
            tiles: [l.tileUrl],
            minzoom: l.minzoom || 0,
            maxzoom: l.maxzoom || 22,
          });
          
          // Build dynamic color expressions if configured
          const fillColorExpr = l.fillColorConfig?.['@@function'] 
            ? buildColorExpr(l.fillColorConfig, [])  // MVT has no static data, buildColorExpr uses property accessors
            : (l.fillColor || '#FFF5CC');
          const lineColorExpr = l.lineColorConfig?.['@@function']
            ? buildColorExpr(l.lineColorConfig, [])
            : (l.lineColor || '#FFFFFF');
          
          if (l.isExtruded) {
            // 3D extruded layer
            const heightExpr = l.heightProperty
              ? ['*', ['coalesce', ['get', l.heightProperty], ['get', 'h'], 10], l.heightMultiplier || 1]
              : 10;
            
            map.addLayer({
              id: `${l.id}-extrusion`,
              type: 'fill-extrusion',
              source: l.id,
              'source-layer': l.sourceLayer || 'udf',
              minzoom: l.minzoom || 0,
              paint: {
                'fill-extrusion-color': fillColorExpr,
                'fill-extrusion-height': heightExpr,
                'fill-extrusion-base': 0,
                'fill-extrusion-opacity': l.extrusionOpacity || 0.9,
              },
              layout: { 'visibility': visible ? 'visible' : 'none' }
            });
          } else {
            // Flat fill layer (if filled)
            if (l.isFilled !== false) {
              map.addLayer({
                id: `${l.id}-fill`,
                type: 'fill',
                source: l.id,
                'source-layer': l.sourceLayer || 'udf',
                minzoom: l.minzoom || 0,
                paint: {
                  'fill-color': fillColorExpr,
                  'fill-opacity': l.fillOpacity || 0.8,
                },
                layout: { 'visibility': visible ? 'visible' : 'none' }
              });
            }
            
            // Outline layer
            if (l.lineWidth > 0) {
              map.addLayer({
                id: `${l.id}-outline`,
                type: 'line',
                source: l.id,
                'source-layer': l.sourceLayer || 'udf',
                minzoom: l.minzoom || 0,
                paint: {
                  'line-color': lineColorExpr,
                  'line-width': l.lineWidth || 1,
                },
                layout: { 'visibility': visible ? 'visible' : 'none' }
              });
            }
          }
        }
      });

      {% if has_tile_layers %}
      rebuildDeckOverlay();
      {% endif %}
    }

    // ========== Layer Visibility Toggle ==========
    window.toggleLayerVisibility = function(layerId, visible) {
      layerVisibility[layerId] = visible;
      
      const layerDef = LAYERS_DATA.find(l => l.id === layerId);
      if (layerDef && layerDef.layerType === 'hex' && layerDef.isTileLayer) {
        {% if has_tile_layers %}
        rebuildDeckOverlay();
        {% endif %}
      } else {
        // Handle MVT and regular layers
        [`${layerId}-fill`, `${layerId}-extrusion`, `${layerId}-outline`, `${layerId}-circle`, `${layerId}-line`].forEach(id => {
          try { 
            if(map.getLayer(id)) {
              map.setLayoutProperty(id, 'visibility', visible ? 'visible' : 'none');
            }
          } catch(e){}
        });
      }
      updateLegend();
      updateLayerPanel();
    };

    // Clicking anywhere on the layer row toggles visibility (same as the eye icon).
    // The eye icon itself stops propagation so we don't double-toggle.
    window.onLayerItemClick = function(e, layerId) {
      try {
        if (e && e.target && e.target.closest && e.target.closest('.layer-eye')) return;
      } catch (_) {}
      const cur = !!layerVisibility[layerId];
      toggleLayerVisibility(layerId, !cur);
    };

    // ========== Layer Panel ==========
    function updateLayerPanel() {
      const list = document.getElementById('layer-list');
      if (!list) return;
      
      list.innerHTML = LAYERS_DATA.map(l => {
        const visible = layerVisibility[l.id];
        let stripBg = '#0090ff';

        const toGradient = (cols) => {
          if (!cols || !cols.length) return null;
          if (cols.length === 1) return cols[0];
          const stops = cols.map((c, i) => `${c} ${(i / (cols.length - 1)) * 100}%`).join(', ');
          return `linear-gradient(to bottom, ${stops})`;
        };
        
        if (l.layerType === 'hex') {
          const cfg = l.hexLayer || {};
          const colorCfg = (cfg.filled === false && cfg.getLineColor) ? cfg.getLineColor : cfg.getFillColor;
          if (Array.isArray(colorCfg)) {
            stripBg = toRgba(colorCfg, 1) || stripBg;
          } else if (colorCfg?.['@@function'] === 'colorContinuous' || colorCfg?.['@@function'] === 'colorCategories') {
            const paletteName = colorCfg.colors || (colorCfg['@@function'] === 'colorCategories' ? 'Bold' : 'ArmyRose');
            let cols = getPaletteColors(paletteName, colorCfg.steps || 7);
            if (Array.isArray(cols) && colorCfg?.['@@function'] === 'colorContinuous') {
              const dom = colorCfg.domain;
              const domainReversed = Array.isArray(dom) && dom.length >= 2 && dom[0] > dom[dom.length - 1];
              const wantsReverse = !!colorCfg.reverse;
              const shouldReverse = domainReversed ? !wantsReverse : wantsReverse;
              if (shouldReverse) cols = [...cols].reverse();
            }
            stripBg = toGradient(cols) || stripBg;
          }
        } else if (l.layerType === 'vector') {
          // Prefer line config when stroke-only / fully transparent fill
          const hasFillFunc = l.fillColorConfig?.['@@function'];
          const hasLineFunc = l.lineColorConfig?.['@@function'];
          const useLine = hasLineFunc && (!hasFillFunc || !l.isFilled || l.opacity === 0);
          const cfg = useLine ? l.lineColorConfig : l.fillColorConfig;

          if (!cfg?.['@@function']) {
            if (useLine && l.lineColorRgba) stripBg = l.lineColorRgba;
            else if (l.fillColorRgba) stripBg = l.fillColorRgba;
            else if (l.lineColorRgba) stripBg = l.lineColorRgba;
          } else if (cfg?.colors) {
            let cols = getPaletteColors(cfg.colors, cfg.steps || 7);
            if (Array.isArray(cols) && cfg?.['@@function'] === 'colorContinuous') {
              const dom = cfg.domain;
              const domainReversed = Array.isArray(dom) && dom.length >= 2 && dom[0] > dom[dom.length - 1];
              const wantsReverse = !!cfg.reverse;
              const shouldReverse = domainReversed ? !wantsReverse : wantsReverse;
              if (shouldReverse) cols = [...cols].reverse();
            }
            stripBg = toGradient(cols) || stripBg;
          }
        }
        
        const eyeIcon = visible 
          ? '<svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M12 4.5C7 4.5 2.73 7.61 1 12c1.73 4.39 6 7.5 11 7.5s9.27-3.11 11-7.5c-1.73-4.39-6-7.5-11-7.5zM12 17c-2.76 0-5-2.24-5-5s2.24-5 5-5 5 2.24 5 5-2.24 5-5 5zm0-8c-1.66 0-3 1.34-3 3s1.34 3 3 3 3-1.34 3-3-1.34-3-3-3z"/></svg>'
          : '<svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M12 7c2.76 0 5 2.24 5 5 0 .65-.13 1.26-.36 1.83l2.92 2.92c1.51-1.26 2.7-2.89 3.43-4.75-1.73-4.39-6-7.5-11-7.5-1.4 0-2.74.25-3.98.7l2.16 2.16C10.74 7.13 11.35 7 12 7zM2 4.27l2.28 2.28.46.46C3.08 8.3 1.78 10.02 1 12c1.73 4.39 6 7.5 11 7.5 1.55 0 3.03-.3 4.38-.84l.42.42L19.73 22 21 20.73 3.27 3 2 4.27zM7.53 9.8l1.55 1.55c-.05.21-.08.43-.08.65 0 1.66 1.34 3 3 3 .22 0 .44-.03.65-.08l1.55 1.55c-.67.33-1.41.53-2.2.53-2.76 0-5-2.24-5-5 0-.79.2-1.53.53-2.2zm4.31-.78l3.15 3.15.02-.16c0-1.66-1.34-3-3-3l-.17.01z"/></svg>';
        return `
          <div class="layer-item ${visible ? '' : 'disabled'}" data-layer-id="${l.id}" style="--layer-strip: ${stripBg};" onclick="onLayerItemClick(event, '${l.id}')">
            <span class="layer-name">${l.name}</span>
            <span class="layer-eye" onclick="event.stopPropagation(); toggleLayerVisibility('${l.id}', ${!visible})">${eyeIcon}</span>
          </div>
        `;
      }).join('');
    }

    // ========== Legend ==========
    function updateLegend() {
      const legend = document.getElementById('color-legend');
      if (!legend) return;
      
      const visibleLayers = LAYERS_DATA.filter(l => layerVisibility[l.id]);
      if (!visibleLayers.length) {
        legend.style.display = 'none';
        return;
      }
      
      let html = '';
      visibleLayers.forEach(l => {
        let colorCfg = null;
        
        if (l.layerType === 'hex') {
          const cfg = l.hexLayer || {};
          colorCfg = (cfg.filled === false && cfg.getLineColor) ? cfg.getLineColor : cfg.getFillColor;
        } else if (l.layerType === 'vector') {
          // Prefer line color config if fill doesn't have a color function, or if not filled/transparent
          const hasFillFunc = l.fillColorConfig?.['@@function'];
          const hasLineFunc = l.lineColorConfig?.['@@function'];
          
          if (hasLineFunc && (!hasFillFunc || !l.isFilled || l.opacity === 0)) {
            colorCfg = l.lineColorConfig;
          } else {
          colorCfg = l.fillColorConfig;
          }
          
          // Show simple line legend for stroke-only vector layers with static color
          if (!colorCfg?.['@@function'] && l.lineColorRgba && !l.isFilled) {
            html += `
              <div class="legend-layer">
                <div class="legend-title">${l.name}</div>
              </div>
            `;
            return;
          }
        }
        
        // Only show legend for layers with explicit color functions
        const fnType = colorCfg?.['@@function'];
        if (!fnType || !colorCfg?.attr) return;
        if (fnType !== 'colorContinuous' && fnType !== 'colorCategories') return;
        const paletteName = colorCfg.colors || (fnType === 'colorCategories' ? 'Bold' : 'ArmyRose');
        
        // Handle categorical legend
        if (fnType === 'colorCategories') {
          // Use detected categories or provided ones
          let catPairs = colorCfg._detectedCategories || colorCfg.categories || [];
          // Normalize to {value, label} format
          catPairs = catPairs.map(c => typeof c === 'object' && c.label ? c : { value: c, label: c });
          if (!catPairs.length) return;
          
          let cols = getPaletteColors(paletteName, Math.max(catPairs.length, 3));
          if (!cols || !cols.length) {
            cols = ['#7F3C8D','#11A579','#3969AC','#F2B701','#E73F74','#80BA5A','#E68310','#008695','#CF1C90','#f97b72'];
          }
          
          // Use labelAttr name in title if available
          const titleAttr = colorCfg.labelAttr || colorCfg.attr;
          
          html += `
            <div class="legend-layer">
              <div class="legend-title">${l.name}: ${titleAttr}</div>
              <div class="legend-categories">
                ${catPairs.map((cat, i) => `
                  <div class="legend-cat-item">
                    <div class="legend-cat-swatch" style="background:${cols[i % cols.length]};"></div>
                    <span class="legend-cat-label" title="${cat.label}">${cat.label}</span>
                  </div>
                `).join('')}
              </div>
            </div>
          `;
          return;
        }
        
        // Handle continuous legend
        if (fnType !== 'colorContinuous' || !colorCfg.domain?.length) return;
        
        // Use dynamic domain if available (from autoDomain calculation)
        const domain = colorCfg._dynamicDomain || colorCfg.domain;
        const [d0, d1] = domain;
        const domainReversed = d0 > d1;
        const steps = colorCfg.steps || 7;
        
        let cols = getPaletteColors(paletteName, steps);
        if (!cols || !cols.length) {
          cols = ['#e0f3db','#ccebc5','#a8ddb5','#7bccc4','#4eb3d3','#2b8cbe','#0868ac','#084081'];
        }
        const wantsReverse = !!colorCfg.reverse;
        const shouldReverse = domainReversed ? !wantsReverse : wantsReverse;
        if (shouldReverse) cols = [...cols].reverse();
        
        const gradient = `linear-gradient(to right, ${cols.map((c, i) => `${c} ${i / (cols.length - 1) * 100}%`).join(', ')})`;
        
        html += `
          <div class="legend-layer">
            <div class="legend-title">${l.name}: ${colorCfg.attr}</div>
            <div class="legend-gradient" style="background:${gradient};"></div>
            <div class="legend-labels">
              <span>${d0.toFixed(1)}</span>
              <span>${d1.toFixed(1)}</span>
            </div>
          </div>
        `;
      });
      
      if (html) {
        legend.innerHTML = html;
        legend.style.display = 'block';
      } else {
        legend.style.display = 'none';
      }
    }

    // ========== Initialization ==========
    let layersReady = false;
    let autoFitDone = false;
    
    function tryInit() {
      if (layersReady) return;
      
      const needsCartocolor = LAYERS_DATA.some(l => {
        if (l.layerType === 'hex') {
          const cfg = l.hexLayer || {};
          return cfg.getFillColor?.['@@function'] === 'colorContinuous' || cfg.getLineColor?.['@@function'] === 'colorContinuous';
        } else if (l.layerType === 'vector') {
          return l.fillColorConfig?.['@@function'] === 'colorContinuous' || l.lineColorConfig?.['@@function'] === 'colorContinuous';
        }
        return false;
      });
      
      if (needsCartocolor && !window.cartocolor) {
        setTimeout(tryInit, 50);
        return;
      }
      
      layersReady = true;
      addAllLayers();
      updateLayerPanel();
      updateLegend();
      
      // Auto-fit to combined bounds
      if (!HAS_CUSTOM_VIEW && !autoFitDone) {
        const bounds = new mapboxgl.LngLatBounds();
        LAYERS_DATA.forEach(l => {
          if (l.layerType === 'hex' && l.isTileLayer) return;
          const geojson = layerGeoJSONs[l.id];
          if (geojson && geojson.features && geojson.features.length) {
            geojson.features.forEach(f => {
              if (f.geometry.type === 'Point') {
                bounds.extend(f.geometry.coordinates);
              } else if (f.geometry.type === 'Polygon' || f.geometry.type === 'MultiPolygon') {
                const coords = f.geometry.type === 'Polygon' ? [f.geometry.coordinates] : f.geometry.coordinates;
                coords.forEach(poly => poly[0]?.forEach(c => bounds.extend(c)));
              } else if (f.geometry.type === 'LineString') {
                f.geometry.coordinates.forEach(c => bounds.extend(c));
              }
            });
          }
        });
        if (!bounds.isEmpty()) {
          map.fitBounds(bounds, { padding: 50, maxZoom: 15, duration: 500, pitch: {{ pitch }}, bearing: {{ bearing }} });
          autoFitDone = true;
        }
      }
      
      // Setup unified tooltip
      const tt = document.getElementById('tooltip');
      const allQueryableLayers = [];
      LAYERS_DATA.forEach(l => {
        if (l.layerType === 'hex' && l.isTileLayer) return;
        const layerIds = [];
        if (l.layerType === 'hex') {
          const cfg = l.hexLayer || {};
          if (cfg.extruded) {
            layerIds.push(`${l.id}-extrusion`);
          } else {
            layerIds.push(`${l.id}-fill`);
          }
          if (cfg.stroked !== false) layerIds.push(`${l.id}-outline`);
        } else if (l.layerType === 'vector') {
          layerIds.push(`${l.id}-fill`, `${l.id}-outline`, `${l.id}-circle`, `${l.id}-line`);
        } else if (l.layerType === 'mvt') {
          // MVT layers - add fill, outline, and extrusion layer IDs
          if (l.isExtruded) {
            layerIds.push(`${l.id}-extrusion`);
          } else {
            layerIds.push(`${l.id}-fill`);
          }
          layerIds.push(`${l.id}-outline`);
        }
        layerIds.forEach(layerId => {
          allQueryableLayers.push({ layerId, layerDef: l });
        });
      });
      
      // Unified tooltip handler that respects layer order from LAYERS_DATA
      // First layer in LAYERS_DATA = topmost on map
      map.on('mousemove', (e) => { 
        let best = null;
        let bestLayerIndex = Infinity;
        
        // 1) Mapbox layers
        const queryIds = allQueryableLayers
          .map(x => x.layerId)
          .filter(id => map.getLayer(id));
        if (queryIds.length) {
        const features = map.queryRenderedFeatures(e.point, { layers: queryIds });
        for (const f of features) {
          const match = allQueryableLayers.find(x => x.layerId === f.layer.id);
            if (!match) continue;
            if (!layerVisibility[match.layerDef.id]) continue;

            const idx = LAYERS_DATA.findIndex(l => l.id === match.layerDef.id);
            if (idx !== -1 && idx < bestLayerIndex) {
              bestLayerIndex = idx;
              best = { type: 'mapbox', feature: f, layerDef: match.layerDef };
            }
            break; // queryRenderedFeatures returns in z-order; take first visible
          }
        }

      {% if has_tile_layers %}
        // 2) Deck.gl tile layers
        if (deckOverlay) {
        const info = deckOverlay.pickObject({ x: e.point.x, y: e.point.y, radius: 4 });
        if (info?.object) {
            const baseLayerId = info.layer?.id?.split('-tiles-')[0];
            const layerDef = LAYERS_DATA.find(l => l.id === baseLayerId || info.layer?.id?.startsWith(l.id));
          if (layerDef && layerVisibility[layerDef.id]) {
              const idx = LAYERS_DATA.findIndex(l => l.id === layerDef.id);
              if (idx !== -1 && idx < bestLayerIndex) {
                bestLayerIndex = idx;
                best = { type: 'deck', object: info.object, layerDef };
              }
            }
          }
        }
        {% endif %}
        
        if (!best) {
          tt.style.display = 'none';
          map.getCanvas().style.cursor = '';
          return;
        }
        
            map.getCanvas().style.cursor = 'pointer';
        const layerDef = best.layerDef;
            const cols = layerDef.tooltipColumns || [];
        let lines = [];
        
        if (best.type === 'mapbox') {
          const p = best.feature?.properties || {};
          if (p.hex != null) {
            lines.push(`<span class="tt-row"><span class="tt-key">hex</span><span class="tt-val">${String(p.hex).slice(0,12)}...</span></span>`);
          }
          if (cols.length) {
            cols.forEach(k => {
              if (k === 'hex') return;
              const v = p[k];
              if (v == null) return;
              lines.push(`<span class="tt-row"><span class="tt-key">${k}</span><span class="tt-val">${typeof v === 'number' ? v.toFixed(2) : v}</span></span>`);
            });
          } else if (!lines.length) {
            lines = Object.keys(p).slice(0, 5).map(k => `<span class="tt-row"><span class="tt-key">${k}</span><span class="tt-val">${p[k]}</span></span>`);
          }
        } else {
          // Deck.gl tile layer
          const obj = best.object;
          const p = obj?.properties || obj || {};
            const colorAttr = layerDef.hexLayer?.getFillColor?.attr || 'metric';
          const hexVal = p.hex ?? obj?.hex;
          if (hexVal != null) {
            lines.push(`<span class="tt-row"><span class="tt-key">hex</span><span class="tt-val">${String(hexVal).slice(0, 12)}...</span></span>`);
          }
            if (cols.length) {
              cols.forEach(col => {
              if (col === 'hex') return;
              const val = p[col] ?? obj?.[col];
              if (val == null) return;
              lines.push(`<span class="tt-row"><span class="tt-key">${col}</span><span class="tt-val">${typeof val === 'number' ? Number(val).toFixed(2) : val}</span></span>`);
            });
          } else if (p[colorAttr] != null || obj?.[colorAttr] != null) {
            const val = p[colorAttr] ?? obj?.[colorAttr];
              lines.push(`<span class="tt-row"><span class="tt-key">${colorAttr}</span><span class="tt-val">${Number(val).toFixed(2)}</span></span>`);
          }
            }
            
            if (lines.length) {
              tt.innerHTML = `<strong class="tt-title">${layerDef.name}</strong>` + lines.join('');
          tt.style.left = `${e.point.x + 10}px`;
          tt.style.top = `${e.point.y + 10}px`;
              tt.style.display = 'block';
        } else {
          tt.style.display = 'none';
        }
      });
      
      map.on('mouseleave', () => { 
        map.getCanvas().style.cursor = '';
        tt.style.display = 'none'; 
      });
    }

    map.on('load', tryInit);
    map.on('load', () => setTimeout(() => map.resize(), 200));
    window.addEventListener('resize', () => map.resize());
    document.addEventListener('visibilitychange', () => { if (!document.hidden) map.resize(); });
    
    {% if highlight_on_click %}
    // Click-to-highlight (hex and vector)
    (function() {
      const HL_FILL = 'rgba(255,255,0,0.3)', HL_LINE = 'rgba(255,255,0,1)', HL_LW = 3;
      let selected = null, added = false;
      
      // Find original unclipped geometry using _fused_idx
      function findOriginalFeature(clickedFeature, layerId) {
        const layerDef = LAYERS_DATA.find(l => layerId.startsWith(l.id));
        if (!layerDef) return null;
        
        const originalGeoJSON = layerGeoJSONs[layerDef.id];
        if (!originalGeoJSON?.features?.length) return null;
        
        const idx = clickedFeature.properties?._fused_idx;
        if (idx != null && originalGeoJSON.features[idx]) {
          return originalGeoJSON.features[idx];
        }
        return null;
      }
      
      function highlight(feature, layerId = null) {
        let geojson = { type: 'FeatureCollection', features: [] };
        
        if (feature) {
          const props = feature.properties || {};
          const hexId = props.hex || props.h3;
          
          // If it's a hex, use H3 to get boundary
          if (hexId && typeof h3 !== 'undefined') {
            try {
              const id = toH3(hexId);
              if (id && h3.isValidCell(id)) {
                const b = h3.cellToBoundary(id).map(([lat,lng]) => [lng,lat]);
                b.push(b[0]);
                geojson.features.push({ type:'Feature', geometry:{ type:'Polygon', coordinates:[b] }, properties: props });
              }
      } catch(e) {}
          }
          // For vectors: try to find original unclipped geometry
          else if (layerId) {
            const originalFeature = findOriginalFeature(feature, layerId);
            if (originalFeature?.geometry) {
              geojson.features.push({ 
                type: 'Feature', 
                geometry: originalFeature.geometry, 
                properties: props 
              });
            } else if (feature.geometry) {
              // Fallback to clicked geometry if original not found
              geojson.features.push({ type:'Feature', geometry: feature.geometry, properties: props });
            }
          }
          else if (feature.geometry) {
            geojson.features.push({ type:'Feature', geometry: feature.geometry, properties: props });
          }
        }
        
        if (!added) {
          map.addSource('feature-hl', { type:'geojson', data:geojson });
          map.addLayer({ id:'feature-hl-fill', type:'fill', source:'feature-hl', paint:{'fill-color':HL_FILL,'fill-opacity':1} });
          map.addLayer({ id:'feature-hl-line', type:'line', source:'feature-hl', paint:{'line-color':HL_LINE,'line-width':HL_LW} });
          added = true;
        } else {
          map.getSource('feature-hl').setData(geojson);
        }
        selected = feature;
      }
      
      function getLayers() {
        const layers = [];
        LAYERS_DATA.forEach(l => {
          if (l.isTileLayer) return;
          const ids = l.layerType === 'vector' 
            ? [`${l.id}-fill`, `${l.id}-circle`, `${l.id}-line`]
            : [l.hexLayer?.extruded ? `${l.id}-extrusion` : `${l.id}-fill`];
          ids.forEach(id => { try { if (map.getLayer(id)) layers.push(id); } catch(e){} });
        });
        return layers;
      }
      
      map.on('load', () => {
        map.on('click', e => {
          const queryLayers = getLayers();
          if (!queryLayers.length) return;
          
          let feats = [];
          try { feats = map.queryRenderedFeatures(e.point, { layers: queryLayers }) || []; } catch(err) {}
          
          if (feats.length > 0) {
            const clickedLayerId = feats[0].layer?.id || '';
            highlight(feats[0], clickedLayerId);
          } else if (typeof deckOverlay !== 'undefined') {
            const info = deckOverlay?.pickObject?.({ x:e.point.x, y:e.point.y, radius:4 });
            if (info?.object) {
              highlight({ properties: info.object.properties || info.object, geometry: null });
            } else if (selected) {
              highlight(null);
            }
          } else if (selected) {
            highlight(null);
          }
        });
      });
    })();
    {% endif %}
    
    {% if on_click %}
    // Click broadcast - sends click events on any map feature (hex or vector)
    (function() {
      const CONFIG = {{ on_click | tojson }};
      const CHANNEL = CONFIG.channel || 'fused-bus';
      const MESSAGE_TYPE = CONFIG.message_type || 'feature_click';
      const PROPERTY_FILTER = CONFIG.properties || null;  // null = send all
      const INCLUDE_COORDS = CONFIG.include_coords !== false;
      const INCLUDE_LAYER = CONFIG.include_layer !== false;
      
      // Setup broadcast channel
      let broadcastChannel = null;
      try {
        if ('BroadcastChannel' in window) {
          broadcastChannel = new BroadcastChannel(CHANNEL);
        }
      } catch(e) {
        console.warn('[ClickBroadcast] BroadcastChannel not available:', e);
      }
      
      // Broadcast to all possible targets
      function broadcast(message) {
        const messageStr = JSON.stringify(message);
        
        // BroadcastChannel (same-origin tabs/windows)
        if (broadcastChannel) {
          try { broadcastChannel.postMessage(message); } catch(e) {}
        }
        
        // PostMessage to parent frame
        try { window.parent.postMessage(messageStr, '*'); } catch(e) {}
        
        // PostMessage to top frame (if different from parent)
        try {
          if (window.top && window.top !== window.parent) {
            window.top.postMessage(messageStr, '*');
          }
        } catch(e) {}
        
        // PostMessage to all sibling frames
        try {
          if (window.top && window.top.frames) {
            for (let i = 0; i < window.top.frames.length; i++) {
              const frame = window.top.frames[i];
              if (frame !== window) {
                try { frame.postMessage(messageStr, '*'); } catch(e) {}
              }
            }
          }
        } catch(e) {}
      }
      
      // Get all layers that should respond to clicks
      function getClickableLayers() {
        const layerIds = [];
        
        // From LAYERS_DATA (multi-layer maps)
        if (typeof LAYERS_DATA !== 'undefined' && Array.isArray(LAYERS_DATA)) {
          LAYERS_DATA.forEach(layerDef => {
            if (layerDef.isTileLayer) return;  // Skip tile layers (handled separately)
            
            if (layerDef.layerType === 'vector') {
              // Vector layers: fill, circle, line
              [`${layerDef.id}-fill`, `${layerDef.id}-circle`, `${layerDef.id}-line`].forEach(id => {
                try {
                  if (map.getLayer(id)) layerIds.push(id);
                } catch(e) {}
              });
            } else if (layerDef.layerType === 'hex') {
              // Hex layers: extrusion or fill
              const ids = layerDef.hexLayer?.extruded 
                ? [`${layerDef.id}-extrusion`] 
                : [`${layerDef.id}-fill`];
              ids.forEach(id => {
                try {
                  if (map.getLayer(id)) layerIds.push(id);
                } catch(e) {}
              });
            }
          });
        }
        
        return layerIds;
      }
      
      // Handle click event
      function handleClick(event) {
        const clickableLayers = getClickableLayers();
        if (!clickableLayers.length) return;
        
        // Query features at click point
        let features = [];
        try {
          features = map.queryRenderedFeatures(event.point, { layers: clickableLayers }) || [];
        } catch(err) {
          console.warn('[ClickBroadcast] Error querying features:', err);
        return;
      }
      
        if (!features.length) return;
        
        // Use the top-most feature
        const feature = features[0];
        const properties = feature.properties || {};
        const layerId = feature.layer?.id || '';
        
        // Find user-friendly layer name from LAYERS_DATA
        let layerName = layerId;
        if (typeof LAYERS_DATA !== 'undefined') {
          const layerDef = LAYERS_DATA.find(l => layerId.startsWith(l.id));
          if (layerDef) layerName = layerDef.name || layerDef.id;
        }
        
        // Filter properties if specified, otherwise send all
        let propertiesToSend = {};
        if (PROPERTY_FILTER && Array.isArray(PROPERTY_FILTER)) {
          PROPERTY_FILTER.forEach(key => {
            if (properties[key] !== undefined) {
              propertiesToSend[key] = properties[key];
            }
          });
      } else {
          propertiesToSend = { ...properties };
        }
        
        // Build message
        const message = {
          type: MESSAGE_TYPE,
          properties: propertiesToSend,
          timestamp: Date.now()
        };
        
        if (INCLUDE_COORDS) {
          message.lngLat = {
            lng: event.lngLat.lng,
            lat: event.lngLat.lat
          };
        }
        
        if (INCLUDE_LAYER) {
          message.layer = layerName;
        }
        
        broadcast(message);
      }
      
      // Initialize click handler when map is ready
      map.on('load', () => {
        map.on('click', handleClick);
      });
    })();
    {% endif %}
    
    {% if debug %}
    // Debug Panel functionality
    let debugApplyTimeout = null;
    
    // Current debug state for hex layer
    const debugState = {
      // Layer toggles
      pickable: true,
      filled: true,
      stroked: true,
      extruded: false,
      opacity: 1,
      coverage: 0.9,
      
      // Fill color
      fillFn: 'colorContinuous',
      fillAttr: 'metric',
      fillPalette: 'ArmyRose',
      fillReverse: false,
      fillDomainMin: 0,
      fillDomainMax: 100,
      fillSteps: 7,
      fillNullColor: '#b8b8b8',
      fillStaticColor: '#0090ff',
      // Only treat view as "user-set" after real user interaction on the map (pointer/wheel/touch)
      userMapInteracted: false,
      
      // Line color
      lineFn: 'static',
      lineAttr: 'metric',
      linePalette: 'ArmyRose',
      lineDomainMin: 0,
      lineDomainMax: 100,
      lineStaticColor: '#ffffff',
      lineWidth: 1,
      
      // Elevation
      heightAttr: 'metric',
      elevationScale: 10,

      // Tooltip
      tooltipColumns: [],
      tooltipAllColumns: [],
    };

    // Active layer being edited in the debug panel (single set of controls, per-layer editing)
    let activeDebugLayerId = null;

    function getEditableLayers() {
      // Hex layers include both static and tiled (Deck overlay) hex layers
      return (Array.isArray(LAYERS_DATA) ? LAYERS_DATA : []).filter(l =>
        l && (l.layerType === 'hex' || l.layerType === 'vector')
      );
    }

    function getActiveLayerDef() {
      const layers = getEditableLayers();
      if (!layers.length) return null;
      const found = layers.find(l => l.id === activeDebugLayerId);
      return found || layers[0];
    }

    function initDebugLayerSelector() {
      const sel = document.getElementById('dbg-layer-select');
      if (!sel) return;
      const layers = getEditableLayers();
      sel.innerHTML = layers.map(l => {
        const kind = l.layerType === 'vector' ? 'vector' : (l.isTileLayer ? 'hex (tile)' : 'hex');
        const label = `${l.name || l.id} — ${kind}`;
        return `<option value="${l.id}">${label}</option>`;
      }).join('');
      const initial = getActiveLayerDef();
      activeDebugLayerId = initial?.id || null;
      if (activeDebugLayerId) sel.value = activeDebugLayerId;
      sel.addEventListener('change', () => {
        activeDebugLayerId = sel.value;
        populateAttrDropdown(); // re-hydrate UI from selected layer + its columns
        updateSectionVisibility();
        updateFillFnOptions();
        updateLineFnOptions();
        if (typeof updateConfigOutput === 'function') updateConfigOutput();
      });
    }
    
    function toggleDebugPanel() {
      const panel = document.getElementById('debug-panel');
      const toggle = document.getElementById('debug-toggle');
      if (panel.classList.contains('collapsed')) {
        panel.classList.remove('collapsed');
        toggle.innerHTML = '&#x2039;';
      } else {
        panel.classList.add('collapsed');
        toggle.innerHTML = '&#x203a;';
      }
      // Keep toggle pinned to the panel edge
      updateDebugTogglePosition();
    }

    function updateDebugTogglePosition() {
      const panel = document.getElementById('debug-panel');
      const toggle = document.getElementById('debug-toggle');
      const shell = document.getElementById('debug-shell');
      if (!panel || !toggle) return;
      if (panel.classList.contains('collapsed')) {
        if (shell) shell.style.setProperty('--debug-panel-w', '0px');
      } else {
        const w = panel.getBoundingClientRect().width;
        if (shell) shell.style.setProperty('--debug-panel-w', `${w}px`);
      }
    }
    
    // Toggle section visibility based on checkboxes
    function updateSectionVisibility() {
      const stroked = document.getElementById('dbg-stroked')?.checked;
      const extruded = document.getElementById('dbg-extruded')?.checked;
      const filled = document.getElementById('dbg-filled')?.checked;
      
      document.getElementById('line-color-section').style.display = stroked ? 'block' : 'none';
      document.getElementById('elevation-section').style.display = extruded ? 'block' : 'none';
      document.getElementById('fill-color-section').style.display = filled ? 'block' : 'none';
    }
    
    // Toggle fill/line function options
    function updateFillFnOptions() {
      const fn = document.getElementById('dbg-fill-fn')?.value;
      document.getElementById('fill-fn-options').style.display = fn !== 'static' ? 'block' : 'none';
      document.getElementById('fill-static-options').style.display = fn === 'static' ? 'block' : 'none';
    }
    
    function updateLineFnOptions() {
      const fn = document.getElementById('dbg-line-fn')?.value;
      document.getElementById('line-fn-options').style.display = fn !== 'static' ? 'block' : 'none';
      document.getElementById('line-static-options').style.display = fn === 'static' ? 'block' : 'none';
    }
    
    // Sync slider and input
    function syncSliderInput(sliderId, inputId) {
      const slider = document.getElementById(sliderId);
      const input = document.getElementById(inputId);
      if (slider && input) {
        slider.addEventListener('input', () => { input.value = slider.value; scheduleLayerUpdate(); });
        input.addEventListener('change', () => { slider.value = input.value; scheduleLayerUpdate(); });
      }
    }
    
    // Update color label when color picker changes
    function syncColorLabel(colorId, labelId) {
      const color = document.getElementById(colorId);
      const label = document.getElementById(labelId);
      if (color && label) {
        color.addEventListener('input', () => { label.textContent = color.value; scheduleLayerUpdate(); });
      }
    }

    // Minimal custom palette dropdown: uses a hidden <select> as the source of truth.
    function initPaletteDropdown(selectId, triggerId, menuId, nameId, swatchId) {
      const sel = document.getElementById(selectId);
      const trigger = document.getElementById(triggerId);
      const menu = document.getElementById(menuId);
      const nameEl = document.getElementById(nameId);
      const swatchEl = document.getElementById(swatchId);
      if (!sel || !trigger || !menu || !nameEl || !swatchEl) return;
      const reverseEl = document.getElementById('dbg-reverse-colors');
      const shouldReverse = () => (selectId === 'dbg-palette') && !!reverseEl?.checked;

      const stopsToGradient = (cols) => {
        if (!cols || !cols.length) return 'linear-gradient(90deg, #555, #999)';
        const n = cols.length;
        const stops = cols.map((c, i) => `${c} ${(i / Math.max(1, n - 1)) * 100}%`);
        return `linear-gradient(90deg, ${stops.join(', ')})`;
      };

      const renderSwatch = (paletteName) => {
        const cols0 = getPaletteColors(paletteName, 7) || getPaletteColors(paletteName, 9) || null;
        const cols = (cols0 && shouldReverse()) ? [...cols0].reverse() : cols0;
        swatchEl.style.background = stopsToGradient(cols);
      };

      const close = () => { menu.style.display = 'none'; trigger.setAttribute('aria-expanded', 'false'); };
      const open = () => { menu.style.display = 'block'; trigger.setAttribute('aria-expanded', 'true'); };
      const isOpen = () => menu.style.display !== 'none';

      const setValue = (val, dispatch = true) => {
        sel.value = val;
        nameEl.textContent = val;
        renderSwatch(val);
        if (dispatch) sel.dispatchEvent(new Event('change', { bubbles: true }));
      };

      const syncFromSelect = () => {
        // Don't override config-driven values; just reflect whatever the select currently has.
        const v = sel.value || (sel.options?.[0]?.value || '');
        if (!sel.value && v) sel.value = v;
        nameEl.textContent = v || 'Palette';
        trigger.title = v || 'Palette';
        trigger.setAttribute('aria-label', v || 'Palette');
        if (v) renderSwatch(v);
      };

      // Build menu items from select options
      const buildMenu = () => {
        const opts = Array.from(sel.options || []);
        menu.innerHTML = opts.map(o => {
          const v = o.value;
          // Swatch-only item; palette name shown via hover tooltip (title)
          return `
            <div class="pal-item" data-value="${v}" title="${v}" aria-label="${v}">
              <span class="pal-item-swatch" data-swatch="${v}"></span>
            </div>
          `;
        }).join('');

        // Fill swatches after insertion
        menu.querySelectorAll('[data-swatch]').forEach(el => {
          const v = el.getAttribute('data-swatch');
          const cols0 = getPaletteColors(v, 7) || getPaletteColors(v, 9) || null;
          const cols = (cols0 && shouldReverse()) ? [...cols0].reverse() : cols0;
          el.style.background = stopsToGradient(cols);
        });
      };

      buildMenu();

      // Sync UI when select changes (e.g. populateAttrDropdown)
      sel.addEventListener('change', () => {
        syncFromSelect();
      });

      // Manual sync hook (when code sets sel.value without firing change)
      sel.addEventListener('pal:sync', () => {
        syncFromSelect();
      });

      // Init current
      syncFromSelect();
      
      // If the fill reverse checkbox changes, update palette previews too (trigger + menu).
      if (selectId === 'dbg-palette' && reverseEl) {
        reverseEl.addEventListener('change', () => {
          buildMenu();
          syncFromSelect();
        });
      }

      trigger.addEventListener('click', (e) => {
        e.preventDefault();
        e.stopPropagation();
        if (isOpen()) close();
        else open();
      });

      menu.addEventListener('click', (e) => {
        const item = e.target?.closest?.('.pal-item');
        if (!item) return;
        const v = item.getAttribute('data-value');
        if (v) setValue(v, true);
        close();
      });

      document.addEventListener('click', (e) => {
        if (!isOpen()) return;
        const within = trigger.contains(e.target) || menu.contains(e.target);
        if (!within) close();
      });
    }
    
    // Debug panel - update view state from map
    let debugUpdatingMap = false;  // Flag to prevent feedback loop
    
    function syncDebugFromMap(markMoved = false) {
      if (debugUpdatingMap) return;  // Skip if we're the ones moving the map
      if (markMoved) debugState.userMapInteracted = true;
      const center = map.getCenter();
      document.getElementById('dbg-lng').value = center.lng.toFixed(4);
      document.getElementById('dbg-lat').value = center.lat.toFixed(4);
      document.getElementById('dbg-zoom').value = map.getZoom().toFixed(1);
      document.getElementById('dbg-pitch').value = Math.round(map.getPitch());
      document.getElementById('dbg-bearing').value = Math.round(map.getBearing());
      updateConfigOutput();
    }
    
    // Populate all attribute dropdowns
    function populateAttrDropdown() {
      const layerDef = getActiveLayerDef();
      if (!layerDef) return;

      const attrs = new Set();
      const tooltipKeys = new Set();

      // 1) Try to discover columns from the active layer's data
      if (layerDef.layerType === 'hex' && layerDef.isTileLayer) {
        // Tile hex layer: use cached tile data if available (autoDomain + attribute inference)
        const layerTiles = (TILE_DATA_STORE || {})[layerDef.id] || {};
        Object.values(layerTiles || {}).forEach(tileData => {
          (tileData || []).forEach(d => {
            const obj = d?.properties || d || {};
            Object.keys(obj).forEach(k => {
              if (['hex', 'h3', 'geometry', '_fused_idx', 'properties'].includes(k)) return;
              const val = obj[k];
              if (typeof val === 'number' && isFinite(val)) attrs.add(k);
              tooltipKeys.add(k);
            });
          });
        });
      } else if (layerDef.layerType === 'hex') {
        // Static hex layer: inspect row objects
        (layerDef.data || []).forEach(d => {
          const obj = d?.properties || d || {};
          Object.keys(obj).forEach(k => {
            if (['hex', 'h3', 'geometry', '_fused_idx', 'properties'].includes(k)) return;
            const val = obj[k];
            if (typeof val === 'number' && isFinite(val)) attrs.add(k);
            tooltipKeys.add(k);
          });
        });
      } else if (layerDef.layerType === 'vector') {
        // GeoJSON vector layer: inspect feature properties
        (layerDef.geojson?.features || []).forEach(f => {
          const obj = f?.properties || {};
          Object.keys(obj).forEach(k => {
            if (['geometry', '_fused_idx', 'properties'].includes(k)) return;
            const val = obj[k];
            if (typeof val === 'number' && isFinite(val)) attrs.add(k);
            tooltipKeys.add(k);
          });
        });
      }

      // 2) Always include attrs referenced by current config, so the dropdown doesn't "lose" them
      const cfg = layerDef.hexLayer || layerDef.vectorLayer || {};
      const fillCfg = cfg.getFillColor || layerDef.fillColorConfig || {};
      const lineCfg = cfg.getLineColor || layerDef.lineColorConfig || {};
      const referenced = [
        (fillCfg && typeof fillCfg === 'object') ? fillCfg.attr : null,
        (lineCfg && typeof lineCfg === 'object') ? lineCfg.attr : null,
        debugState.heightAttr
      ].filter(Boolean);
      referenced.forEach(a => attrs.add(a));

      if (attrs.size === 0) attrs.add('metric');
      const attrOptions = [...attrs].sort().map(a => `<option value="${a}">${a}</option>`).join('');

      // Persist "all possible tooltip columns" for snippet generation and UI defaults.
      debugState.tooltipAllColumns = [...tooltipKeys]
        .filter(k => k && !['geometry', '_fused_idx', 'properties'].includes(k))
        .sort();
      
      // Populate all attribute dropdowns
      ['dbg-attr', 'dbg-height-attr', 'dbg-line-attr'].forEach(id => {
        const el = document.getElementById(id);
        if (el) el.innerHTML = attrOptions;
      });
      
      // Initialize from the selected layer's config
      if (layerDef) {
        const cfg = layerDef.hexLayer || layerDef.vectorLayer || {};
        const fillCfg = cfg.getFillColor || layerDef.fillColorConfig || {};
        const lineCfg = cfg.getLineColor || layerDef.lineColorConfig || {};
        
        // Layer toggles
        const setCheckbox = (id, val) => { const el = document.getElementById(id); if (el) el.checked = val; };
        const setInput = (id, val) => { const el = document.getElementById(id); if (el) el.value = val; };
        
        setCheckbox('dbg-filled', cfg.filled !== false);
        // Vector uses `stroked`, hex uses `stroked` too; processed vector also has `isStroked`
        setCheckbox('dbg-stroked', (cfg.stroked ?? layerDef.isStroked) !== false);
        setCheckbox('dbg-extruded', cfg.extruded === true);
        
        // Pickable is always-on (debug UI removed)
        debugState.pickable = true;
        debugState.filled = cfg.filled !== false;
        debugState.stroked = (cfg.stroked ?? layerDef.isStroked) !== false;
        debugState.extruded = cfg.extruded === true;
        
        // Opacity
        setInput('dbg-opacity', cfg.opacity ?? layerDef.opacity ?? 1);
        setInput('dbg-opacity-slider', cfg.opacity ?? layerDef.opacity ?? 1);
        debugState.opacity = cfg.opacity ?? layerDef.opacity ?? 1;
        // coverage control removed from debug UI (still supported via JSON config)
        
        // Fill color - detect static (array) vs function
        if (Array.isArray(fillCfg) || (layerDef.fillColorRgba && !fillCfg['@@function'])) {
          // Static color
          setInput('dbg-fill-fn', 'static');
          debugState.fillFn = 'static';
          // Convert array to hex for color picker
          if (Array.isArray(fillCfg) && fillCfg.length >= 3) {
            const hex = '#' + fillCfg.slice(0, 3).map(c => Math.round(c).toString(16).padStart(2, '0')).join('');
            setInput('dbg-fill-static', hex);
            debugState.fillStaticColor = hex;
          }
        } else {
          // Function-based fill (default to colorContinuous)
          const fn = (fillCfg && typeof fillCfg === 'object' && fillCfg['@@function']) ? fillCfg['@@function'] : 'colorContinuous';
          setInput('dbg-fill-fn', fn);
          debugState.fillFn = fn;
        }
        
        // Set attribute from config or pick first available
        if (fillCfg.attr) {
          setInput('dbg-attr', fillCfg.attr);
          debugState.fillAttr = fillCfg.attr;
        } else if (attrs.size > 0) {
          const first = [...attrs].sort()[0];
          setInput('dbg-attr', first);
          debugState.fillAttr = first;
          // Trigger domain calculation for first attribute (only if DuckDB available)
          if (duckDbReady) {
            document.getElementById('dbg-attr')?.dispatchEvent(new Event('change'));
          }
        }
        
        // Palette (always reset on layer switch to avoid leakage)
        const pal = (fillCfg && typeof fillCfg === 'object' && fillCfg.colors) ? fillCfg.colors : 'ArmyRose';
        setInput('dbg-palette', pal);
        debugState.fillPalette = pal;
        document.getElementById('dbg-palette')?.dispatchEvent(new Event('pal:sync'));

        // Reverse colors (always reset on layer switch)
        const domainFromUser = !!layerDef?.fillDomainFromUser;
        const reverseFromDomain = domainFromUser && Array.isArray(fillCfg.domain) && fillCfg.domain.length >= 2 && fillCfg.domain[0] > fillCfg.domain[fillCfg.domain.length - 1];
        const reverseVal = (fillCfg && typeof fillCfg === 'object' && fillCfg.reverse === true) || reverseFromDomain;
        const revEl = document.getElementById('dbg-reverse-colors');
        if (revEl) revEl.checked = !!reverseVal;
        debugState.fillReverse = !!reverseVal;
        // Domain + Steps + Null color: always reset on layer switch (but don't change slider extent)
        const rMin = document.getElementById('dbg-domain-range-min');
        const rMax = document.getElementById('dbg-domain-range-max');
        const extMin = rMin ? Number(rMin.min) : NaN;
        const extMax = rMax ? Number(rMax.max) : NaN;

        // If config has a domain, use it; otherwise default to current extent if available.
        if (fillCfg && typeof fillCfg === 'object' && Array.isArray(fillCfg.domain) && fillCfg.domain.length >= 2) {
          const dmin = Math.min(...fillCfg.domain);
          const dmax = Math.max(...fillCfg.domain);
          setInput('dbg-domain-min', dmin);
          setInput('dbg-domain-max', dmax);
          debugState.fillDomainMin = dmin;
          debugState.fillDomainMax = dmax;
        } else if (Number.isFinite(extMin) && Number.isFinite(extMax)) {
          setInput('dbg-domain-min', extMin);
          setInput('dbg-domain-max', extMax);
          debugState.fillDomainMin = extMin;
          debugState.fillDomainMax = extMax;
        }
        // Move thumbs to match inputs (but do not recompute extent)
        syncDomainSliderFromInputs();

        const steps = (fillCfg && typeof fillCfg === 'object' && Number.isFinite(parseInt(fillCfg.steps))) ? parseInt(fillCfg.steps) : 7;
        setInput('dbg-steps', steps);
        debugState.fillSteps = steps;

        const nullHex = (fillCfg && typeof fillCfg === 'object' && Array.isArray(fillCfg.nullColor) && fillCfg.nullColor.length >= 3)
          ? ('#' + fillCfg.nullColor.slice(0,3).map(c => Math.round(c).toString(16).padStart(2,'0')).join(''))
          : '#b8b8b8';
        setInput('dbg-null-color', nullHex);
        debugState.fillNullColor = nullHex;
        const nullLabel = document.getElementById('dbg-null-color-label');
        if (nullLabel) nullLabel.textContent = nullHex;
        
        // Line color (always reset on layer switch)
        if (Array.isArray(lineCfg) && lineCfg.length >= 3) {
          debugState.lineFn = 'static';
          setInput('dbg-line-fn', 'static');
          // Convert RGB array to hex
          const r = lineCfg[0], g = lineCfg[1], b = lineCfg[2];
          const hex = '#' + [r, g, b].map(x => x.toString(16).padStart(2, '0')).join('');
          setInput('dbg-line-static', hex);
          debugState.lineStaticColor = hex;
          const label = document.getElementById('dbg-line-static-label');
          if (label) label.textContent = hex;
        } else if (lineCfg && typeof lineCfg === 'object' && lineCfg['@@function']) {
          setInput('dbg-line-fn', lineCfg['@@function']);
          debugState.lineFn = lineCfg['@@function'];
          if (lineCfg.attr) {
            setInput('dbg-line-attr', lineCfg.attr);
            debugState.lineAttr = lineCfg.attr;
          }
          const lpal = lineCfg.colors || 'ArmyRose';
          setInput('dbg-line-palette', lpal);
          debugState.linePalette = lpal;
          document.getElementById('dbg-line-palette')?.dispatchEvent(new Event('pal:sync'));
          if (lineCfg.domain) {
            setInput('dbg-line-domain-min', Math.min(...lineCfg.domain));
            setInput('dbg-line-domain-max', Math.max(...lineCfg.domain));
            debugState.lineDomainMin = Math.min(...lineCfg.domain);
            debugState.lineDomainMax = Math.max(...lineCfg.domain);
          }
        } else {
          // Default line = static white
          debugState.lineFn = 'static';
          setInput('dbg-line-fn', 'static');
          const hex = '#ffffff';
          setInput('dbg-line-static', hex);
          debugState.lineStaticColor = hex;
        }
        document.getElementById('dbg-line-palette')?.dispatchEvent(new Event('pal:sync'));
        
        // Line width (hex: lineWidthMinPixels; vector processed: lineWidth)
        const lw = cfg.lineWidthMinPixels ?? layerDef.lineWidth ?? 1;
        setInput('dbg-line-width', lw);
        setInput('dbg-line-width-slider', lw);
        debugState.lineWidth = lw;
        
        // Elevation
        setInput('dbg-height-attr', debugState.fillAttr);
        setInput('dbg-height-scale', cfg.elevationScale ?? 10);
        debugState.heightAttr = debugState.fillAttr;
        debugState.elevationScale = cfg.elevationScale ?? 10;
        
        // Update visibility
        updateSectionVisibility();
        updateFillFnOptions();
        updateLineFnOptions();

        // Tooltip columns UI
        const tooltipContainer = document.getElementById('dbg-tooltip-cols');
        if (tooltipContainer) {
          const initialCols = (cfg.tooltipColumns || cfg.tooltipAttrs || layerDef.tooltipColumns || []).filter(Boolean);
          // If we couldn't infer columns from data yet (common for tile layers before any tile loads),
          // fall back to the configured tooltip columns so the checklist is still usable.
          const inferredAll = Array.isArray(debugState.tooltipAllColumns) ? debugState.tooltipAllColumns : [];
          const allCols = (inferredAll && inferredAll.length) ? inferredAll : initialCols;
          debugState.tooltipAllColumns = allCols;

          // Default = select all columns, unless explicitly provided by config.
          debugState.tooltipColumns = initialCols.length ? initialCols : allCols;
          tooltipContainer.innerHTML = allCols.map(k => {
            const checked = debugState.tooltipColumns.includes(k) ? 'checked' : '';
            return `<label class="debug-check"><input type="checkbox" data-tooltip-col="${k}" ${checked} /><code>${k}</code></label>`;
          }).join('');

          tooltipContainer.querySelectorAll('input[type="checkbox"][data-tooltip-col]').forEach(cb => {
            cb.addEventListener('change', () => {
              scheduleLayerUpdate();
            });
          });

          // Apply initial tooltip selection immediately (so tooltips work before the user edits anything)
          layerDef.tooltipColumns = debugState.tooltipColumns;
          if (layerDef.hexLayer) layerDef.hexLayer.tooltipColumns = debugState.tooltipColumns;
          if (layerDef.vectorLayer) layerDef.vectorLayer.tooltipColumns = debugState.tooltipColumns;
        }
      }
    }

    // Domain dual slider wiring (fill domain only)
    function syncDomainSliderFromInputs() {
      const minEl = document.getElementById('dbg-domain-min');
      const maxEl = document.getElementById('dbg-domain-max');
      const rMin = document.getElementById('dbg-domain-range-min');
      const rMax = document.getElementById('dbg-domain-range-max');
      if (!minEl || !maxEl || !rMin || !rMax) return;
      const minV = parseFloat(minEl.value);
      const maxV = parseFloat(maxEl.value);
      if (Number.isFinite(minV)) rMin.value = String(minV);
      if (Number.isFinite(maxV)) rMax.value = String(maxV);
    }

    function syncDomainInputsFromSlider() {
      const minEl = document.getElementById('dbg-domain-min');
      const maxEl = document.getElementById('dbg-domain-max');
      const rMin = document.getElementById('dbg-domain-range-min');
      const rMax = document.getElementById('dbg-domain-range-max');
      if (!minEl || !maxEl || !rMin || !rMax) return;
      let a = parseFloat(rMin.value);
      let b = parseFloat(rMax.value);
      if (!Number.isFinite(a) || !Number.isFinite(b)) return;
      if (a > b) [a, b] = [b, a];
      minEl.value = String(a);
      maxEl.value = String(b);
      // Keep thumbs ordered too
      rMin.value = String(a);
      rMax.value = String(b);
    }

    function setDomainSliderBounds(extMin, extMax) {
      const rMin = document.getElementById('dbg-domain-range-min');
      const rMax = document.getElementById('dbg-domain-range-max');
      if (!rMin || !rMax) return;
      const lo = Number.isFinite(extMin) ? extMin : 0;
      const hi = Number.isFinite(extMax) ? extMax : 100;
      rMin.min = String(lo);
      rMin.max = String(hi);
      rMax.min = String(lo);
      rMax.max = String(hi);
      // keep step aligned with numeric input step
      rMin.step = '0.1';
      rMax.step = '0.1';
    }
    
    // Helper: hex color to RGB array
    function hexToRgb(hex) {
      const r = parseInt(hex.slice(1, 3), 16);
      const g = parseInt(hex.slice(3, 5), 16);
      const b = parseInt(hex.slice(5, 7), 16);
      return [r, g, b];
    }
    
    // Generate config output
    function updateConfigOutput() {
      const output = document.getElementById('dbg-output');
      if (!output) return;

      // Deep equality + "strip defaults" helpers for minimal snippet output.
      const deepEqual = (a, b) => {
        if (a === b) return true;
        if (a == null || b == null) return a === b;
        if (Array.isArray(a) || Array.isArray(b)) {
          if (!Array.isArray(a) || !Array.isArray(b)) return false;
          if (a.length !== b.length) return false;
          for (let i = 0; i < a.length; i++) if (!deepEqual(a[i], b[i])) return false;
          return true;
        }
        if (typeof a === 'object' && typeof b === 'object') {
          const ak = Object.keys(a);
          const bk = Object.keys(b);
          if (ak.length !== bk.length) return false;
          for (const k of ak) {
            if (!Object.prototype.hasOwnProperty.call(b, k)) return false;
            if (!deepEqual(a[k], b[k])) return false;
          }
          return true;
        }
        return false;
      };

      const stripDefaults = (obj, defaults) => {
        if (obj == null) return obj;
        if (Array.isArray(obj)) return obj; // arrays replace defaults
        if (typeof obj !== 'object') return obj;
        const out = {};
        for (const [k, v] of Object.entries(obj)) {
          const d = defaults && typeof defaults === 'object' ? defaults[k] : undefined;
          if (typeof v === 'object' && v !== null && !Array.isArray(v) && typeof d === 'object' && d !== null && !Array.isArray(d)) {
            const nested = stripDefaults(v, d);
            if (nested && typeof nested === 'object' && !Array.isArray(nested) && Object.keys(nested).length === 0) continue;
            if (deepEqual(nested, d)) continue;
            out[k] = nested;
            continue;
          }
          if (d !== undefined && deepEqual(v, d)) continue;
          if (v !== undefined) out[k] = v;
        }
        return out;
      };

      // Use the active debug layer for snippet generation.
      // IMPORTANT: define this BEFORE any code references `firstLayer` to avoid TDZ errors.
      const firstLayer = getActiveLayerDef() ||
        (LAYERS_DATA.find(l => l.layerType === 'hex') ||
         LAYERS_DATA.find(l => l.layerType === 'vector') ||
         LAYERS_DATA[0]);
      
      // Build getFillColor
      let getFillColor;
      if (debugState.fillFn === 'static') {
        getFillColor = hexToRgb(debugState.fillStaticColor);
      } else {
        // Build full object, then strip against defaults so we only keep overrides.
        const baseFill = DEFAULT_HEX_CONFIG?.hexLayer?.getFillColor || DEFAULT_VECTOR_CONFIG?.vectorLayer?.getFillColor || {};
        const rMin = document.getElementById('dbg-domain-range-min');
        const rMax = document.getElementById('dbg-domain-range-max');
        const extentMin = rMin ? Number(rMin.min) : NaN;
        const extentMax = rMax ? Number(rMax.max) : NaN;
        const selectedMin = Number(debugState.fillDomainMin);
        const selectedMax = Number(debugState.fillDomainMax);
        const hasSql = !!firstLayer?.sql;
        const domainFromUser = !!firstLayer?.fillDomainFromUser;
        const shouldIncludeDomain =
          domainFromUser ||
          (!hasSql) || // non-SQL layers need a domain to colorContinuous
          (Number.isFinite(extentMin) && Number.isFinite(extentMax) &&
            (Math.abs(selectedMin - extentMin) > 1e-9 || Math.abs(selectedMax - extentMax) > 1e-9));

        const fillObj = {
          '@@function': debugState.fillFn,
          attr: debugState.fillAttr,
          ...(shouldIncludeDomain ? { domain: [debugState.fillDomainMin, debugState.fillDomainMax] } : {}),
          colors: debugState.fillPalette,
          ...(debugState.fillReverse ? { reverse: true } : {}),
          steps: debugState.fillSteps,
          nullColor: hexToRgb(debugState.fillNullColor),
        };
        getFillColor = stripDefaults(fillObj, baseFill);
      }
      
      // Build getLineColor
      let getLineColor;
      if (debugState.lineFn === 'static') {
        getLineColor = hexToRgb(debugState.lineStaticColor);
      } else {
        const baseLine = DEFAULT_HEX_CONFIG?.hexLayer?.getLineColor || DEFAULT_VECTOR_CONFIG?.vectorLayer?.getLineColor || {};
        const lineObj = {
          '@@function': debugState.lineFn,
          attr: debugState.lineAttr,
          // Domain is required for colorContinuous; include it when non-static functions are used.
          ...(debugState.lineFn === 'colorContinuous' ? { domain: [debugState.lineDomainMin, debugState.lineDomainMax] } : {}),
          colors: debugState.linePalette,
        };
        getLineColor = stripDefaults(lineObj, baseLine);
      }
      
      const basemapVal = document.getElementById('dbg-basemap')?.value || INITIAL_BASEMAP || 'dark';
      
      const layerName = firstLayer?.name || 'Layer';
      const layerKind = firstLayer?.layerType || 'hex';
      const layerType = (layerKind === 'vector') ? 'vector' : 'hex';

      // Keep snippet short: only include view after real user interaction.
      const commonConfig = {};
      if (debugState.userMapInteracted) {
        commonConfig.initialViewState = {
          longitude: parseFloat(document.getElementById('dbg-lng').value) || 0,
          latitude: parseFloat(document.getElementById('dbg-lat').value) || 0,
          zoom: parseFloat(document.getElementById('dbg-zoom').value) || 8,
          pitch: parseInt(document.getElementById('dbg-pitch').value) || 0,
          bearing: parseInt(document.getElementById('dbg-bearing').value) || 0
        };
      }
      if (basemapVal && basemapVal !== (INITIAL_BASEMAP || 'dark')) {
        commonConfig.basemap = basemapVal;
      }

      let layerStyleBlock = {};
      // Tooltip columns: default behavior is "all columns". Only emit tooltipColumns if user selected a subset.
      // Use `debugState.tooltipAllColumns` (populated in populateAttrDropdown) so this can't break if the UI isn't mounted yet.
      const allTooltipCols = Array.isArray(debugState.tooltipAllColumns) ? debugState.tooltipAllColumns : [];
      const tooltipAllSelected =
        allTooltipCols.length > 0 &&
        Array.isArray(debugState.tooltipColumns) &&
        debugState.tooltipColumns.length === allTooltipCols.length &&
        allTooltipCols.every(k => debugState.tooltipColumns.includes(k));
      const shouldIncludeTooltipColumns =
        Array.isArray(debugState.tooltipColumns) &&
        debugState.tooltipColumns.length > 0 &&
        !tooltipAllSelected;

      if (layerKind === 'vector') {
        const vecType = firstLayer?.vectorLayer?.['@@type'] || 'GeoJsonLayer';
        const baseVec = DEFAULT_VECTOR_CONFIG?.vectorLayer || {};
        const vecCandidate = {
          '@@type': vecType,
          filled: debugState.filled,
          stroked: debugState.stroked,
          opacity: debugState.opacity,
          ...(debugState.filled ? { getFillColor } : {}),
          ...(debugState.stroked ? { getLineColor, lineWidthMinPixels: debugState.lineWidth } : {}),
          ...(shouldIncludeTooltipColumns ? { tooltipColumns: debugState.tooltipColumns } : {}),
        };
        const vecOut = stripDefaults(vecCandidate, baseVec);
        layerStyleBlock = Object.keys(vecOut).length ? { vectorLayer: vecOut } : {};
      } else {
        const baseHex = DEFAULT_HEX_CONFIG?.hexLayer || {};
        const sqlVal = (HAS_SQL_LAYERS && document.getElementById('dbg-sql')?.value) ? document.getElementById('dbg-sql').value.trim() : '';
        const hexCandidate = {
          filled: debugState.filled,
          stroked: debugState.stroked,
          extruded: debugState.extruded,
          opacity: debugState.opacity,
          ...(debugState.filled ? { getFillColor } : {}),
          ...(debugState.stroked ? { getLineColor, lineWidthMinPixels: debugState.lineWidth } : {}),
          ...(debugState.extruded ? { 
            elevationScale: debugState.elevationScale,
            getElevation: `@@=properties.${debugState.heightAttr}`
          } : {}),
          ...(shouldIncludeTooltipColumns ? { tooltipColumns: debugState.tooltipColumns } : {}),
          ...(sqlVal ? { sql: sqlVal } : {}),
        };
        const hexOut = stripDefaults(hexCandidate, baseHex);
        layerStyleBlock = Object.keys(hexOut).length ? { hexLayer: hexOut } : {};
      }

      // Build full layer config
      const fullConfig = {
        name: layerName,
        type: layerType,
        data: 'df',  // placeholder for dataframe variable
        config: {
          ...commonConfig,
          ...layerStyleBlock
        }
      };
      if (fullConfig.config && typeof fullConfig.config === 'object' && Object.keys(fullConfig.config).length === 0) {
        delete fullConfig.config;
      }
      
      // Python-style output
      const toPython = (obj, indent = 0) => {
        const pad = '  '.repeat(indent);
        if (obj === null) return 'None';
        if (obj === true) return 'True';
        if (obj === false) return 'False';
        if (typeof obj === 'string') {
          // Special case: 'df' should not be quoted (it's a variable)
          if (obj === 'df') return 'df';
          return `"${obj}"`;
        }
        if (typeof obj === 'number') return Number.isInteger(obj) ? String(obj) : obj.toFixed(2);
        if (Array.isArray(obj)) return `[${obj.map(v => toPython(v, 0)).join(', ')}]`;
        if (typeof obj === 'object') {
          const entries = Object.entries(obj).map(([k, v]) => `${pad}  "${k}": ${toPython(v, indent + 1)}`);
          return `{\n${entries.join(',\n')}\n${pad}}`;
        }
        return String(obj);
      };
      
      output.value = `config = [${toPython(fullConfig, 0)}]`;
    }
    
    // Apply debug changes to map view
    function applyViewChanges() {
      // User-driven camera update (counts as "map moved" for snippet purposes)
      debugState.userMapInteracted = true;
      const lng = parseFloat(document.getElementById('dbg-lng').value);
      const lat = parseFloat(document.getElementById('dbg-lat').value);
      const zoom = parseFloat(document.getElementById('dbg-zoom').value);
      const pitch = parseInt(document.getElementById('dbg-pitch').value) || 0;
      const bearing = parseInt(document.getElementById('dbg-bearing').value) || 0;
      
      if (!isNaN(lng) && !isNaN(lat) && !isNaN(zoom)) {
        // Set flag to prevent syncDebugFromMap from overwriting our inputs
        debugUpdatingMap = true;
        map.jumpTo({ center: [lng, lat], zoom, pitch, bearing });
        // Clear flag after a short delay
        setTimeout(() => { debugUpdatingMap = false; }, 100);
      }
      updateConfigOutput();
    }

    function initBasemapControl() {
      const sel = document.getElementById('dbg-basemap');
      if (!sel) return;
      const known = [
        { value: 'dark', label: 'Dark' },
        { value: 'light', label: 'Light' },
        { value: 'satellite', label: 'Satellite' },
        { value: 'streets', label: 'Streets' }
      ];
      sel.innerHTML = '';
      known.forEach(o => {
        const opt = document.createElement('option');
        opt.value = o.value;
        opt.textContent = o.label;
        sel.appendChild(opt);
      });

      // If initial basemap is custom (style URL), add it as a selectable option.
      const initial = INITIAL_BASEMAP || 'dark';
      if (!known.some(o => o.value === initial) && typeof initial === 'string' && initial) {
        const opt = document.createElement('option');
        opt.value = initial;
        opt.textContent = 'Custom';
        sel.appendChild(opt);
      }
      sel.value = initial;

      const applyBasemapChange = () => {
        const basemapVal = sel.value || 'dark';
        const nextStyle = BASEMAP_STYLES[basemapVal] || basemapVal;
        const view = {
          center: map.getCenter(),
          zoom: map.getZoom(),
          pitch: map.getPitch(),
          bearing: map.getBearing()
        };
        try { map.setStyle(nextStyle); } catch (e) { return; }
        map.once('style.load', () => {
          try { map.jumpTo(view); } catch (e) {}
          if (typeof rebuildDeckOverlay === 'function' && HAS_TILE_LAYERS) rebuildDeckOverlay();
          if (typeof addAllLayers === 'function') addAllLayers();
          if (typeof rebuildQueryableLayers === 'function') rebuildQueryableLayers();
          if (typeof updateLegend === 'function') updateLegend();
          if (typeof updateLayerPanel === 'function') updateLayerPanel();
          syncDebugFromMap();
          updateConfigOutput();
        });
      };

      sel.addEventListener('change', applyBasemapChange);
    }
    
    // Apply layer style changes live
    let lastDebugStructure = null;
    function applyLayerChanges() {
      // Read all current values from form
      const getVal = (id, def) => document.getElementById(id)?.value ?? def;
      const getChecked = (id) => document.getElementById(id)?.checked ?? false;
      const getNum = (id, def) => {
        const v = parseFloat(getVal(id, def));
        return Number.isFinite(v) ? v : def;
      };
      
      // Layer toggles (pickable is always-on)
      debugState.pickable = true;
      debugState.filled = getChecked('dbg-filled');
      debugState.stroked = getChecked('dbg-stroked');
      debugState.extruded = getChecked('dbg-extruded');
      debugState.opacity = getNum('dbg-opacity', 1);
      // coverage control removed from debug UI (still supported via JSON config)
      
      // Fill color
      debugState.fillFn = getVal('dbg-fill-fn', 'colorContinuous');
      debugState.fillAttr = getVal('dbg-attr', 'metric');
      debugState.fillPalette = getVal('dbg-palette', 'ArmyRose');
      debugState.fillReverse = !!document.getElementById('dbg-reverse-colors')?.checked;
      debugState.fillDomainMin = getNum('dbg-domain-min', 0);
      debugState.fillDomainMax = getNum('dbg-domain-max', 100);
      debugState.fillSteps = parseInt(getVal('dbg-steps', 7)) || 7;
      debugState.fillNullColor = getVal('dbg-null-color', '#b8b8b8');
      debugState.fillStaticColor = getVal('dbg-fill-static', '#0090ff');
      
      // Line color
      debugState.lineFn = getVal('dbg-line-fn', 'static');
      debugState.lineAttr = getVal('dbg-line-attr', 'metric');
      debugState.linePalette = getVal('dbg-line-palette', 'ArmyRose');
      debugState.lineDomainMin = getNum('dbg-line-domain-min', 0);
      debugState.lineDomainMax = getNum('dbg-line-domain-max', 100);
      debugState.lineStaticColor = getVal('dbg-line-static', '#ffffff');
      debugState.lineWidth = getNum('dbg-line-width', 1);
      
      // Elevation
      debugState.heightAttr = getVal('dbg-height-attr', debugState.fillAttr);
      debugState.elevationScale = getNum('dbg-height-scale', 10);

      // Tooltip columns (multi-select)
      const tooltipCols = [];
      document.querySelectorAll('#dbg-tooltip-cols input[type="checkbox"][data-tooltip-col]').forEach(cb => {
        if (cb.checked) tooltipCols.push(cb.getAttribute('data-tooltip-col'));
      });
      debugState.tooltipColumns = tooltipCols;
      
      // Update section visibility
      updateSectionVisibility();
      updateFillFnOptions();
      updateLineFnOptions();
      
      // Update config output
      updateConfigOutput();
      
      // Build fill color config
      let fillColorCfg;
      if (debugState.fillFn === 'static') {
        fillColorCfg = hexToRgb(debugState.fillStaticColor);
      } else {
        fillColorCfg = {
          '@@function': debugState.fillFn,
          attr: debugState.fillAttr,
          domain: [debugState.fillDomainMin, debugState.fillDomainMax],
          colors: debugState.fillPalette,
          ...(debugState.fillReverse ? { reverse: true } : {}),
          steps: debugState.fillSteps,
          nullColor: hexToRgb(debugState.fillNullColor)
        };
      }
      
      // Build line color config
      let lineColorCfg;
      if (debugState.lineFn === 'static') {
        lineColorCfg = hexToRgb(debugState.lineStaticColor);
      } else {
        lineColorCfg = {
          '@@function': debugState.lineFn,
          attr: debugState.lineAttr,
          domain: [debugState.lineDomainMin, debugState.lineDomainMax],
          colors: debugState.linePalette
        };
      }

      // Helper: rgb array -> rgba string
      const rgbToRgba = (rgb, a = 1) => {
        if (!Array.isArray(rgb) || rgb.length < 3) return null;
        const r = parseInt(rgb[0]), g = parseInt(rgb[1]), b = parseInt(rgb[2]);
        const alpha = (typeof a === 'number' && isFinite(a)) ? Math.max(0, Math.min(1, a)) : 1;
        return `rgba(${r},${g},${b},${alpha})`;
      };
      
      // Apply debug changes ONLY to the currently selected layer.
      const l = getActiveLayerDef();
      if (!l) return;

      // Tooltip reads `layerDef.tooltipColumns` (top-level), so keep it in sync for the active layer.
      l.tooltipColumns = debugState.tooltipColumns;

      if (l.hexLayer) {
        l.hexLayer.pickable = true;
        l.hexLayer.filled = debugState.filled;
        l.hexLayer.stroked = debugState.stroked;
        l.hexLayer.extruded = debugState.extruded;
        l.hexLayer.opacity = debugState.opacity;
        l.hexLayer.coverage = debugState.coverage;
        l.hexLayer.getFillColor = fillColorCfg;
        l.hexLayer.getLineColor = lineColorCfg;
        l.hexLayer.lineWidthMinPixels = debugState.lineWidth;
        l.hexLayer.elevationScale = debugState.elevationScale;
        l.hexLayer.tooltipColumns = debugState.tooltipColumns;
      }

      // Also apply to vector layers (GeoJSON/vector)
      if (l.vectorLayer) {
        l.vectorLayer.filled = debugState.filled;
        l.vectorLayer.stroked = debugState.stroked;
        l.vectorLayer.opacity = debugState.opacity;
        l.vectorLayer.getFillColor = fillColorCfg;
        l.vectorLayer.getLineColor = lineColorCfg;
        l.vectorLayer.lineWidthMinPixels = debugState.lineWidth;
        l.vectorLayer.tooltipColumns = debugState.tooltipColumns;
      }

      // Keep processed vector fields in sync (used by addAllLayers())
      if (l.layerType === 'vector') {
        l.isFilled = debugState.filled;
        l.isStroked = debugState.stroked;
        l.opacity = debugState.opacity;
        l.lineWidth = debugState.lineWidth;

        if (fillColorCfg && typeof fillColorCfg === 'object' && !Array.isArray(fillColorCfg) && fillColorCfg['@@function']) {
          l.fillColorConfig = fillColorCfg;
          l.fillColorRgba = null;
        } else {
          l.fillColorConfig = {};
          l.fillColorRgba = rgbToRgba(fillColorCfg, debugState.opacity);
        }

        if (lineColorCfg && typeof lineColorCfg === 'object' && !Array.isArray(lineColorCfg) && lineColorCfg['@@function']) {
          l.lineColorConfig = lineColorCfg;
          l.lineColorRgba = null;
        } else {
          l.lineColorConfig = {};
          l.lineColorRgba = rgbToRgba(lineColorCfg, 1);
        }
      } else {
        // For hex layers (static or tile), legend uses these too
        l.fillColorConfig = fillColorCfg;
        l.lineColorConfig = lineColorCfg;
        l.opacity = debugState.opacity;
      }
      
      // Structural toggles require rebuilding Mapbox layers; numeric/style edits can update in-place.
      const currStructure = { filled: !!debugState.filled, stroked: !!debugState.stroked, extruded: !!debugState.extruded };
      const structuralChanged = !lastDebugStructure ||
        lastDebugStructure.filled !== currStructure.filled ||
        lastDebugStructure.stroked !== currStructure.stroked ||
        lastDebugStructure.extruded !== currStructure.extruded;

      // Re-render: Deck tile layers (if present)
      if (typeof rebuildDeckOverlay === 'function' && HAS_TILE_LAYERS) {
        rebuildDeckOverlay();
      }

      // Mapbox: update in-place when possible to avoid flicker
      if (!structuralChanged && typeof updateMapboxLayersFast === 'function') {
        const ok = updateMapboxLayersFast();
        if (!ok && typeof addAllLayers === 'function') addAllLayers();
      } else if (typeof addAllLayers === 'function') {
        addAllLayers();
      }

      lastDebugStructure = currStructure;
      if (typeof updateLegend === 'function') updateLegend();
      if (typeof updateLayerPanel === 'function') updateLayerPanel();
    }
    
    // Debounced apply for smooth slider experience
    function scheduleLayerUpdate() {
      // rAF throttle: reduces flicker and keeps updates responsive while typing quickly
      if (debugApplyTimeout) return;
      debugApplyTimeout = requestAnimationFrame(() => {
        debugApplyTimeout = null;
        applyLayerChanges();
      });
    }

    // Domain slider behavior (simplified):
    // - while dragging: only update the input boxes (no map repaint)
    // - on mouseup/touchend ("change"): apply the map update once
    function onDomainSliderInput() {
      syncDomainInputsFromSlider();
    }

    function onDomainSliderChange() {
      syncDomainInputsFromSlider();
      scheduleLayerUpdate();
    }
    
    // Initialize debug panel
    map.on('load', () => {
      
      // Initial sync should NOT cause initialViewState to appear in snippet output.
      syncDebugFromMap(false);
      initBasemapControl();
      initDebugLayerSelector();
      
      // Initialize DuckDB for SQL filtering
      if (HAS_SQL_LAYERS) {
        initDuckDB();
        const sqlInput = document.getElementById('dbg-sql');
        if (sqlInput) {
          sqlInput.addEventListener('input', scheduleSqlQuery);
        }
      }
      
      // Delay populating attrs until some tile data loads
      setTimeout(populateAttrDropdown, 500);
      setTimeout(populateAttrDropdown, 2000);
      
      // Sync on map move:
      // Only count as "user moved" after a real user interaction on the map canvas
      // (pointer/wheel). This avoids programmatic camera changes (auto-fit, jumpTo, etc.)
      // from polluting the snippet with initialViewState.
      try {
        const canvas = map.getCanvas();
        if (canvas) {
          canvas.addEventListener('pointerdown', () => { debugState.userMapInteracted = true; }, { passive: true });
          canvas.addEventListener('wheel', () => { debugState.userMapInteracted = true; }, { passive: true });
          canvas.addEventListener('touchstart', () => { debugState.userMapInteracted = true; }, { passive: true });
        }
      } catch (e) {}
      map.on('moveend', () => syncDebugFromMap(!!debugState.userMapInteracted));
      
      // Bind view inputs
      let viewUpdateTimeout = null;
      function scheduleViewUpdate() {
        clearTimeout(viewUpdateTimeout);
        viewUpdateTimeout = setTimeout(applyViewChanges, 50);
      }
      ['dbg-lng', 'dbg-lat', 'dbg-zoom', 'dbg-pitch', 'dbg-bearing'].forEach(id => {
        document.getElementById(id)?.addEventListener('input', scheduleViewUpdate);
        document.getElementById(id)?.addEventListener('change', applyViewChanges);
      });
      
      // Bind layer toggles
      ['dbg-filled', 'dbg-stroked', 'dbg-extruded'].forEach(id => {
        document.getElementById(id)?.addEventListener('change', applyLayerChanges);
      });
      
      // Bind opacity input directly
      ['dbg-opacity'].forEach(id => {
        document.getElementById(id)?.addEventListener('change', scheduleLayerUpdate);
        document.getElementById(id)?.addEventListener('input', scheduleLayerUpdate);
      });
      
      // Bind function type selectors
      document.getElementById('dbg-fill-fn')?.addEventListener('change', () => { updateFillFnOptions(); scheduleLayerUpdate(); });
      document.getElementById('dbg-line-fn')?.addEventListener('change', () => { updateLineFnOptions(); scheduleLayerUpdate(); });
      
      // Bind fill color inputs
      ['dbg-attr', 'dbg-palette', 'dbg-reverse-colors', 'dbg-domain-min', 'dbg-domain-max', 'dbg-steps', 'dbg-null-color', 'dbg-fill-static'].forEach(id => {
        document.getElementById(id)?.addEventListener('change', scheduleLayerUpdate);
        document.getElementById(id)?.addEventListener('input', scheduleLayerUpdate);
      });
      
      // Recalculate domain when attribute changes (uses DuckDB)
      document.getElementById('dbg-attr')?.addEventListener('change', async () => {
        const attr = document.getElementById('dbg-attr')?.value;
        if (!attr || !duckDbReady || !duckConn) return;
        
        try {
          const res = await duckConn.query(`SELECT MIN("${attr}") as min_val, MAX("${attr}") as max_val FROM data`);
          const row = res.toArray()[0];
          if (row) {
            let minVal = row.min_val, maxVal = row.max_val;
            if (typeof minVal === 'bigint') minVal = Number(minVal);
            if (typeof maxVal === 'bigint') maxVal = Number(maxVal);
            if (Number.isFinite(minVal) && Number.isFinite(maxVal)) {
              // Update slider extent only; don't override the user's selected domain.
              if (typeof setDomainSliderBounds === 'function') setDomainSliderBounds(minVal, maxVal);
              if (typeof syncDomainSliderFromInputs === 'function') syncDomainSliderFromInputs();
            }
          }
        } catch (e) { console.warn('Domain calc error:', e); }
      });
      // Domain dual slider -> inputs (no repaint while dragging)
      document.getElementById('dbg-domain-range-min')?.addEventListener('input', onDomainSliderInput);
      document.getElementById('dbg-domain-range-max')?.addEventListener('input', onDomainSliderInput);
      // On mouseup/touchend, apply once
      document.getElementById('dbg-domain-range-min')?.addEventListener('change', onDomainSliderChange);
      document.getElementById('dbg-domain-range-max')?.addEventListener('change', onDomainSliderChange);
      // Inputs -> domain dual slider
      document.getElementById('dbg-domain-min')?.addEventListener('input', () => { syncDomainSliderFromInputs(); });
      document.getElementById('dbg-domain-max')?.addEventListener('input', () => { syncDomainSliderFromInputs(); });
      
      // Bind line color inputs
      ['dbg-line-attr', 'dbg-line-palette', 'dbg-line-domain-min', 'dbg-line-domain-max', 'dbg-line-static', 'dbg-line-width'].forEach(id => {
        document.getElementById(id)?.addEventListener('change', scheduleLayerUpdate);
        document.getElementById(id)?.addEventListener('input', scheduleLayerUpdate);
      });
      
      // Bind elevation inputs
      ['dbg-height-attr', 'dbg-height-scale'].forEach(id => {
        document.getElementById(id)?.addEventListener('change', scheduleLayerUpdate);
        document.getElementById(id)?.addEventListener('input', scheduleLayerUpdate);
      });
      
      // Sync sliders with inputs
      syncSliderInput('dbg-opacity-slider', 'dbg-opacity');
      syncSliderInput('dbg-line-width-slider', 'dbg-line-width');
      
      // Sync color pickers with labels
      syncColorLabel('dbg-null-color', 'dbg-null-color-label');
      syncColorLabel('dbg-fill-static', 'dbg-fill-static-label');
      syncColorLabel('dbg-line-static', 'dbg-line-static-label');

      // Custom palette dropdowns (with previews)
      initPaletteDropdown('dbg-palette', 'dbg-palette-trigger', 'dbg-palette-menu', 'dbg-palette-name', 'dbg-palette-swatch');
      initPaletteDropdown('dbg-line-palette', 'dbg-line-palette-trigger', 'dbg-line-palette-menu', 'dbg-line-palette-name', 'dbg-line-palette-swatch');
      
      // Initialize section visibility
      updateSectionVisibility();
      updateFillFnOptions();
      updateLineFnOptions();

      // Resizable debug panel (right-edge grab handle)
      updateDebugTogglePosition();
      const panel = document.getElementById('debug-panel');
      const handle = document.getElementById('debug-resize-handle');
      if (panel && handle) {
        const minW = 240;
        const maxW = 520;
        let dragging = false;
        let startX = 0;
        let startW = 0;

        const onMove = (e) => {
          if (!dragging) return;
          const dx = e.clientX - startX;
          const next = Math.max(minW, Math.min(maxW, startW + dx));
          panel.style.width = `${next}px`;
          updateDebugTogglePosition();
        };
        const onUp = () => {
          if (!dragging) return;
          dragging = false;
          document.body.style.userSelect = '';
          window.removeEventListener('mousemove', onMove);
          window.removeEventListener('mouseup', onUp);
        };
        handle.addEventListener('mousedown', (e) => {
          if (panel.classList.contains('collapsed')) return;
          dragging = true;
          startX = e.clientX;
          startW = panel.getBoundingClientRect().width;
          document.body.style.userSelect = 'none';
          window.addEventListener('mousemove', onMove);
          window.addEventListener('mouseup', onUp);
        });
      }
    });
    {% endif %}
  </script>
</body>
</html>
""").render(
        mapbox_token=mapbox_token,
        layers_data=processed_layers,
        default_hex_config=DEFAULT_DECK_HEX_CONFIG,
        default_vector_config=DEFAULT_DECK_CONFIG,
        style_url=style_url,
        basemap=basemap_value,
        center_lng=center_lng,
        center_lat=center_lat,
        zoom=zoom,
        pitch=pitch,
        bearing=bearing,
        has_custom_view=has_custom_view,
        has_tile_layers=has_tile_layers,
        has_mvt_layers=has_mvt_layers,
        has_sql_layers=has_sql_layers,
        highlight_on_click=highlight_on_click,
        palettes=(["ArmyRose"] + sorted([p for p in KNOWN_CARTOCOLOR_PALETTES if p != "ArmyRose"])),
        on_click=on_click or {},
        debug=debug,
    )

    common = fused.load("https://github.com/fusedio/udfs/tree/bb3aa1b/public/common/")
    return common.html_to_obj(html)


def deckgl_map(
    gdf,
    config: typing.Union[dict, str, None] = None,
    mapbox_token: str = "pk.eyJ1IjoiaXNhYWNmdXNlZGxhYnMiLCJhIjoiY2xicGdwdHljMHQ1bzN4cWhtNThvbzdqcSJ9.73fb6zHMeO_c8eAXpZVNrA",
    basemap: str = "dark",
):

    layers = [
        {
            "type": "vector",
            "data": gdf,
            "config": config,
            "name": "Layer 1",
        }
    ]

    return deckgl_layers(
        layers=layers,
        mapbox_token=mapbox_token,
        basemap=basemap,
    )



def deckgl_raster(
    image_data=None,  # numpy array or image URL string
    bounds=None,  # [west, south, east, north]
    tile_url: str = None,  # XYZ tile URL with {z}/{x}/{y}
    config: dict = None,
    mapbox_token: str = "pk.eyJ1IjoiaXNhYWNmdXNlZGxhYnMiLCJhIjoiY2xicGdwdHljMHQ1bzN4cWhtNThvbzdqcSJ9.73fb6zHMeO_c8eAXpZVNrA",
    basemap: str = "dark",
):
    """
    Render raster data on a map - either a static image or XYZ tiles.
    
    Args:
        image_data: numpy array (H, W, 3 or 4) or image URL string (for static image)
        bounds: [west, south, east, north] geographic bounds (for static image)
        tile_url: XYZ tile URL with {z}/{x}/{y} placeholders (for tiled raster)
        config: Optional dict with initialViewState and rasterLayer config
        mapbox_token: Mapbox access token
        basemap: 'dark', 'satellite', 'light', or 'streets'
    
    Returns:
        HTML object for rendering
    
    Examples:
        # Static image
        deckgl_raster(image_array, bounds=[-122.5, 37.7, -122.3, 37.9])
        
        # Tiled raster
        deckgl_raster(tile_url="https://udf.ai/my_udf/run/tiles/{z}/{x}/{y}?dtype_out_raster=png")
    """
    import json
    import numpy as np
    import base64
    from io import BytesIO
    from copy import deepcopy
    
    # Basemap styles
    basemap_styles = {
        "dark": "mapbox://styles/mapbox/dark-v11",
        "satellite": "mapbox://styles/mapbox/satellite-streets-v12",
        "light": "mapbox://styles/mapbox/light-v11",
        "streets": "mapbox://styles/mapbox/streets-v12"
    }
    style_url = basemap_styles.get(basemap.lower(), basemap_styles["dark"])
    
    # Handle tiled raster mode
    if tile_url is not None:
        # Get view settings from config
        view = (config or {}).get("initialViewState", {})
        center = [view.get("longitude", 0), view.get("latitude", 0)]
        zoom = view.get("zoom", 2)
        
        html = Template(r"""
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="initial-scale=1,maximum-scale=1,user-scalable=no"/>
  <link href="https://api.mapbox.com/mapbox-gl-js/v3.2.0/mapbox-gl.css" rel="stylesheet"/>
  <script src="https://api.mapbox.com/mapbox-gl-js/v3.2.0/mapbox-gl.js"></script>
  <style>html,body{margin:0;height:100%}#map{position:absolute;inset:0}</style>
</head>
<body>
<div id="map"></div>
<script>
mapboxgl.accessToken = {{ mapbox_token | tojson }};
const map = new mapboxgl.Map({
  container: 'map',
  style: {{ style_url | tojson }},
  center: {{ center | tojson }},
  zoom: {{ zoom }},
  projection: 'mercator'
});

map.on('load', () => {
  map.addSource('raster-tiles', {
    type: 'raster',
    tiles: [{{ tile_url | tojson }}],
    tileSize: 256
  });
  
  map.addLayer({
    id: 'raster-tiles-layer',
    type: 'raster',
    source: 'raster-tiles'
  });
});

map.on('error', (e) => {
  console.error('[raster] Map error:', e.error?.message || e.error || e);
  if (e.tile) console.error('[raster] Failed tile:', e.tile.tileID?.canonical);
  if (e.source) console.error('[raster] Source:', e.sourceId);
});

</script>
</body>
</html>
""").render(
            mapbox_token=mapbox_token,
            tile_url=tile_url,
            style_url=style_url,
            center=center,
            zoom=zoom,
        )
        
        common = fused.load("https://github.com/fusedio/udfs/tree/bb3aa1b/public/common/")
        return common.html_to_obj(html)
    
    # Default config for static image mode
    DEFAULT_RASTER_CONFIG = {
        "initialViewState": {
            "longitude": None,
            "latitude": None,
            "zoom": 10,
            "pitch": 0,
            "bearing": 0
        },
        "rasterLayer": {
            "opacity": 1.0
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
    
    # Convert bounds to Mapbox image source coordinates format
    # [top-left, top-right, bottom-right, bottom-left]
    coordinates = [
        [west, north],   # top-left
        [east, north],   # top-right
        [east, south],   # bottom-right
        [west, south],   # bottom-left
    ]
    
    html = Template(r"""
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="initial-scale=1,maximum-scale=1,user-scalable=no"/>
  <link href="https://api.mapbox.com/mapbox-gl-js/v3.2.0/mapbox-gl.css" rel="stylesheet"/>
  <script src="https://api.mapbox.com/mapbox-gl-js/v3.2.0/mapbox-gl.js"></script>
  <style>html,body{margin:0;height:100%}#map{position:absolute;inset:0}</style>
</head>
<body>
<div id="map"></div>
<script>
mapboxgl.accessToken = {{ mapbox_token | tojson }};
const map = new mapboxgl.Map({
  container: 'map',
  style: {{ style_url | tojson }},
  center: [{{ center_lng }}, {{ center_lat }}],
  zoom: {{ zoom }},
  pitch: {{ pitch }},
  bearing: {{ bearing }},
  projection: 'mercator'
});

map.on('load', () => {
  map.addSource('raster-image', {
    type: 'image',
    url: {{ image_url | tojson }},
    coordinates: {{ coordinates | tojson }}
  });
  
  map.addLayer({
    id: 'raster-layer',
    type: 'raster',
    source: 'raster-image',
    paint: { 'raster-opacity': {{ opacity }} }
  });
  
  map.fitBounds([[{{ west }}, {{ south }}], [{{ east }}, {{ north }}]], { padding: 20, duration: 500 });
});
</script>
</body>
</html>
""").render(
        mapbox_token=mapbox_token,
        image_url=image_url,
        coordinates=coordinates,
        style_url=style_url,
        opacity=opacity,
        center_lng=center_lng,
        center_lat=center_lat,
        zoom=zoom,
        pitch=pitch,
        bearing=bearing,
        west=west,
        south=south,
        east=east,
        north=north,
    )

    common = fused.load("https://github.com/fusedio/udfs/tree/bb3aa1b/public/common/")
    return common.html_to_obj(html)


def enable_map_broadcast(html_input, channel: str = "fused-bus", dataset: str = "all"):
    """
    Inject viewport broadcast into any Mapbox GL map HTML.
    
    When the map moves, this broadcasts the visible bounds as a spatial filter
    that other components (like histograms) can use to filter their data.
    
    Args:
        html_input: HTML string or response object containing a Mapbox GL map
        channel: BroadcastChannel name (must match histogram's channel)
        dataset: Dataset identifier for filtering (use "all" to broadcast to all)
    
    Returns:
        Modified HTML with broadcast capability
    
    Usage:
        html = deckgl_hex(df, config)
        map_with_broadcast = enable_map_broadcast(html, channel="fused-bus")
        return map_with_broadcast
    """
    html_string, response_mode = _normalize_html_input(html_input)
    
    broadcast_script = f"""
<script>
(function() {{
  if (!window.mapboxgl || typeof map === 'undefined') return;
  
  const CHANNEL = {json.dumps(channel)};
  const DATASET = {json.dumps(dataset)};
  const componentId = 'map-broadcast-' + Math.random().toString(36).substr(2, 9);
  
  // Setup BroadcastChannel
  let bc = null;
  try {{ if ('BroadcastChannel' in window) bc = new BroadcastChannel(CHANNEL); }} catch (e) {{}}
  
  // Send message to all possible targets
  function busSend(obj) {{
    const s = JSON.stringify(obj);
    try {{ if (bc) bc.postMessage(obj); }} catch(e) {{}}
    try {{ window.parent.postMessage(s, '*'); }} catch(e) {{}}
    try {{ if (window.top && window.top !== window.parent) window.top.postMessage(s, '*'); }} catch(e) {{}}
    try {{
      if (window.top && window.top.frames) {{
        for (let i = 0; i < window.top.frames.length; i++) {{
          const f = window.top.frames[i];
          if (f !== window) try {{ f.postMessage(s, '*'); }} catch(e) {{}}
        }}
      }}
    }} catch(e) {{}}
  }}
  
  // Track last bounds to avoid duplicate broadcasts
  let lastBounds = null;
  
  function getBoundsArray() {{
    const b = map.getBounds();
    return [
      +b.getWest().toFixed(6),
      +b.getSouth().toFixed(6),
      +b.getEast().toFixed(6),
      +b.getNorth().toFixed(6)
    ];
  }}
  
  function boundsEqual(a, b) {{
    if (!a || !b) return false;
    return a[0] === b[0] && a[1] === b[1] && a[2] === b[2] && a[3] === b[3];
  }}
  
  function broadcastBounds() {{
    const bounds = getBoundsArray();
    if (boundsEqual(bounds, lastBounds)) return;
    lastBounds = bounds;
    
    const [west, south, east, north] = bounds;
    
    // Send unified filter message (compatible with histogram_message.py)
    busSend({{
      type: 'filter',
      fromComponent: componentId,
      timestamp: Date.now(),
      dataset: DATASET,
      filter: {{
        type: 'spatial',
        field: 'geometry',
        values: [west, south, east, north]  // [minLng, minLat, maxLng, maxLat]
      }}
    }});
    
    // Also send legacy format for backwards compatibility
    busSend({{
      type: 'spatial_filter',
      source: componentId,
      timestamp: Date.now(),
      bounds: {{
        sw: {{ lng: west, lat: south }},
        ne: {{ lng: east, lat: north }}
      }}
    }});
  }}
  
  // Listen for map movement - broadcast immediately on every move
  map.on('move', broadcastBounds);
  map.on('moveend', broadcastBounds);
  
  // Initial broadcast on load
  map.on('load', () => {{
    setTimeout(broadcastBounds, 300);
  }});
  
  // If map is already loaded, broadcast now
  if (map.loaded()) {{
    setTimeout(broadcastBounds, 100);
  }}
  
  // Announce component ready
  setTimeout(() => {{
    busSend({{
      type: 'component_ready',
      componentType: 'map',
      componentId: componentId,
      capabilities: ['spatial_filter'],
      dataSource: 'viewport',
      protocol: 'unified'
    }});
  }}, 200);
  
}})();
</script>
"""
    
    # Inject before closing </body> tag
    if "</body>" in html_string:
        injected_html = html_string.replace("</body>", f"{broadcast_script}\n</body>")
    elif "</html>" in html_string:
        injected_html = html_string.replace("</html>", f"{broadcast_script}\n</html>")
    else:
        injected_html = html_string + broadcast_script

    if response_mode:
        common = fused.load("https://github.com/fusedio/udfs/tree/bb3aa1b/public/common/")
        return common.html_to_obj(injected_html)
    return injected_html


def enable_location_listener(html_input, channel: str = "fused-bus", zoom_offset: float = 0, padding: int = 50, max_zoom: int = 18):
    """
    Inject location listener into any Mapbox GL map HTML.
    
    When a location_selector broadcasts a location change, this map will
    automatically pan/zoom to fit the specified bounds.
    
    Args:
        html_input: HTML string or response object containing a Mapbox GL map
        channel: BroadcastChannel name (must match location_selector's channel)
        zoom_offset: Extra zoom levels to add after fitBounds (e.g., 0.5 for tighter fit)
        padding: Padding in pixels around bounds (smaller = tighter fit)
        max_zoom: Maximum zoom level allowed
    
    Returns:
        Modified HTML with location listener capability
    
    Usage:
        html = deckgl_hex(df, config)
        # Default fit
        map_with_listener = enable_location_listener(html, channel="fused-bus")
        # Tighter fit (zoom in 0.5 more)
        map_with_listener = enable_location_listener(html, zoom_offset=0.5, padding=20)
        return map_with_listener
    """
    html_string, response_mode = _normalize_html_input(html_input)
    
    listener_script = f"""
<script>
(function() {{
  if (typeof map === 'undefined') return;
  
  const CHANNEL = {json.dumps(channel)};
  const ZOOM_OFFSET = {zoom_offset};
  const PADDING = {padding};
  const MAX_ZOOM = {max_zoom};
  const componentId = 'map-location-listener-' + Math.random().toString(36).substr(2, 9);
  
  // Setup BroadcastChannel
  let bc = null;
  try {{ if ('BroadcastChannel' in window) bc = new BroadcastChannel(CHANNEL); }} catch (e) {{}}
  
  function handleLocationChange(message) {{
    try {{
      if (typeof message === 'string') {{
        try {{ message = JSON.parse(message); }} catch (_) {{ return; }}
      }}
      if (!message) return;
      
      // Handle location_change messages from location_selector
      if (message.type === 'location_change' && message.location) {{
        const loc = message.location;
        
        if (loc.bounds && Array.isArray(loc.bounds) && loc.bounds.length === 4) {{
          const [west, south, east, north] = loc.bounds;
          
          // Validate bounds are finite numbers
          if (!Number.isFinite(west) || !Number.isFinite(south) || 
              !Number.isFinite(east) || !Number.isFinite(north)) {{
            console.warn('[map_location_listener] Invalid bounds (NaN/Infinity):', loc.bounds);
            return;
          }}
          
          // Calculate adaptive padding - reduce if bounds are small
          // to prevent NaN from oversized padding on small areas
          const boundsWidth = Math.abs(east - west);
          const boundsHeight = Math.abs(north - south);
          const minDim = Math.min(boundsWidth, boundsHeight);
          // For very small areas (< 0.1 degrees ≈ 10km), cap padding at 30
          const adaptivePadding = minDim < 0.1 ? Math.min(PADDING, 30) : PADDING;
          
          // Fit map to bounds with optional zoom offset
          if (ZOOM_OFFSET > 0) {{
            // First fit bounds, then zoom in a bit more
            map.once('moveend', () => {{
              const currentZoom = map.getZoom();
              const targetZoom = Math.min(currentZoom + ZOOM_OFFSET, MAX_ZOOM);
              map.easeTo({{ zoom: targetZoom, duration: 300 }});
            }});
          }}
          
          try {{
          map.fitBounds(
            [[west, south], [east, north]],
            {{
                padding: adaptivePadding,
                duration: 800,
                maxZoom: MAX_ZOOM
              }}
            );
          }} catch (fitError) {{
            console.warn('[map_location_listener] fitBounds failed, using flyTo fallback:', fitError);
            // Fallback: just fly to center
            const centerLng = (west + east) / 2;
            const centerLat = (south + north) / 2;
            map.flyTo({{ center: [centerLng, centerLat], zoom: 14, duration: 800 }});
          }}
        }}
      }}
    }} catch (e) {{
      console.warn('[map_location_listener] Error handling message:', e);
    }}
  }}
  
  // Listen on BroadcastChannel
  if (bc) {{
    bc.onmessage = (ev) => handleLocationChange(ev.data);
  }}
  
  // Also listen via postMessage
  window.addEventListener('message', (ev) => handleLocationChange(ev.data));
  
}})();
</script>
"""
    
    # Inject before closing </body> tag
    if "</body>" in html_string:
        injected_html = html_string.replace("</body>", f"{listener_script}\n</body>")
    elif "</html>" in html_string:
        injected_html = html_string.replace("</html>", f"{listener_script}\n</html>")
    else:
        injected_html = html_string + listener_script

    if response_mode:
        common = fused.load("https://github.com/fusedio/udfs/tree/bb3aa1b/public/common/")
        return common.html_to_obj(injected_html)
    return injected_html


def enable_hex_click_broadcast(
    html_input,
    channel: str = "fused-bus",
    message_type: str = "hex_click",
    properties: list = None,
):
    html_string, response_mode = _normalize_html_input(html_input)
    properties_js = json.dumps(properties) if properties else "null"
    
    click_script = f"""
<script>
(function() {{
  if (typeof map === 'undefined') return;
  
  const CHANNEL = {json.dumps(channel)};
  const MESSAGE_TYPE = {json.dumps(message_type)};
  const PROPERTIES_FILTER = {properties_js};
  const componentId = 'hex-click-' + Math.random().toString(36).substr(2, 9);
  
  // Setup BroadcastChannel
  let bc = null;
  try {{ if ('BroadcastChannel' in window) bc = new BroadcastChannel(CHANNEL); }} catch (e) {{}}
  
  // Send message to all possible targets
  function busSend(obj) {{
    const s = JSON.stringify(obj);
    try {{ if (bc) bc.postMessage(obj); }} catch(e) {{}}
    try {{ window.parent.postMessage(s, '*'); }} catch(e) {{}}
    try {{ if (window.top && window.top !== window.parent) window.top.postMessage(s, '*'); }} catch(e) {{}}
    try {{
      if (window.top && window.top.frames) {{
        for (let i = 0; i < window.top.frames.length; i++) {{
          const f = window.top.frames[i];
          if (f !== window) try {{ f.postMessage(s, '*'); }} catch(e) {{}}
        }}
      }}
    }} catch(e) {{}}
  }}
  
  // Get all queryable layer IDs
  function getQueryableLayers() {{
    const layers = [];
    // Try to get layers from LAYERS_DATA if available (multi-layer hex maps)
    if (typeof LAYERS_DATA !== 'undefined' && Array.isArray(LAYERS_DATA)) {{
      LAYERS_DATA.forEach(l => {{
        if (!l.isTileLayer) {{
          const cfg = l.hexLayer || {{}};
          if (cfg.extruded) {{
            layers.push(`${{l.id}}-extrusion`);
          }} else {{
            layers.push(`${{l.id}}-fill`);
          }}
          layers.push(`${{l.id}}-outline`);
        }}
      }});
    }}
    // Also try common layer IDs
    ['gdf-fill', 'gdf-circle', 'hex-fill', 'hex-line'].forEach(id => {{
      if (map.getLayer(id) && !layers.includes(id)) layers.push(id);
    }});
    return layers.filter(id => {{ try {{ return map.getLayer(id); }} catch(e) {{ return false; }} }});
  }}
  
  // Handle click on hex
  map.on('click', (e) => {{
    const layers = getQueryableLayers();
    if (!layers.length) return;
    
    const features = map.queryRenderedFeatures(e.point, {{ layers }});
    if (!features || !features.length) return;
    
    const feature = features[0];
    const props = feature.properties || {{}};
    const hexId = props.hex || props.h3 || props.index || props.id || null;
    
    // Filter properties if specified
    let propsToSend = {{}};
    if (PROPERTIES_FILTER && Array.isArray(PROPERTIES_FILTER)) {{
      PROPERTIES_FILTER.forEach(key => {{
        if (props[key] !== undefined) propsToSend[key] = props[key];
      }});
    }} else {{
      propsToSend = {{ ...props }};
    }}
    
    // Send the click message
    const message = {{
      type: MESSAGE_TYPE,
      hex: hexId,
      properties: propsToSend,
      lngLat: {{ lng: e.lngLat.lng, lat: e.lngLat.lat }},
      timestamp: Date.now(),
      fromComponent: componentId
    }};
    
    busSend(message);
  }});
  
  // Also handle clicks on Deck.gl tile layers if present
  if (typeof deckOverlay !== 'undefined' && deckOverlay) {{
    map.on('click', (e) => {{
      const info = deckOverlay.pickObject({{ x: e.point.x, y: e.point.y, radius: 4 }});
      if (info?.object) {{
        const obj = info.object;
        const props = obj?.properties || obj || {{}};
        const hexId = props.hex || obj.hex || null;
        
        let propsToSend = {{}};
        if (PROPERTIES_FILTER && Array.isArray(PROPERTIES_FILTER)) {{
          PROPERTIES_FILTER.forEach(key => {{
            const val = props[key] ?? obj[key];
            if (val !== undefined) propsToSend[key] = val;
          }});
        }} else {{
          propsToSend = {{ ...props }};
        }}
        
        const message = {{
          type: MESSAGE_TYPE,
          hex: hexId,
          properties: propsToSend,
          lngLat: {{ lng: e.lngLat.lng, lat: e.lngLat.lat }},
          timestamp: Date.now(),
          fromComponent: componentId
        }};
        
        busSend(message);
      }}
    }});
  }}
  
}})();
</script>
"""
    
    # Inject before closing </body> tag
    if "</body>" in html_string:
        injected_html = html_string.replace("</body>", f"{click_script}\n</body>")
    elif "</html>" in html_string:
        injected_html = html_string.replace("</html>", f"{click_script}\n</html>")
    else:
        injected_html = html_string + click_script

    if response_mode:
        common = fused.load("https://github.com/fusedio/udfs/tree/bb3aa1b/public/common/")
        return common.html_to_obj(injected_html)
    return injected_html


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
    html_string, response_mode = _normalize_html_input(html_input)
    
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
        common = fused.load("https://github.com/fusedio/udfs/tree/bb3aa1b/public/common/")
        return common.html_to_obj(injected_html)
    return injected_html


def _normalize_html_input(html_input):
    """
    Convert HTML string or response-like object to (html_string, response_mode) tuple.
    
    Args:
        html_input: HTML string or response object with 'text', 'data', or 'body' attribute
    
    Returns:
        (html_string, response_mode): Normalized HTML string and whether input was a response object
    """
    response_mode = not isinstance(html_input, str)
    if isinstance(html_input, str):
        return html_input, False
    
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
    return html_string, response_mode


def _deep_merge_dict(base: dict, extra: dict) -> dict:
    for key, value in extra.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            _deep_merge_dict(base[key], value)
        else:
            base[key] = value
    return base


def _load_deckgl_config(raw_config, default_config):
    """Merge user config with defaults. Returns merged config dict."""
    merged = deepcopy(default_config)
    if raw_config in (None, ""):
        return merged

    if isinstance(raw_config, str):
        try:
            user_config = json.loads(raw_config)
        except json.JSONDecodeError:
            return merged
    elif isinstance(raw_config, dict):
        user_config = raw_config
    else:
        return merged

    if not isinstance(user_config, dict):
        return merged

    try:
        return _deep_merge_dict(merged, deepcopy(user_config))
    except Exception:
        return deepcopy(default_config)


def _get_view_state(config: dict, default_view_state: dict):
    """Get initialViewState from config, falling back to defaults."""
    view_state = config.get("initialViewState")
    if not isinstance(view_state, dict):
        config["initialViewState"] = deepcopy(default_view_state)
        return config["initialViewState"]
    return view_state


def _get_layer_config(config: dict, layer_key: str, default_layer: dict):
    """Get layer config from config, falling back to defaults."""
    layer = config.get(layer_key)
    if not isinstance(layer, dict):
        config[layer_key] = deepcopy(default_layer)
    return config[layer_key]




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


def _sanitize_geojson_value(v):
    """Sanitize a value for JSON serialization (shared utility)."""
    import math
    if isinstance(v, (float, int, str, bool)) or v is None:
        if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            return None
        return v
    if hasattr(v, 'item'):  # numpy types
        try:
            return v.item()
        except Exception:
            return str(v)
    if isinstance(v, (list, tuple, set)):
        return ", ".join(str(s) for s in v)
    return str(v) if v is not None else None


# ============================================================================
# PYDECK UTILITY FUNCTIONS
# ============================================================================

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


# ============================================================================
# PYDECK CONFIGS
# ============================================================================

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


def _normalize_pydeck_config(default_cfg: dict, config: typing.Union[dict, str, None]):
    """
    Merge a user config (dict or JSON string) with a default pydeck config.
    - None / \"\"  -> just a shallow copy of default_cfg
    - str         -> JSON-decoded and merged over defaults
    - dict        -> merged over defaults
    """
    cfg = default_cfg.copy()
    if config is None or config == "":
        return cfg
    if isinstance(config, str):
        try:
            cfg.update(json.loads(config))
        except Exception:
            # If JSON is invalid, fall back to defaults silently
            return cfg
    else:
        cfg.update(config)
    return cfg


def pydeck_point(gdf, config=None):
    """
    Pydeck based maps. Use this to render HTML interactive maps from data.

    Takes a config dict based on:
    'config = {
        "fill_color": '[255, 100 + cnt, 0]' # dynamically sets the colors of the fill based on the `cnt` values col
    '
    """
    cfg = _normalize_pydeck_config(DEFAULT_CONFIG, config)

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
def pydeck_hex(df=None, config: typing.Union[dict, str, None] = None):
    """
    Pydeck based maps. Use this to render HTML interactive maps from data.

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

    config = _normalize_pydeck_config(DEFAULT_H3_CONFIG, config)

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
    Pydeck based maps. Use this to render HTML interactive maps from data.

    Takes a config dict based on:
    'config = {
        "fill_color": '[255, 100 + cnt, 0]' # dynamically sets the colors of the fill based on the `cnt` values col
    '
    """
    cfg = _normalize_pydeck_config(DEFAULT_POLYGON_CONFIG, config)

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
    return deck.to_html(as_string=True)