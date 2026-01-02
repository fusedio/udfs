
import json
import typing
import numpy as np
import pandas as pd
import geopandas as gpd
from copy import deepcopy

import fused



# ============================================================
# Default Configurations
# ============================================================

DEFAULT_DECK_HEX_CONFIG = {
    "initialViewState": {
        "longitude": None,
        "latitude": None,
        "zoom": 8,
        "pitch": 0,
        "bearing": 0
    },
    "hexLayer": {
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
        "pointRadiusMinPixels": 10,
        "pickable": True,
        "lineWidthMinPixels": 0,
        "getFillColor": {
            "@@function": "colorContinuous",
            "attr": "house_age",
            "domain": [0, 50],
            "colors": "ArmyRose",
            "steps": 7,
            "nullColor": [200, 200, 200, 180]
        }
    }
}

DEFAULT_DECK_RASTER_CONFIG = {
    "rasterLayer": {
        "opacity": 1.0,
    }
}

VALID_HEX_LAYER_PROPS = {
    "@@type", "filled", "stroked", "pickable", "extruded", "opacity",
    "getFillColor", "getLineColor", "lineWidthMinPixels", "elevationScale",
    "getHexagon", "tooltipColumns", "tooltipAttrs", "coverage", "id",
    "visible", "data", "sql", "transitions", "highlightColor", "autoHighlight"
}

# ============================================================
# CDN URLs
# ============================================================

FUSEDMAPS_CDN_JS = "https://cdn.jsdelivr.net/gh/milind-soni/fusedmaps@58632bc/dist/fusedmaps.umd.js"
FUSEDMAPS_CDN_CSS = "https://cdn.jsdelivr.net/gh/milind-soni/fusedmaps@58632bc/dist/fusedmaps.css"

# ============================================================
# Minimal HTML Template
# ============================================================

MINIMAL_TEMPLATE = """<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <link href="https://api.mapbox.com/mapbox-gl-js/v3.2.0/mapbox-gl.css" rel="stylesheet" />
  <script src="https://api.mapbox.com/mapbox-gl-js/v3.2.0/mapbox-gl.js"></script>
  <script src="https://unpkg.com/h3-js@4.1.0/dist/h3-js.umd.js"></script>
  {extra_scripts}
  <script type="module">
    import * as cartocolor from 'https://esm.sh/cartocolor@5.0.2';
    window.cartocolor = cartocolor;
  </script>
  <link href="{css_url}" rel="stylesheet" />
  <script src="{js_url}"></script>
  <style>
    html, body {{ margin:0; height:100%; width:100%; display:flex; overflow:hidden; }}
    #map {{ flex:1; height:100%; }}
  </style>
</head>
<body>
  <div id="map"></div>
  <script>
    // Wait for cartocolor to load, then initialize
    function waitForCartocolor(callback) {{
      if (window.cartocolor) {{
        callback();
      }} else {{
        setTimeout(() => waitForCartocolor(callback), 50);
      }}
    }}
    
    waitForCartocolor(() => {{
      const config = {config_json};
      FusedMaps.init(config);
    }});
  </script>
</body>
</html>"""


# ============================================================
# Main Functions
# ============================================================

@fused.udf(cache_max_age=0)
def udf(
    config: typing.Union[dict, str, None] = None,
    mapbox_token: str = "pk.eyJ1IjoiaXNhYWNmdXNlZGxhYnMiLCJhIjoiY2xicGdwdHljMHQ1bzN4cWhtNThvbzdqcSJ9.73fb6zHMeO_c8eAXpZVNrA",
    basemap: str = "dark",
    theme: str = "dark",
    n_points: int = 50,
    seed: int = 0,
    debug: bool = True,
):
    """Example UDF using deckgl_map with DEFAULT_DECK_CONFIG."""
    import geopandas as gpd
    from shapely.geometry import Point
    import numpy as np
    
    # Create sample point data around New York City
    # Fields match DEFAULT_DECK_CONFIG expectations
    rng = np.random.default_rng(int(seed))
    data = []
    base_lat, base_lng = 40.7128, -74.0060
    
    for i in range(int(n_points)):
        lat = base_lat + (i % 10 - 5) * 0.01
        lng = base_lng + (i // 10 - 2) * 0.01
        data.append({
            'geometry': Point(lng, lat),
            'house_age': (i % 50),  # 0-49 years, matches domain [0, 50]
            'mrt_distance': int(rng.integers(100, 5000)),  # meters
            'price': int(rng.integers(200_000, 800_000)),  # dollars
            'index': i
        })
    
    gdf = gpd.GeoDataFrame(data, crs='EPSG:4326')
    
    # Use DEFAULT_DECK_CONFIG if none provided (for points/vectors)
    if config is None:
        config = DEFAULT_DECK_CONFIG
    
    # IMPORTANT: `deckgl_layers()` reads initialViewState from its own argument,
    # not from a per-layer config. Keep the per-layer config focused on vector styling.
    view_state = {"longitude": -74.0060, "latitude": 40.7128, "zoom": 12, "pitch": 0, "bearing": 0}

    layers = [{"type": "vector", "data": gdf, "config": config, "name": "Sample Points"}]
    return deckgl_layers(
        layers=layers,
        mapbox_token=mapbox_token,
        basemap=basemap,
        theme=theme,
        initialViewState=view_state,
        debug=debug,
    )



def deckgl_layers(
    layers: list,
    mapbox_token: str = "pk.eyJ1IjoiaXNhYWNmdXNlZGxhYnMiLCJhIjoiY2xicGdwdHljMHQ1bzN4cWhtNThvbzdqcSJ9.73fb6zHMeO_c8eAXpZVNrA",
    basemap: str = "dark",
    initialViewState: typing.Optional[dict] = None,
    theme: str = "dark",
    highlight_on_click: bool = True,
    on_click: dict = None,
    debug: bool = False,
):
    """
    Render mixed hex and vector layers on a single interactive map.
    
    This refactored version uses the FusedMaps CDN package instead of
    embedding all JavaScript in the template.
    
    Args:
        layers: List of layer dicts, each with:
            - "type": "hex" or "vector" (required)
            - "data": DataFrame with hex column (for hex) or GeoDataFrame (for vector)
            - "tile_url": XYZ tile URL template (for hex tile layers)
            - "config": Layer config dict
            - "name": Display name for layer toggle (optional)
        mapbox_token: Mapbox access token.
        basemap: 'dark', 'satellite', 'light', or 'streets'.
        initialViewState: Optional view state override.
        theme: UI theme ('dark' or 'light').
        highlight_on_click: Enable click-to-highlight.
        on_click: Click broadcast config.
    
    Returns:
        HTML object for rendering in Fused Workbench
    """
    
    # Basemap styles
    basemap_styles = {
        "dark": "mapbox://styles/mapbox/dark-v11",
        "satellite": "mapbox://styles/mapbox/satellite-streets-v12",
        "light": "mapbox://styles/mapbox/light-v11",
        "streets": "mapbox://styles/mapbox/streets-v12"
    }
    style_url = basemap_styles.get(basemap.lower(), basemap_styles["dark"])
    
    # Process each layer
    processed_layers = []
    auto_center = None
    has_tile_layers = False
    
    for i, layer_def in enumerate(layers):
        layer_type = layer_def.get("type", "hex").lower()
        df = layer_def.get("data")
        tile_url = layer_def.get("tile_url")
        source_layer = layer_def.get("source_layer") or layer_def.get("sourceLayer")
        image_url = layer_def.get("image_url")
        bounds = layer_def.get("bounds")
        config = layer_def.get("config", {})
        name = layer_def.get("name", f"Layer {i + 1}")
        visible = layer_def.get("visible", True)
        
        if layer_type == "hex":
            processed = _process_hex_layer(i, df, tile_url, config, name, visible)
            if processed:
                processed_layers.append(processed)
                if processed.get("isTileLayer"):
                    has_tile_layers = True
                # Auto-center from first layer with data
                if auto_center is None and df is not None and len(df) > 0:
                    auto_center = _compute_center_from_hex(df)
        
        elif layer_type == "vector":
            # If a tile_url + source_layer is provided, treat it as an MVT layer (vector tiles),
            # otherwise treat it as a GeoJSON vector layer (static data).
            if tile_url:
                processed = _process_mvt_layer(i, tile_url, source_layer, config, name, visible)
            else:
                processed = _process_vector_layer(i, df, config, name, visible)
            if processed:
                processed_layers.append(processed)
                # Auto-center from polygons/points
                if auto_center is None and df is not None and len(df) > 0:
                    auto_center = _compute_center_from_gdf(df)
        
        elif layer_type == "raster":
            processed = _process_raster_layer(i, tile_url, image_url, bounds, config, name, visible)
            if processed:
                processed_layers.append(processed)
        elif layer_type == "mvt":
            processed = _process_mvt_layer(i, tile_url, source_layer, config, name, visible)
            if processed:
                processed_layers.append(processed)
    
    # Build initial view state
    if initialViewState:
        view_state = initialViewState
        has_custom_view = True
    else:
        # Use auto-detected center or default
        center = auto_center or {"longitude": -122.4, "latitude": 37.8}
        view_state = {
            "longitude": center.get("longitude", -122.4),
            "latitude": center.get("latitude", 37.8),
            "zoom": 10,
            "pitch": 0,
            "bearing": 0
        }
        has_custom_view = False
    
    # Build config for FusedMaps
    fusedmaps_config = {
        "containerId": "map",
        "mapboxToken": mapbox_token,
        "styleUrl": style_url,
        "initialViewState": view_state,
        "layers": processed_layers,
        "hasCustomView": has_custom_view,
        "debug": bool(debug),
        "ui": {
            "legend": True,
            "layerPanel": True,
            "tooltip": True,
            "theme": theme
        },
        "highlightOnClick": highlight_on_click,
    }
    
    # Add messaging config if on_click specified
    if on_click:
        fusedmaps_config["messaging"] = {
            "clickBroadcast": {
                "enabled": True,
                **on_click
            }
        }
    
    # Extra scripts needed for hex tile layers (Deck.gl + hyparquet)
    extra_scripts = ""
    if has_tile_layers:
        extra_scripts = """
  <script src="https://unpkg.com/deck.gl@9.1.3/dist.min.js"></script>
  <script src="https://unpkg.com/@deck.gl/geo-layers@9.1.3/dist.min.js"></script>
  <script type="module">
    import * as hyparquet from "https://cdn.jsdelivr.net/npm/hyparquet@1.23.3/+esm";
    window.hyparquet = hyparquet;
  </script>
"""

    # Generate HTML
    html = MINIMAL_TEMPLATE.format(
        css_url=FUSEDMAPS_CDN_CSS,
        js_url=FUSEDMAPS_CDN_JS,
        config_json=json.dumps(fusedmaps_config),
        extra_scripts=extra_scripts
    )
    
    # Return as HTML object
    common = fused.load("https://github.com/fusedio/udfs/tree/bb3aa1b/public/common/")
    return common.html_to_obj(html)


def deckgl_hex(
    df=None,
    config=None,
    mapbox_token: str = "pk.eyJ1IjoiaXNhYWNmdXNlZGxhYnMiLCJhIjoiY2xicGdwdHljMHQ1bzN4cWhtNThvbzdqcSJ9.73fb6zHMeO_c8eAXpZVNrA",
    basemap: str = "dark",
    tile_url: str = None,
    layers: list = None,
    highlight_on_click: bool = True,
    on_click: dict = None,
):
    """
    Render H3 hexagon layer(s) on an interactive map.
    
    Convenience wrapper around deckgl_layers() for hex-only use cases.
    """
    if layers is None:
        if df is not None:
            layers = [{"type": "hex", "data": df, "config": config, "name": "Layer 1"}]
        elif tile_url is not None:
            layers = [{"type": "hex", "tile_url": tile_url, "config": config, "name": "Tile Layer"}]
        else:
            raise ValueError("Provide df, tile_url, or layers parameter")
    else:
        layers = [{"type": "hex", **layer_def} for layer_def in layers]
    
    return deckgl_layers(
        layers=layers,
        mapbox_token=mapbox_token,
        basemap=basemap,
        highlight_on_click=highlight_on_click,
        on_click=on_click,
    )


def deckgl_map(
    gdf,
    config: typing.Union[dict, str, None] = None,
    mapbox_token: str = "pk.eyJ1IjoiaXNhYWNmdXNlZGxhYnMiLCJhIjoiY2xicGdwdHljMHQ1bzN4cWhtNThvbzdqcSJ9.73fb6zHMeO_c8eAXpZVNrA",
    basemap: str = "dark",
):
    """
    Render a GeoDataFrame on an interactive map.
    
    Convenience wrapper around deckgl_layers() for vector-only use cases.
    """
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
    image_data=None,
    bounds=None,
    tile_url: str = None,
    config: dict = None,
    mapbox_token: str = "pk.eyJ1IjoiaXNhYWNmdXNlZGxhYnMiLCJhIjoiY2xicGdwdHljMHQ1bzN4cWhtNThvbzdqcSJ9.73fb6zHMeO_c8eAXpZVNrA",
    basemap: str = "dark",
):
    """
    Render raster data on a map - either a static image or XYZ tiles.
    """
    if tile_url is not None:
        layers = [{"type": "raster", "tile_url": tile_url, "config": config, "name": "Raster"}]
        return deckgl_layers(
            layers=layers,
            mapbox_token=mapbox_token,
            basemap=basemap,
        )
    
    if image_data is None:
        raise ValueError("Provide either tile_url or image_data (with bounds=[west, south, east, north])")
    if bounds is None or len(bounds) != 4:
        raise ValueError("Static raster requires bounds=[west, south, east, north]")

    image_url = None
    if isinstance(image_data, str):
        image_url = image_data
    else:
        # Encode numpy array -> PNG data URL
        import base64
        import io
        import numpy as np
        from PIL import Image

        arr = np.asarray(image_data)
        # Common raster convention (rasterio): (bands, height, width). Convert to (height, width, bands).
        if arr.ndim == 3 and arr.shape[0] in (1, 3, 4) and arr.shape[1] > 1 and arr.shape[2] > 1:
            arr = np.transpose(arr, (1, 2, 0))

        if arr.ndim == 2:
            mode = "L"
        elif arr.ndim == 3 and arr.shape[2] == 3:
            mode = "RGB"
        elif arr.ndim == 3 and arr.shape[2] == 4:
            mode = "RGBA"
        else:
            raise ValueError(f"Unsupported image_data shape: {arr.shape} (expected HxW, HxWx3, or HxWx4)")

        if arr.dtype != np.uint8:
            arr = np.clip(arr, 0, 255).astype(np.uint8)

        im = Image.fromarray(arr, mode=mode)
        buf = io.BytesIO()
        im.save(buf, format="PNG")
        b64 = base64.b64encode(buf.getvalue()).decode("ascii")
        image_url = f"data:image/png;base64,{b64}"

    layers = [{"type": "raster", "image_url": image_url, "bounds": list(bounds), "config": config, "name": "Raster"}]
    return deckgl_layers(
        layers=layers,
        mapbox_token=mapbox_token,
        basemap=basemap,
    )


# ============================================================
# Layer Processing Functions
# ============================================================

def _process_hex_layer(idx: int, df, tile_url: str, config: dict, name: str, visible: bool) -> dict:
    """Process a hex layer definition into FusedMaps format."""
    
    # Parse config
    if isinstance(config, str):
        try:
            config = json.loads(config)
        except:
            config = {}
    config = config or {}
    
    # Merge with defaults
    merged_config = _deep_merge_dict(deepcopy(DEFAULT_DECK_HEX_CONFIG), config)
    hex_layer = merged_config.get("hexLayer", {})
    
    # Clean invalid props
    for key in list(hex_layer.keys()):
        if key not in VALID_HEX_LAYER_PROPS:
            hex_layer.pop(key, None)
    
    is_tile_layer = tile_url is not None

    # Tile layer config pass-through (for Deck.gl TileLayer options)
    tile_layer_config = None
    if is_tile_layer and isinstance(config, dict):
        tile_layer_config = config.get("tileLayerConfig") or config.get("tileLayer") or None
    
    # Process data
    data_records = []
    if not is_tile_layer and df is not None and hasattr(df, 'to_dict'):
        # Drop geometry column
        df_clean = df.drop(columns=['geometry'], errors='ignore') if hasattr(df, 'drop') else df
        df_clean = df_clean.copy()
        
        # Convert hex IDs to string format
        hex_col = next((c for c in ['hex', 'h3', 'index', 'id'] if c in df_clean.columns), None)
        if hex_col:
            df_clean['hex'] = df_clean[hex_col].apply(_to_hex_str)
        
        # Convert to records
        data_records = _sanitize_records(df_clean.to_dict('records'))
    
    # Extract tooltip columns
    tooltip_columns = _extract_tooltip_columns(config, hex_layer)
    
    return {
        "id": f"layer-{idx}",
        "name": name,
        "layerType": "hex",
        "data": data_records,
        "tileUrl": tile_url,
        "isTileLayer": is_tile_layer,
        "tileLayerConfig": tile_layer_config,
        "hexLayer": hex_layer,
        "tooltipColumns": tooltip_columns,
        "visible": visible,
    }


def _process_vector_layer(idx: int, df, config: dict, name: str, visible: bool) -> dict:
    """Process a vector layer definition into FusedMaps format."""
    
    if df is None:
        return None
    
    # Parse config
    if isinstance(config, str):
        try:
            config = json.loads(config)
        except:
            config = {}
    config = config or {}
    
    # Merge with defaults
    merged_config = _deep_merge_dict(deepcopy(DEFAULT_DECK_CONFIG), config)
    vector_layer = merged_config.get("vectorLayer", {})
    
    # Reproject to EPSG:4326 if needed
    if hasattr(df, "crs") and df.crs and getattr(df.crs, "to_epsg", lambda: None)() != 4326:
        try:
            df = df.to_crs(epsg=4326)
        except:
            pass
    
    # Convert to GeoJSON
    geojson_obj = {"type": "FeatureCollection", "features": []}
    if hasattr(df, "to_json"):
        try:
            geojson_obj = json.loads(df.to_json())
        except:
            pass
    
    # Sanitize properties and add index
    for idx_f, feat in enumerate(geojson_obj.get("features", [])):
        feat["properties"] = {k: _sanitize_value(v) for k, v in (feat.get("properties") or {}).items()}
        feat["properties"]["_fused_idx"] = idx_f
    
    # Extract color config
    fill_color_config = None
    fill_color_rgba = None
    is_filled = vector_layer.get("filled", True)
    
    if is_filled:
        fill_color_raw = vector_layer.get("getFillColor")
        if isinstance(fill_color_raw, dict) and fill_color_raw.get("@@function"):
            fill_color_config = fill_color_raw
        elif isinstance(fill_color_raw, (list, tuple)) and len(fill_color_raw) >= 3:
            r, g, b = int(fill_color_raw[0]), int(fill_color_raw[1]), int(fill_color_raw[2])
            a = fill_color_raw[3] / 255.0 if len(fill_color_raw) > 3 else 0.6
            fill_color_rgba = f"rgba({r},{g},{b},{a})"
    
    # Line color
    line_color_raw = vector_layer.get("getLineColor")
    line_color_rgba = None
    line_color_config = None
    
    if isinstance(line_color_raw, dict) and line_color_raw.get("@@function"):
        line_color_config = line_color_raw
    elif isinstance(line_color_raw, (list, tuple)) and len(line_color_raw) >= 3:
        r, g, b = int(line_color_raw[0]), int(line_color_raw[1]), int(line_color_raw[2])
        a = line_color_raw[3] / 255.0 if len(line_color_raw) > 3 else 1.0
        line_color_rgba = f"rgba({r},{g},{b},{a})"
    
    tooltip_columns = _extract_tooltip_columns(config, vector_layer)
    
    return {
        "id": f"layer-{idx}",
        "name": name,
        "layerType": "vector",
        "geojson": geojson_obj,
        "vectorLayer": vector_layer,
        "fillColorConfig": fill_color_config,
        "fillColorRgba": fill_color_rgba,
        "lineColorConfig": line_color_config,
        "lineColorRgba": line_color_rgba,
        "lineWidth": vector_layer.get("lineWidthMinPixels") or vector_layer.get("getLineWidth", 1),
        "pointRadius": vector_layer.get("pointRadiusMinPixels") or vector_layer.get("pointRadius", 6),
        "isFilled": is_filled,
        "isStroked": vector_layer.get("stroked", True),
        "opacity": vector_layer.get("opacity", 0.8),
        "tooltipColumns": tooltip_columns,
        "visible": visible,
    }

def _process_mvt_layer(idx: int, tile_url: str, source_layer: str, config: dict, name: str, visible: bool) -> dict:
    """Process a vector tile (MVT) layer definition into FusedMaps format."""
    if not tile_url:
        return None

    # Parse config
    if isinstance(config, str):
        try:
            config = json.loads(config)
        except:
            config = {}
    config = config or {}

    merged_config = _deep_merge_dict(deepcopy(DEFAULT_DECK_CONFIG), config)
    vector_layer = merged_config.get("vectorLayer", {}) or {}

    # Extract fill/line configs from the vectorLayer config (same schema used for GeoJSON vector layer styling)
    fill_color_config = None
    fill_color = None
    fill_opacity = float(vector_layer.get("opacity", 0.8))
    fill_opacity = max(0.0, min(1.0, fill_opacity))

    fill_color_raw = vector_layer.get("getFillColor")
    if isinstance(fill_color_raw, dict) and fill_color_raw.get("@@function"):
        fill_color_config = fill_color_raw
    elif isinstance(fill_color_raw, (list, tuple)) and len(fill_color_raw) >= 3:
        # Convert [r,g,b,(a)] to rgba string
        r, g, b = int(fill_color_raw[0]), int(fill_color_raw[1]), int(fill_color_raw[2])
        a = (fill_color_raw[3] / 255.0) if len(fill_color_raw) > 3 else 0.8
        fill_color = f"rgba({r},{g},{b},{a})"
    elif isinstance(fill_color_raw, str):
        fill_color = fill_color_raw

    line_color_config = None
    line_color = None
    line_color_raw = vector_layer.get("getLineColor")
    if isinstance(line_color_raw, dict) and line_color_raw.get("@@function"):
        line_color_config = line_color_raw
    elif isinstance(line_color_raw, (list, tuple)) and len(line_color_raw) >= 3:
        r, g, b = int(line_color_raw[0]), int(line_color_raw[1]), int(line_color_raw[2])
        a = (line_color_raw[3] / 255.0) if len(line_color_raw) > 3 else 1.0
        line_color = f"rgba({r},{g},{b},{a})"
    elif isinstance(line_color_raw, str):
        line_color = line_color_raw

    line_width = vector_layer.get("lineWidthMinPixels") or vector_layer.get("getLineWidth", 1)
    try:
        line_width = float(line_width)
    except:
        line_width = 1.0

    tooltip_columns = _extract_tooltip_columns(config, vector_layer)

    return {
        "id": f"layer-{idx}",
        "name": name,
        "layerType": "mvt",
        "tileUrl": tile_url,
        "sourceLayer": source_layer or "udf",
        "fillColorConfig": fill_color_config,
        "fillColor": fill_color,
        "fillOpacity": fill_opacity,
        "isFilled": vector_layer.get("filled", True),
        "lineColorConfig": line_color_config,
        "lineColor": line_color,
        "lineWidth": line_width,
        "isExtruded": vector_layer.get("extruded", False),
        "extrusionOpacity": float(vector_layer.get("opacity", 0.9)),
        "heightProperty": vector_layer.get("heightProperty") or "height",
        "heightMultiplier": vector_layer.get("heightMultiplier") or 1,
        "tooltipColumns": tooltip_columns,
        "visible": visible,
    }


def _process_raster_layer(idx: int, tile_url: str, image_url, bounds, config: dict, name: str, visible: bool) -> dict:
    """Process a raster layer definition into FusedMaps format."""
    
    if not tile_url and not image_url:
        return None
    
    config = config or {}
    merged_config = _deep_merge_dict(deepcopy(DEFAULT_DECK_RASTER_CONFIG), config)
    raster_layer = merged_config.get("rasterLayer", {})
    
    opacity = float(raster_layer.get("opacity", 1.0))
    opacity = max(0.0, min(1.0, opacity))
    
    out = {
        "id": f"layer-{idx}",
        "name": name,
        "layerType": "raster",
        "rasterLayer": {"opacity": opacity},
        "opacity": opacity,
        "visible": visible,
    }
    if tile_url:
        out["tileUrl"] = tile_url
    if image_url:
        out["imageUrl"] = image_url
        out["imageBounds"] = list(bounds) if bounds is not None else None
    return out


# ============================================================
# Helper Functions
# ============================================================

def _deep_merge_dict(base: dict, extra: dict) -> dict:
    """Deep merge extra into base."""
    result = deepcopy(base)
    for k, v in extra.items():
        if k in result and isinstance(result[k], dict) and isinstance(v, dict):
            result[k] = _deep_merge_dict(result[k], v)
        else:
            result[k] = deepcopy(v)
    return result


def _to_hex_str(val) -> str:
    """Convert hex ID to canonical string format."""
    if val is None:
        return None
    try:
        if isinstance(val, (int, float, np.integer)):
            return format(int(val), 'x')
        if isinstance(val, str):
            return format(int(val), 'x') if val.isdigit() else val
    except (ValueError, OverflowError):
        pass
    return str(val) if val is not None else None


def _sanitize_value(val):
    """Sanitize a value for JSON serialization."""
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return None
    if isinstance(val, (np.integer, np.int64, np.uint64)):
        return int(val)
    if isinstance(val, np.floating):
        return float(val)
    return val


def _sanitize_records(records: list) -> list:
    """Sanitize all values in a list of records."""
    return [
        {k: _sanitize_value(v) for k, v in row.items()}
        for row in records
    ]


def _extract_tooltip_columns(config: dict, layer_config: dict) -> list:
    """Extract tooltip columns from config."""
    # Check multiple sources
    sources = [
        config.get("tooltipColumns"),
        config.get("tooltipAttrs"),
        layer_config.get("tooltipColumns"),
        layer_config.get("tooltipAttrs"),
    ]
    for src in sources:
        if src:
            return list(src)
    return []


def _compute_center_from_hex(df) -> dict:
    """Compute center from hex data."""
    try:
        import h3
        hex_col = next((c for c in ['hex', 'h3', 'index', 'id'] if c in df.columns), None)
        if hex_col and len(df) > 0:
            sample_hex = str(df[hex_col].iloc[0])
            if sample_hex.isdigit():
                sample_hex = format(int(sample_hex), 'x')
            lat, lng = h3.cell_to_latlng(sample_hex)
            return {"longitude": lng, "latitude": lat}
    except:
        pass
    return None


def _compute_center_from_gdf(gdf) -> dict:
    """Compute center from GeoDataFrame."""
    try:
        if hasattr(gdf, 'geometry') and len(gdf) > 0:
            centroid = gdf.geometry.unary_union.centroid
            return {"longitude": centroid.x, "latitude": centroid.y}
    except:
        pass
    return None


# ============================================================
# Messaging Functions (wrappers - messaging is built into fusedmaps)
# ============================================================

def enable_map_broadcast(html_input, channel: str = "fused-bus", dataset: str = "all"):
    """
    Note: In the refactored version, broadcasting is enabled via config.
    This function is kept for API compatibility but just returns the input.
    """
    # Broadcasting is now handled by passing messaging config to deckgl_layers
    return html_input


def enable_map_sync(html_input, channel: str = "default"):
    """
    Note: In the refactored version, sync is enabled via config.
    This function is kept for API compatibility but just returns the input.
    """
    return html_input

