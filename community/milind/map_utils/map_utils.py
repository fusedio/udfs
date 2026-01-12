
import json
import typing
import numpy as np
import pandas as pd
import geopandas as gpd
from copy import deepcopy

import fused



# ============================================================
# Default Configurations (New Clean Format)
# ============================================================

DEFAULT_HEX_STYLE = {
    "fillColor": {
        "type": "continuous",
        "attr": "cnt",
        "palette": "ArmyRose",
        "steps": 20,
        "nullColor": [184, 184, 184]
    },
    "lineColor": [255, 255, 255],
    "opacity": 1,
    "filled": True,
    "stroked": True,
    "extruded": False,
    "elevationScale": 1,
    "lineWidth": 1
}

DEFAULT_VECTOR_STYLE = {
    "fillColor": {
        "type": "continuous",
        "attr": "house_age",
        "palette": "ArmyRose",
        "domain": [0, 50],
        "steps": 7,
        "nullColor": [200, 200, 200, 180]
    },
    "opacity": 0.8,
    "filled": True,
    "stroked": True,
    "pointRadius": 10,
    "lineWidth": 0
}

DEFAULT_RASTER_STYLE = {
    "opacity": 1.0
}

# Legacy defaults (for backwards compatibility during transition)
DEFAULT_DECK_HEX_CONFIG = {
    "style": DEFAULT_HEX_STYLE
}

DEFAULT_DECK_CONFIG = {
    "style": DEFAULT_VECTOR_STYLE
}

DEFAULT_DECK_RASTER_CONFIG = {
    "style": DEFAULT_RASTER_STYLE
}

# Valid style properties for the new format
VALID_STYLE_PROPS = {
    "fillColor", "lineColor", "opacity", "filled", "stroked",
    "extruded", "elevationAttr", "elevationScale", "lineWidth", "pointRadius"
}

# Valid tile options
VALID_TILE_PROPS = {
    "minZoom", "maxZoom", "zoomOffset", "tileSize", "maxRequests"
}

# ============================================================
# CDN URLs
# ============================================================

# NOTE: Pin to a specific commit for reproducibility.
# You can override this per-run via `deckgl_layers(..., fusedmaps_ref=...)`.
#
# - main ref: 73faec4 (loading spinner for MVT/raster/PMTiles layers)
FUSEDMAPS_CDN_REF_DEFAULT = "72ae434"
FUSEDMAPS_CDN_JS = f"https://cdn.jsdelivr.net/gh/milind-soni/fusedmaps@{FUSEDMAPS_CDN_REF_DEFAULT}/dist/fusedmaps.umd.js"
FUSEDMAPS_CDN_CSS = f"https://cdn.jsdelivr.net/gh/milind-soni/fusedmaps@{FUSEDMAPS_CDN_REF_DEFAULT}/dist/fusedmaps.css"

def _fusedmaps_cdn_urls(ref: typing.Optional[str] = None) -> tuple[str, str]:
    """Return (js_url, css_url) for a given fusedmaps git ref (commit/tag/branch)."""
    ref = (ref or FUSEDMAPS_CDN_REF_DEFAULT).strip()
    js_url = f"https://cdn.jsdelivr.net/gh/milind-soni/fusedmaps@{ref}/dist/fusedmaps.umd.js"
    css_url = f"https://cdn.jsdelivr.net/gh/milind-soni/fusedmaps@{ref}/dist/fusedmaps.css"
    return js_url, css_url

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
  {custom_head}
  <script type="module">
    import * as cartocolor from 'https://esm.sh/cartocolor@5.0.2';
    window.cartocolor = cartocolor;
  </script>
  <link href="{css_url}" rel="stylesheet" />
  <script src="{js_url}"></script>
  <style>
    html, body {{ margin:0; height:100%; width:100%; display:flex; overflow:hidden; }}
    #map {{ flex:1; height:100%; }}
    {custom_css}
  </style>
</head>
<body>
  <div id="map"></div>
  {custom_body}
  <script>
    // Initialize even if cartocolor fails to load (network/csp issues).
    // If cartocolor loads later, palettes will start working for subsequent style updates.
    function waitForCartocolor(callback, tries) {{
      tries = Number.isFinite(tries) ? tries : 0;
      if (window.cartocolor) return callback();
      // ~2s timeout, then proceed with a safe fallback.
      if (tries >= 40) {{
        try {{ window.cartocolor = window.cartocolor || {{}}; }} catch (e) {{}}
        return callback();
      }}
      setTimeout(() => waitForCartocolor(callback, tries + 1), 50);
    }}

    waitForCartocolor(() => {{
      const config = {config_json};
      // IMPORTANT: avoid naming collisions with user-injected `on_init` code.
      // (Users often write: `const instance = window.__fusedMapsInstance;`)
      const __fm_instance = FusedMaps.init(config);
      const __fm_map = __fm_instance.map;
      // Expose globally for custom scripts
      window.__fusedMapsInstance = __fm_instance;
      window.__fusedMapsMap = __fm_map;
      
      // User-provided on_init callback
      {on_init}
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
    sidebar: typing.Optional[str] = None,  # None | "show" | "hide"
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
        sidebar=sidebar,
    )



def deckgl_layers(
    layers: list,
    mapbox_token: str = "pk.eyJ1IjoiaXNhYWNmdXNlZGxhYnMiLCJhIjoiY2xicGdwdHljMHQ1bzN4cWhtNThvbzdqcSJ9.73fb6zHMeO_c8eAXpZVNrA",
    basemap: str = "dark",
    initialViewState: typing.Optional[dict] = None,
    theme: str = "dark",
    highlight_on_click: bool = True,
    on_click: typing.Union[dict, bool, None] = None,  # Click broadcast config, False to disable
    map_broadcast: typing.Optional[dict] = None,  # Viewport broadcast config: {"channel": "fused-bus", "dataset": "all"}
    location_listener: typing.Union[dict, bool, None] = None,  # Listen for feature clicks and fly to bounds: {"channel": "fused-bus"}, False to disable
    sidebar: typing.Optional[str] = None,  # None | "show" | "hide"
    debug: typing.Optional[bool] = None,  # deprecated alias for sidebar
    fusedmaps_ref: typing.Optional[str] = None,  # override CDN ref (commit/tag/branch)
    # --- Widget positioning ---
    widgets: typing.Optional[dict] = None,  # Position/enable widgets: {"controls": "bottom-left", "legend": False, ...}
    # --- AI Configuration ---
    ai_udf_url: typing.Optional[str] = None,  # URL to AI UDF that converts prompts to SQL
    ai_schema: typing.Optional[str] = None,  # Schema string to pass to AI UDF (auto-extracted if not provided)
    ai_context: typing.Optional[str] = None,  # Custom domain context for AI (e.g., CDL crop codes)
    # --- Custom injection for extending without modifying FusedMaps package ---
    custom_head: str = "",  # HTML to inject in <head> (scripts, stylesheets)
    custom_css: str = "",   # CSS rules to inject in <style>
    custom_body: str = "",  # HTML to inject in <body> (custom UI elements)
    on_init: str = "",      # JavaScript to run after FusedMaps.init() - has access to `map` and `instance`
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
        on_click: Click broadcast config (sends feature clicks to other components).
        map_broadcast: Viewport broadcast config (sends map bounds to other components).
            Example: {"channel": "fused-bus", "dataset": "all"}
        
        # Custom injection (extend without modifying FusedMaps package):
        custom_head: HTML to inject in <head> (e.g., external scripts, stylesheets).
        custom_css: CSS rules to inject (without <style> tags).
        custom_body: HTML to inject in <body> (e.g., custom UI containers).
        on_init: JavaScript code to run after FusedMaps.init(). Has access to:
            - `map`: The Mapbox GL map instance
            - `instance`: The FusedMaps instance (with .store, .addLayer, etc.)
            - `config`: The FusedMaps config object
    
    Returns:
        HTML object for rendering in Fused Workbench
        
    Example (adding location search widget):
        geocoder_head = '''
        <script src="https://api.mapbox.com/mapbox-gl-js/plugins/mapbox-gl-geocoder/v5.0.0/mapbox-gl-geocoder.min.js"></script>
        <link rel="stylesheet" href="https://api.mapbox.com/mapbox-gl-js/plugins/mapbox-gl-geocoder/v5.0.0/mapbox-gl-geocoder.css">
        '''
        geocoder_init = '''
        map.addControl(new MapboxGeocoder({
            accessToken: mapboxgl.accessToken,
            mapboxgl: mapboxgl,
            placeholder: 'Search location...'
        }));
        '''
        return deckgl_layers(layers=layers, custom_head=geocoder_head, on_init=geocoder_init)
    """
    
    # Basemap styles
    basemap_styles = {
        "dark": "mapbox://styles/mapbox/dark-v11",
        "satellite": "mapbox://styles/mapbox/satellite-streets-v12",
        "light": "mapbox://styles/mapbox/light-v11",
        "streets": "mapbox://styles/mapbox/streets-v12"
    }
    style_url = basemap_styles.get(basemap.lower(), basemap_styles["dark"])

    # Default widget positions (can be overridden or disabled with False)
    default_widgets = {
        "controls": "bottom-left",   # zoom/home/screenshot
        "scale": "bottom-left",      # scale bar
        "basemap": "bottom-left",    # basemap switcher
        "layers": "top-right",       # layer visibility panel
        "legend": "bottom-right",    # color legend
        "geocoder": False,           # location search (disabled by default)
    }
    # Merge user overrides
    widget_config = {**default_widgets, **(widgets or {})}
    
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
        parquet_url = layer_def.get("parquetUrl") or layer_def.get("parquet_url")
        sql = layer_def.get("sql")
        data_ref = layer_def.get("data_ref") or layer_def.get("dataRef") or layer_def.get("data_var") or layer_def.get("dataVar")
        
        # Validate sources early so "missing data" doesn't silently render nothing.
        if layer_type == "hex":
            if not tile_url and parquet_url is None and df is None:
                raise ValueError(
                    f"Hex layer '{name}' is missing a source. Provide one of: "
                    f"data=<DataFrame with 'hex'>, tile_url=<xyz template>, parquetUrl=<parquet endpoint>."
                )
        elif layer_type in ("vector", "mvt"):
            # vector: either data (GeoDataFrame) or vector tiles
            if layer_type == "vector" and tile_url and not source_layer:
                raise ValueError(f"Vector tile layer '{name}' requires source_layer when tile_url is provided.")
            if not tile_url and df is None:
                raise ValueError(
                    f"Vector layer '{name}' is missing a source. Provide one of: "
                    f"data=<GeoDataFrame>, tile_url=<mvt tiles> (+ source_layer)."
                )
        elif layer_type == "raster":
            if not tile_url and not image_url:
                raise ValueError(
                    f"Raster layer '{name}' is missing a source. Provide one of: "
                    f"tile_url=<xyz tiles> OR image_url=<static image> (+ bounds=[w,s,e,n])."
                )
            if image_url and (bounds is None or len(bounds) != 4):
                raise ValueError(f"Raster layer '{name}' with image_url requires bounds=[west,south,east,north].")
        elif layer_type == "pmtiles":
            pmtiles_url = layer_def.get("pmtiles_url") or layer_def.get("pmtilesUrl")
            pmtiles_path = layer_def.get("pmtiles_path") or layer_def.get("pmtilesPath")
            if not pmtiles_url and not pmtiles_path:
                raise ValueError(
                    f"PMTiles layer '{name}' is missing a source. Provide one of: "
                    f"pmtiles_url=<signed URL> OR pmtiles_path=<s3://...> (will be signed automatically)."
                )
        
        if layer_type == "hex":
            processed = _process_hex_layer(i, df, tile_url, config, name, visible)
            if processed and data_ref:
                processed["dataRef"] = str(data_ref)
            # Support SQL parquet-backed hex layers (non-tile) by passing through parquetUrl/sql.
            if processed and not processed.get("isTileLayer") and parquet_url:
                processed["parquetUrl"] = parquet_url
                if sql is not None:
                    processed["sql"] = sql
            # Tooltip from layer_def takes precedence
            tooltip_from_def = layer_def.get("tooltip") or layer_def.get("tooltipColumns")
            if processed and tooltip_from_def:
                processed["tooltip"] = list(tooltip_from_def)
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
            if processed and data_ref:
                processed["dataRef"] = str(data_ref)
            # Tooltip from layer_def takes precedence
            tooltip_from_def = layer_def.get("tooltip") or layer_def.get("tooltipColumns")
            if processed and tooltip_from_def:
                processed["tooltip"] = list(tooltip_from_def)
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
        
        elif layer_type == "pmtiles":
            pmtiles_url = layer_def.get("pmtiles_url") or layer_def.get("pmtilesUrl")
            pmtiles_path = layer_def.get("pmtiles_path") or layer_def.get("pmtilesPath")
            minzoom = layer_def.get("minzoom") if layer_def.get("minzoom") is not None else layer_def.get("minZoom")
            maxzoom = layer_def.get("maxzoom") if layer_def.get("maxzoom") is not None else layer_def.get("maxZoom")
            
            # Sign S3 path if needed
            if pmtiles_path and not pmtiles_url:
                pmtiles_url = fused.api.sign_url(pmtiles_path)
            
            processed = _process_pmtiles_layer(i, pmtiles_url, pmtiles_path, source_layer, config, name, visible, minzoom=minzoom, maxzoom=maxzoom)
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
    
    # Sidebar behavior:
    # - sidebar=None => don't mount at all (no toggle)
    # - sidebar="show"|"hide" => mount and start open/closed
    # Back-compat:
    # - debug=True => sidebar="show"
    # - debug=False => sidebar=None
    if sidebar is None and debug is True:
        sidebar = "show"
    if sidebar is None and debug is False:
        sidebar = None
    if sidebar not in (None, "show", "hide"):
        raise ValueError("sidebar must be one of: None, 'show', 'hide'")

    # Build config for FusedMaps
    fusedmaps_config = {
        "containerId": "map",
        "mapboxToken": mapbox_token,
        "styleUrl": style_url,
        "initialViewState": view_state,
        "layers": processed_layers,
        "hasCustomView": has_custom_view,
        "ui": {
            "legend": True,
            "layerPanel": True,
            "tooltip": True,
            "theme": theme,
        },
        "widgets": widget_config,
        "highlightOnClick": highlight_on_click,
    }

    if sidebar is not None:
        fusedmaps_config["sidebar"] = sidebar

    # AI UDF URL (backend prompt-to-SQL)
    if ai_udf_url:
        fusedmaps_config["aiUdfUrl"] = ai_udf_url

        # Auto-extract schema from first DuckDB hex layer if not provided
        extracted_schema = ai_schema
        if not extracted_schema:
            for layer in processed_layers:
                parquet_url = layer.get("parquetUrl")
                if layer.get("layerType") == "hex" and parquet_url:
                    try:
                        extracted_schema = extract_schema(parquet_url)
                    except:
                        pass
                    break

        if extracted_schema and not extracted_schema.startswith("Error"):
            fusedmaps_config["aiSchema"] = extracted_schema
        if ai_context:
            fusedmaps_config["aiContext"] = ai_context

    # Add messaging config for broadcast/click events
    messaging_config = {}
    if map_broadcast:
        messaging_config["broadcast"] = {
            "enabled": True,
            "channel": map_broadcast.get("channel", "fused-bus"),
            "dataset": map_broadcast.get("dataset", "all")
        }
    # Click broadcast: enabled by default so forms/charts receive feature clicks
    # Pass on_click=False to disable
    if on_click is not False:
        click_cfg = on_click if isinstance(on_click, dict) else {}
        messaging_config["clickBroadcast"] = {
            "enabled": True,
            "channel": click_cfg.get("channel", "fused-bus"),
            "messageType": click_cfg.get("messageType", "feature_click"),
            "includeCoords": click_cfg.get("includeCoords", True),
            "includeLayer": click_cfg.get("includeLayer", True),
        }
        if click_cfg.get("properties"):
            messaging_config["clickBroadcast"]["properties"] = click_cfg["properties"]
    # Location listener: enabled by default so scatter/chart clicks fly map to bounds
    # Pass location_listener=False to disable
    if location_listener is not False:
        loc_cfg = location_listener if isinstance(location_listener, dict) else {}
        messaging_config["locationListener"] = {
            "enabled": True,
            "channel": loc_cfg.get("channel", "fused-bus"),
            "zoomOffset": loc_cfg.get("zoomOffset", 0),
            "padding": loc_cfg.get("padding", 50),
            "maxZoom": loc_cfg.get("maxZoom", 18),
        }
    if messaging_config:
        fusedmaps_config["messaging"] = messaging_config
    
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
    js_url, css_url = _fusedmaps_cdn_urls(fusedmaps_ref)

    html = MINIMAL_TEMPLATE.format(
        css_url=css_url,
        js_url=js_url,
        config_json=json.dumps(fusedmaps_config),
        extra_scripts=extra_scripts,
        custom_head=custom_head or "",
        custom_css=custom_css or "",
        custom_body=custom_body or "",
        on_init=on_init or "",
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
    map_broadcast: typing.Optional[dict] = None,
    sidebar: typing.Optional[str] = None,
    debug: typing.Optional[bool] = None,  # deprecated alias
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
        map_broadcast=map_broadcast,
        sidebar=sidebar,
        debug=debug,
    )


def deckgl_map(
    gdf,
    config: typing.Union[dict, str, None] = None,
    mapbox_token: str = "pk.eyJ1IjoiaXNhYWNmdXNlZGxhYnMiLCJhIjoiY2xicGdwdHljMHQ1bzN4cWhtNThvbzdqcSJ9.73fb6zHMeO_c8eAXpZVNrA",
    basemap: str = "dark",
    sidebar: typing.Optional[str] = None,
    debug: typing.Optional[bool] = None,  # deprecated alias
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
        sidebar=sidebar,
        debug=debug,
    )


def deckgl_pmtiles(
    pmtiles_path: str = None,
    pmtiles_url: str = None,
    source_layer: str = None,
    config: dict = None,
    mapbox_token: str = "pk.eyJ1IjoiaXNhYWNmdXNlZGxhYnMiLCJhIjoiY2xicGdwdHljMHQ1bzN4cWhtNThvbzdqcSJ9.73fb6zHMeO_c8eAXpZVNrA",
    basemap: str = "dark",
    sidebar: typing.Optional[str] = None,
    debug: typing.Optional[bool] = None,  # deprecated alias
):
    """
    Render PMTiles vector data on an interactive map.
    
    Args:
        pmtiles_path: S3 path to PMTiles file (will be signed automatically)
        pmtiles_url: Pre-signed URL to PMTiles file
        source_layer: Name of the source layer in the PMTiles (auto-detected if not provided)
        config: Layer styling config (same as vectorLayer config)
        mapbox_token: Mapbox access token
        basemap: 'dark', 'satellite', 'light', or 'streets'
        sidebar: 'show', 'hide', or None (no sidebar)
    
    Example:
        return deckgl_pmtiles(
            pmtiles_path="s3://my-bucket/data.pmtiles",
            config={
                "vectorLayer": {
                    "getFillColor": {
                        "attr": "value",
                        "domain": [0, 100],
                        "colors": "Viridis"
                    }
                }
            }
        )
    """
    if not pmtiles_path and not pmtiles_url:
        raise ValueError("Provide either pmtiles_path (s3://...) or pmtiles_url (signed URL)")
    
    layers = [{
        "type": "pmtiles",
        "pmtiles_path": pmtiles_path,
        "pmtiles_url": pmtiles_url,
        "source_layer": source_layer,
        "config": config,
        "name": "PMTiles Layer",
    }]
    
    return deckgl_layers(
        layers=layers,
        mapbox_token=mapbox_token,
        basemap=basemap,
        sidebar=sidebar,
        debug=debug,
    )


def deckgl_raster(
    image_data=None,
    bounds=None,
    tile_url: str = None,
    config: dict = None,
    mapbox_token: str = "pk.eyJ1IjoiaXNhYWNmdXNlZGxhYnMiLCJhIjoiY2xicGdwdHljMHQ1bzN4cWhtNThvbzdqcSJ9.73fb6zHMeO_c8eAXpZVNrA",
    basemap: str = "dark",
    sidebar: typing.Optional[str] = None,
    debug: typing.Optional[bool] = None,  # deprecated alias
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
            sidebar=sidebar,
            debug=debug,
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
        sidebar=sidebar,
        debug=debug,
    )


# ============================================================
# Layer Processing Functions
# ============================================================

def _process_hex_layer(idx: int, df, tile_url: str, config: dict, name: str, visible: bool) -> dict:
    """Process a hex layer definition into FusedMaps new format."""

    # Parse config
    if isinstance(config, str):
        try:
            config = json.loads(config)
        except:
            config = {}
    config = config or {}

    # Support both new format (style key) and legacy format (hexLayer key)
    style = config.get("style") or {}
    tile_opts = config.get("tile") or {}

    # Legacy support: extract from hexLayer if present
    if "hexLayer" in config:
        legacy = config["hexLayer"]
        style = _deep_merge_dict(_convert_legacy_style(legacy), style)

    # Merge with defaults
    style = _deep_merge_dict(deepcopy(DEFAULT_HEX_STYLE), style)

    is_tile_layer = tile_url is not None

    # Extract tile options from legacy tileLayerConfig if present
    if is_tile_layer:
        legacy_tile = config.get("tileLayerConfig") or config.get("tileLayer") or {}
        tile_opts = _deep_merge_dict(legacy_tile, tile_opts)

    # Process data
    data_records = []
    if not is_tile_layer and df is not None and hasattr(df, 'to_dict'):
        df_clean = df.drop(columns=['geometry'], errors='ignore') if hasattr(df, 'drop') else df
        df_clean = df_clean.copy()

        hex_col = next((c for c in ['hex', 'h3', 'index', 'id'] if c in df_clean.columns), None)
        if hex_col:
            df_clean['hex'] = df_clean[hex_col].apply(_to_hex_str)

        data_records = _sanitize_records(df_clean.to_dict('records'))

    # Extract tooltip columns (check legacy hexLayer too)
    legacy_hex = config.get("hexLayer") or {}
    tooltip = _extract_tooltip_columns_new(config, style)
    if not tooltip:
        tooltip = legacy_hex.get("tooltipAttrs") or legacy_hex.get("tooltipColumns") or []

    result = {
        "id": f"layer-{idx}",
        "name": name,
        "layerType": "hex",
        "visible": visible,
    }

    if data_records:
        result["data"] = data_records
    if tile_url:
        result["tileUrl"] = tile_url
        result["isTileLayer"] = True
    if style:
        result["style"] = style
    if tile_opts:
        result["tile"] = tile_opts
    if tooltip:
        result["tooltip"] = tooltip

    return result


def _process_vector_layer(idx: int, df, config: dict, name: str, visible: bool) -> dict:
    """Process a vector layer definition into FusedMaps new format."""

    if df is None:
        return None

    # Parse config
    if isinstance(config, str):
        try:
            config = json.loads(config)
        except:
            config = {}
    config = config or {}

    # Support both new format (style key) and legacy format (vectorLayer key)
    style = config.get("style") or {}

    # Legacy support: extract from vectorLayer if present
    if "vectorLayer" in config:
        legacy = config["vectorLayer"]
        style = _deep_merge_dict(_convert_legacy_style(legacy), style)

    # Merge with defaults
    style = _deep_merge_dict(deepcopy(DEFAULT_VECTOR_STYLE), style)

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

    # Extract tooltip columns (check legacy vectorLayer too)
    legacy_vec = config.get("vectorLayer") or {}
    tooltip = _extract_tooltip_columns_new(config, style)
    if not tooltip:
        tooltip = legacy_vec.get("tooltipColumns") or legacy_vec.get("tooltipAttrs") or []

    result = {
        "id": f"layer-{idx}",
        "name": name,
        "layerType": "vector",
        "visible": visible,
        "geojson": geojson_obj,
    }

    if style:
        result["style"] = style
    if tooltip:
        result["tooltip"] = tooltip

    return result

def _process_mvt_layer(idx: int, tile_url: str, source_layer: str, config: dict, name: str, visible: bool) -> dict:
    """Process a vector tile (MVT) layer definition into FusedMaps new format."""
    if not tile_url:
        return None

    # Parse config
    if isinstance(config, str):
        try:
            config = json.loads(config)
        except:
            config = {}
    config = config or {}

    # Support both new format (style key) and legacy format (vectorLayer key)
    style = config.get("style") or {}
    tile_opts = config.get("tile") or {}

    # Legacy support
    if "vectorLayer" in config:
        legacy = config["vectorLayer"]
        style = _deep_merge_dict(_convert_legacy_style(legacy), style)

    # Merge with defaults
    style = _deep_merge_dict(deepcopy(DEFAULT_VECTOR_STYLE), style)

    tooltip = _extract_tooltip_columns_new(config, style)

    result = {
        "id": f"layer-{idx}",
        "name": name,
        "layerType": "mvt",
        "visible": visible,
        "tileUrl": tile_url,
        "sourceLayer": source_layer or "udf",
    }

    if style:
        result["style"] = style
    if tile_opts:
        result["tile"] = tile_opts
    if tooltip:
        result["tooltip"] = tooltip

    return result


def _process_raster_layer(idx: int, tile_url: str, image_url, bounds, config: dict, name: str, visible: bool) -> dict:
    """Process a raster layer definition into FusedMaps new format."""

    if not tile_url and not image_url:
        return None

    config = config or {}

    # Extract opacity from config
    opacity = 1.0
    if "style" in config:
        opacity = float(config["style"].get("opacity", 1.0))
    elif "rasterLayer" in config:
        opacity = float(config["rasterLayer"].get("opacity", 1.0))
    elif "opacity" in config:
        opacity = float(config["opacity"])

    opacity = max(0.0, min(1.0, opacity))

    result = {
        "id": f"layer-{idx}",
        "name": name,
        "layerType": "raster",
        "visible": visible,
        "opacity": opacity,
    }

    if tile_url:
        result["tileUrl"] = tile_url
    if image_url:
        result["imageUrl"] = image_url
        result["imageBounds"] = list(bounds) if bounds is not None else None

    return result


def _process_pmtiles_layer(
    idx: int,
    pmtiles_url: str,
    pmtiles_path: str,
    source_layer: str,
    config: dict,
    name: str,
    visible: bool,
    minzoom: int = None,
    maxzoom: int = None,
) -> dict:
    """Process a PMTiles layer definition into FusedMaps new format."""
    if not pmtiles_url:
        return None

    # Parse config
    if isinstance(config, str):
        try:
            config = json.loads(config)
        except:
            config = {}
    config = config or {}

    # Support both new format (style key) and legacy format (vectorLayer key)
    style = config.get("style") or {}
    tile_opts = config.get("tile") or {}

    # Legacy support
    if "vectorLayer" in config:
        legacy = config["vectorLayer"]
        style = _deep_merge_dict(_convert_legacy_style(legacy), style)

    # Merge with defaults
    style = _deep_merge_dict(deepcopy(DEFAULT_VECTOR_STYLE), style)

    # Extract exclude_source_layers from config
    exclude_source_layers = (
        config.get("exclude_source_layers") or
        config.get("excludeSourceLayers") or
        []
    )
    if isinstance(exclude_source_layers, str):
        exclude_source_layers = [exclude_source_layers]
    exclude_source_layers = [str(x) for x in exclude_source_layers if str(x).strip()]

    # Extract render toggles
    render_points = config.get("renderPoints") or config.get("render_points")
    render_lines = config.get("renderLines") or config.get("render_lines")
    render_polygons = config.get("renderPolygons") or config.get("render_polygons")

    # Build tile options
    if minzoom is not None:
        tile_opts["minZoom"] = int(minzoom)
    if maxzoom is not None:
        tile_opts["maxZoom"] = int(maxzoom)

    tooltip = _extract_tooltip_columns_new(config, style)

    result = {
        "id": f"layer-{idx}",
        "name": name,
        "layerType": "pmtiles",
        "visible": visible,
        "pmtilesUrl": pmtiles_url,
    }

    if pmtiles_path:
        result["pmtilesPath"] = pmtiles_path
    if source_layer:
        result["sourceLayer"] = source_layer
    if exclude_source_layers:
        result["excludeSourceLayers"] = exclude_source_layers
    if style:
        result["style"] = style
    if tile_opts:
        result["tile"] = tile_opts
    if tooltip:
        result["tooltip"] = tooltip
    if render_points is not None:
        result["renderPoints"] = render_points
    if render_lines is not None:
        result["renderLines"] = render_lines
    if render_polygons is not None:
        result["renderPolygons"] = render_polygons

    return result


# ============================================================
# Helper Functions
# ============================================================

def _convert_legacy_color(color_config) -> typing.Optional[dict]:
    """Convert legacy @@function color config to new format."""
    if not isinstance(color_config, dict):
        return color_config  # Pass through RGB arrays, strings

    fn = color_config.get("@@function")
    if fn == "colorContinuous":
        return {
            "type": "continuous",
            "attr": color_config.get("attr"),
            "palette": color_config.get("colors"),
            "domain": color_config.get("domain"),
            "steps": color_config.get("steps"),
            "nullColor": color_config.get("nullColor"),
            "reverse": color_config.get("reverse"),
            "autoDomain": color_config.get("autoDomain"),
        }
    elif fn == "colorCategories":
        return {
            "type": "categorical",
            "attr": color_config.get("attr"),
            "palette": color_config.get("colors"),
            "categories": color_config.get("categories"),
            "labelAttr": color_config.get("labelAttr"),
            "nullColor": color_config.get("nullColor"),
        }
    elif color_config.get("attr"):
        # Shorthand: {"attr": "value", "domain": [...], "colors": "..."}
        return {
            "type": "continuous",
            "attr": color_config.get("attr"),
            "palette": color_config.get("colors"),
            "domain": color_config.get("domain"),
            "steps": color_config.get("steps"),
            "nullColor": color_config.get("nullColor"),
        }

    return color_config


def _convert_legacy_style(legacy: dict) -> dict:
    """Convert legacy hexLayer/vectorLayer format to new style format."""
    style = {}

    # Color mappings
    if "getFillColor" in legacy:
        style["fillColor"] = _convert_legacy_color(legacy["getFillColor"])
    if "getLineColor" in legacy:
        style["lineColor"] = _convert_legacy_color(legacy["getLineColor"])

    # Direct property mappings
    if "opacity" in legacy:
        style["opacity"] = legacy["opacity"]
    if "filled" in legacy:
        style["filled"] = legacy["filled"]
    if "stroked" in legacy:
        style["stroked"] = legacy["stroked"]
    if "extruded" in legacy:
        style["extruded"] = legacy["extruded"]

    # Elevation
    if "elevationProperty" in legacy:
        style["elevationAttr"] = legacy["elevationProperty"]
    if "elevationScale" in legacy:
        style["elevationScale"] = legacy["elevationScale"]

    # Line width
    if "lineWidthMinPixels" in legacy:
        style["lineWidth"] = legacy["lineWidthMinPixels"]
    elif "getLineWidth" in legacy:
        style["lineWidth"] = legacy["getLineWidth"]

    # Point radius
    if "pointRadiusMinPixels" in legacy:
        style["pointRadius"] = legacy["pointRadiusMinPixels"]
    elif "pointRadius" in legacy:
        style["pointRadius"] = legacy["pointRadius"]

    return style


def _extract_tooltip_columns_new(config: dict, style: dict) -> list:
    """Extract tooltip columns from config (new format)."""
    sources = [
        config.get("tooltip"),
        config.get("tooltipColumns"),
        config.get("tooltipAttrs"),
        style.get("tooltipColumns") if style else None,
        style.get("tooltipAttrs") if style else None,
    ]
    for src in sources:
        if src:
            return list(src)
    return []


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


def extract_schema(parquet_url: str, table_name: str = "data") -> str:
    """
    Extract schema from a parquet file and return a formatted string for AI prompts.

    Args:
        parquet_url: URL to the parquet file
        table_name: Name to use for the table in the schema description

    Returns:
        Formatted schema string for use in AI system prompts

    Example:
        >>> schema = extract_schema("https://example.com/data.parquet")
        >>> print(schema)
        Table: `data`
        Columns:
        - hex (VARCHAR): H3 hexagon ID - REQUIRED in all queries
        - value (DOUBLE)
        - category (VARCHAR)
    """
    import duckdb
    
    try:
        con = duckdb.connect()
        con.execute("INSTALL httpfs; LOAD httpfs;")
        
        # Explicitly use read_parquet instead of letting DuckDB infer the format
        schema_df = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{parquet_url}') LIMIT 1").df()
        
        lines = [f"Table: `{table_name}`", "Columns:"]
        for _, row in schema_df.iterrows():
            col_name = row['column_name']
            col_type = row['column_type']
            if col_name.lower() in ('hex', 'h3', 'h3_cell', 'h3_index'):
                lines.append(f"- {col_name} ({col_type}): H3 hexagon ID - REQUIRED in all SELECT queries")
            else:
                lines.append(f"- {col_name} ({col_type})")
        return "\n".join(lines)
    except Exception as e:
        raise ValueError(f"Could not extract schema from {parquet_url}: {e}")

def build_sql_system_prompt(
    schema: str,
    table_name: str = "data",
    custom_context: str = "",
) -> str:
    """
    Build a system prompt for SQL generation with the given schema.

    Args:
        schema: Schema string (from extract_schema())
        table_name: Table name used in queries
        custom_context: Additional domain-specific context (e.g., CDL crop codes)

    Returns:
        Complete system prompt for AI SQL generation
    """
    return f"""You are a DuckDB SQL query generator for geospatial hex data.

Convert natural language requests into valid DuckDB SELECT statements.

## Schema
{schema}

{f"## Domain Context{chr(10)}{custom_context}" if custom_context else ""}

## CRITICAL RULES
1. ALWAYS include the hex/h3 column in SELECT (required for map rendering)
2. Use `{table_name}` as the table name
3. Return ONLY the SQL query - no explanations, no markdown, no code blocks
4. For filtering, use WHERE clauses
5. Keep queries focused on filtering/aggregating the spatial data

## Example Queries

Filter by value:
SELECT * FROM {table_name} WHERE value > 50

Top N results:
SELECT * FROM {table_name} ORDER BY area DESC LIMIT 100

Multiple conditions:
SELECT * FROM {table_name} WHERE category = 'A' AND value > 10

Aggregation (keep hex column):
SELECT hex, SUM(area) as total_area FROM {table_name} GROUP BY hex

Return ONLY the SQL query."""


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


def enable_location_listener(html_input, zoom_offset: int = 0, padding: int = 40, max_zoom: int = 16):
    """
    Note: In the refactored version, location listening is handled internally.
    This function is kept for API compatibility but just returns the input.
    """
    return html_input


def enable_hex_click_broadcast(html_input, channel: str = "fused-bus"):
    """
    Note: In the refactored version, click broadcasting is handled via config.
    This function is kept for API compatibility but just returns the input.
    """
    return html_input

