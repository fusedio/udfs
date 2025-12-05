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
    
    return deckgl_map(gdf, config)



def deckgl_map(
    gdf,
    config: typing.Union[dict, str, None] = None,
    mapbox_token: str = "pk.eyJ1IjoiaXNhYWNmdXNlZGxhYnMiLCJhIjoiY2xicGdwdHljMHQ1bzN4cWhtNThvbzdqcSJ9.73fb6zHMeO_c8eAXpZVNrA",
    basemap: str = "dark",
):
    """
    Render a GeoDataFrame as an interactive Mapbox GL map with native tooltips.
    
    Args:
        gdf: GeoDataFrame to visualize (points, lines, or polygons).
        config: Deck.GL style overrides (dict or JSON string).
        mapbox_token: Mapbox access token.
        basemap: 'dark', 'satellite', or custom Mapbox style URL.
    """
    import math
    import numpy as _np
    
    # Basemap setup
    basemap_styles = {"dark": "mapbox://styles/mapbox/dark-v11", "satellite": "mapbox://styles/mapbox/satellite-streets-v12", "light": "mapbox://styles/mapbox/light-v11", "streets": "mapbox://styles/mapbox/streets-v12"}
    basemap_key = (basemap or "dark").lower()
    style_url = basemap_styles.get({"sat": "satellite", "satellite-streets": "satellite"}.get(basemap_key, basemap_key), basemap or basemap_styles["dark"])

    # Reproject to EPSG:4326 if needed
    if hasattr(gdf, "crs") and gdf.crs and getattr(gdf.crs, "to_epsg", lambda: None)() != 4326:
        try:
                gdf = gdf.to_crs(epsg=4326)
        except Exception:
            pass

    # Convert to GeoJSON
    try:
        geojson_obj = json.loads(gdf.to_json())
    except Exception:
        geojson_obj = {"type": "FeatureCollection", "features": []}

    # Sanitize properties using shared utility
    for feat in geojson_obj.get("features", []):
        feat["properties"] = {k: _sanitize_geojson_value(v) for k, v in (feat.get("properties") or {}).items()}

    # Auto-center
    auto_center = (0.0, 0.0)
    if hasattr(gdf, "total_bounds"):
        try:
            minx, miny, maxx, maxy = gdf.total_bounds
            auto_center = ((minx + maxx) / 2.0, (miny + maxy) / 2.0)
        except Exception:
            pass

    # Config processing
    merged_config = _load_deckgl_config(config, DEFAULT_DECK_CONFIG)
    initial_view_state = _get_view_state(merged_config, DEFAULT_DECK_CONFIG["initialViewState"])
    vector_layer = _get_layer_config(merged_config, "vectorLayer", DEFAULT_DECK_CONFIG["vectorLayer"])

    # Helper to convert [r,g,b,a] to rgba string
    def to_rgba(color_value, default_alpha=1.0):
        if isinstance(color_value, (list, tuple)) and len(color_value) >= 3:
            r, g, b = int(color_value[0]), int(color_value[1]), int(color_value[2])
            a = color_value[3] / 255.0 if len(color_value) > 3 else default_alpha
            return f"rgba({r},{g},{b},{a})"
        return None
    
    # Extract color config
    fill_color_raw = vector_layer.get("getFillColor")
    fill_color_config, fill_color_rgba, color_attr = {}, None, None
    if isinstance(fill_color_raw, dict) and fill_color_raw.get("@@function") in ("colorContinuous", "colorCategories"):
        fill_color_config, color_attr = fill_color_raw, fill_color_raw.get("attr")
    elif isinstance(fill_color_raw, (list, tuple)):
        fill_color_rgba = to_rgba(fill_color_raw, default_alpha=0.6)
    
    line_color_rgba = to_rgba(vector_layer.get("getLineColor"), default_alpha=1.0)
    
    # Line width validation
    line_width = vector_layer.get("lineWidthMinPixels") or vector_layer.get("getLineWidth", 1)
    if not isinstance(line_width, (int, float)) or not math.isfinite(line_width) or line_width < 0:
        line_width = 1

    point_radius = vector_layer.get("pointRadiusMinPixels") or vector_layer.get("pointRadius", 6)
    is_filled = vector_layer.get("filled", True)
    is_stroked = vector_layer.get("stroked", True)
    opacity = vector_layer.get("opacity", 0.8)

    auto_state = {"longitude": float(auto_center[0]), "latitude": float(auto_center[1]), "zoom": initial_view_state.get("zoom", 11)}
    data_columns = [col for col in gdf.columns if col != "geometry"]
    tooltip_columns = _extract_tooltip_columns((merged_config, vector_layer), data_columns)

    html = Template(r"""
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="initial-scale=1, maximum-scale=1, user-scalable=no" />
  <link href="https://api.mapbox.com/mapbox-gl-js/v3.2.0/mapbox-gl.css" rel="stylesheet" />
  <script src="https://api.mapbox.com/mapbox-gl-js/v3.2.0/mapbox-gl.js"></script>
  <script type="module">
    import * as cartocolor from 'https://esm.sh/cartocolor@5.0.2';
    window.cartocolor = cartocolor;
  </script>
  <style>
    html, body, #map { margin:0; height:100%; width:100%; background:#000; }
    .mapboxgl-popup-content { font-family: Inter,'SF Pro Display','Segoe UI',sans-serif; font-size: 12px; background: rgba(15,15,15,0.95); color:#fff; padding:10px 14px; border-radius:6px; border:1px solid rgba(255,255,255,0.1); box-shadow:0 4px 16px rgba(0,0,0,0.4); max-width:320px; }
    .mapboxgl-popup-content .tt-row { display:flex; justify-content:space-between; padding:3px 0; gap:12px; }
    .mapboxgl-popup-content .tt-key { color:rgba(255,255,255,0.6); font-size:11px; }
    .mapboxgl-popup-content .tt-val { color:#fff; font-weight:500; text-align:right; max-width:180px; word-break:break-word; }
    .color-legend { position:fixed; left:12px; bottom:12px; background:rgba(15,15,15,0.9); color:#fff; padding:8px; border-radius:4px; font-size:11px; display:none; z-index:30; min-width:140px; border:1px solid rgba(255,255,255,0.08); }
    .color-legend .legend-gradient { height:12px; border-radius:3px; border:1px solid rgba(255,255,255,0.06); margin-bottom:6px; }
    .color-legend .legend-labels { display:flex; justify-content:space-between; color:#ccc; font-size:10px;}
    .config-error { position:fixed; right:12px; bottom:12px; background:rgba(180,30,30,0.92); color:#fff; padding:8px 12px; border-radius:4px; font-size:11px; display:none; z-index:30; max-width:300px; }
  </style>
</head>
<body>
<div id="map"></div>
<div id="color-legend" class="color-legend"><div class="legend-title"></div><div class="legend-gradient"></div><div class="legend-labels"><span class="legend-min"></span><span class="legend-max"></span></div></div>
<div id="config-error" class="config-error"></div>

<script>
const MAPBOX_TOKEN = {{ mapbox_token | tojson }};
const GEOJSON = {{ geojson_obj | tojson }};
const AUTO_STATE = {{ auto_state | tojson }};
const FILL_COLOR_CONFIG = {{ fill_color_config | tojson }};
const COLOR_ATTR = {{ color_attr | tojson }};
const TOOLTIP_COLUMNS = {{ tooltip_columns | tojson }};
const LINE_WIDTH = {{ line_width | tojson }};
const POINT_RADIUS = {{ point_radius | tojson }};
const LINE_COLOR = {{ line_color_rgba | tojson }};
const FILL_COLOR = {{ fill_color_rgba | tojson }};
const IS_FILLED = {{ is_filled | tojson }};
const IS_STROKED = {{ is_stroked | tojson }};
const OPACITY = {{ opacity | tojson }};

// Build polygon outlines as LineStrings for stroke-only rendering
function buildOutlines(fc) {
  if (!fc || !Array.isArray(fc.features)) return { type: 'FeatureCollection', features: [] };
  const out = [];
  for (const f of fc.features) {
    if (!f?.geometry?.coordinates) continue;
    const g = f.geometry, c = g.coordinates;
    const rings = g.type === 'Polygon' ? [c] : g.type === 'MultiPolygon' ? c : [];
    for (const poly of rings) {
      for (const ring of poly) {
        if (Array.isArray(ring) && ring.length >= 4)
          out.push({ type: 'Feature', properties: {...f.properties}, geometry: { type: 'LineString', coordinates: ring }});
      }
    }
  }
  return { type: 'FeatureCollection', features: out };
}

const OUTLINES = buildOutlines(GEOJSON);

mapboxgl.accessToken = MAPBOX_TOKEN;
const map = new mapboxgl.Map({
  container: 'map', style: {{ style_url | tojson }},
  center: [AUTO_STATE.longitude, AUTO_STATE.latitude], zoom: AUTO_STATE.zoom
});

function makeColorStops(cfg) {
  if (!cfg || cfg['@@function'] !== 'colorContinuous' || !cfg.domain?.length) return null;
  const [d0, d1] = cfg.domain, rev = d0 > d1, dom = rev ? [d1, d0] : [d0, d1];
  const steps = cfg.steps || 7, name = cfg.colors || 'TealGrn';
  const pal = window.cartocolor?.[name];
  let cols, paletteError = null;
  if (pal) {
    const keys = Object.keys(pal).map(Number).filter(n => !isNaN(n)).sort((a, b) => a - b);
    const bestKey = keys.find(n => n >= steps) || keys[keys.length - 1];
    cols = pal[bestKey] ? [...pal[bestKey]] : null;
  } else if (name && window.cartocolor) {
    paletteError = `Palette "${name}" not found. Valid palettes: ${Object.keys(window.cartocolor).slice(0,10).join(', ')}...`;
  }
  if (!cols || !cols.length) {
    cols = Array.from({length: steps}, (_, i) => {
      const t = i / (steps - 1);
      return `rgb(${Math.round(255 * (1 - t))}, ${Math.round(100 + 155 * t)}, ${Math.round(100 * t)})`;
    });
  }
  if (rev) cols.reverse();
  return { stops: cols.map((c, i) => [dom[0] + (dom[1] - dom[0]) * i / (cols.length - 1), c]), domain: dom, error: paletteError };
}

function makeExpr(attr, spec) {
  if (!spec || !attr) return null;
  const e = ['interpolate', ['linear'], ['get', attr]];
  spec.stops.forEach(s => e.push(s[0], s[1]));
  return e;
}

function addLayers() {
  ['gdf-fill','gdf-line','gdf-line-only','gdf-circle'].forEach(id => { try { if(map.getLayer(id)) map.removeLayer(id); } catch(e){} });
  
  if (!map.getSource('gdf-source')) map.addSource('gdf-source', { type: 'geojson', data: GEOJSON });
  else map.getSource('gdf-source').setData(GEOJSON);
  
  if (OUTLINES.features.length && !map.getSource('gdf-outline-source')) 
    map.addSource('gdf-outline-source', { type: 'geojson', data: OUTLINES });
  else if (map.getSource('gdf-outline-source')) 
    map.getSource('gdf-outline-source').setData(OUTLINES);

  let hasPoly=false, hasPoint=false, hasLine=false;
  for (const f of GEOJSON.features||[]) {
    const t = f.geometry?.type;
    if (t==='Point'||t==='MultiPoint') hasPoint=true;
    if (t==='Polygon'||t==='MultiPolygon') hasPoly=true;
    if (t==='LineString'||t==='MultiLineString') hasLine=true;
  }

  const spec = makeColorStops(FILL_COLOR_CONFIG), expr = makeExpr(COLOR_ATTR, spec);
  const fill = expr || FILL_COLOR || 'rgba(0,144,255,0.6)';
  const lineCol = LINE_COLOR || 'rgba(0,0,0,0.5)', lineW = (typeof LINE_WIDTH==='number' && isFinite(LINE_WIDTH)) ? LINE_WIDTH : 1;

  if (hasPoly) {
    if (IS_FILLED !== false) map.addLayer({ id:'gdf-fill', type:'fill', source:'gdf-source', paint:{'fill-color':fill,'fill-opacity':OPACITY}, filter:['any',['==',['geometry-type'],'Polygon'],['==',['geometry-type'],'MultiPolygon']] });
    if (IS_STROKED !== false && OUTLINES.features.length) map.addLayer({ id:'gdf-line', type:'line', source:'gdf-outline-source', layout:{'line-join':'round','line-cap':'round'}, paint:{'line-color':lineCol,'line-width':lineW}, filter:['any',['==',['geometry-type'],'LineString'],['==',['geometry-type'],'MultiLineString']] });
  }
  if (hasLine) map.addLayer({ id:'gdf-line-only', type:'line', source:'gdf-source', layout:{'line-join':'round','line-cap':'round'}, paint:{'line-color':LINE_COLOR||expr||'rgba(255,0,100,0.9)','line-width':LINE_WIDTH||3,'line-opacity':1}, filter:['any',['==',['geometry-type'],'LineString'],['==',['geometry-type'],'MultiLineString']] });
  if (hasPoint) map.addLayer({ id:'gdf-circle', type:'circle', source:'gdf-source', paint:{'circle-radius':POINT_RADIUS||6,'circle-color':fill,'circle-stroke-color':'rgba(0,0,0,0.5)','circle-stroke-width':(POINT_RADIUS||6)<2?0:1,'circle-opacity':0.9}, filter:['any',['==',['geometry-type'],'Point'],['==',['geometry-type'],'MultiPoint']] });
}

let loaded = false;
function init() {
  if (loaded) return;
  if (FILL_COLOR_CONFIG?.['@@function']==='colorContinuous' && !window.cartocolor) { setTimeout(init, 50); return; }
  addLayers(); loaded = true;
  try { const b = turf?.bbox(GEOJSON); if(b?.length===4) map.fitBounds([[b[0],b[1]],[b[2],b[3]]], {padding:40,maxZoom:15,duration:500}); } catch(e){}
  const spec = makeColorStops(FILL_COLOR_CONFIG);
  if (spec && COLOR_ATTR) {
    const leg = document.getElementById('color-legend');
    leg.querySelector('.legend-title').textContent = COLOR_ATTR;
    leg.querySelector('.legend-gradient').style.background = `linear-gradient(to right, ${spec.stops.map((s,i)=>`${s[1]} ${Math.round(i/(spec.stops.length-1)*100)}%`).join(', ')})`;
    leg.querySelector('.legend-min').textContent = spec.domain[0].toFixed(1);
    leg.querySelector('.legend-max').textContent = spec.domain[1].toFixed(1);
    leg.style.display = 'block';
    // Show palette error if any
    if (spec.error) {
      const errBox = document.getElementById('config-error');
      errBox.textContent = spec.error;
      errBox.style.display = 'block';
    }
  }
}
map.on('load', init);

// Tooltip
let popup = null, lastKey = null;
function fmt(v) { return v==null?'':typeof v==='number'?(!isFinite(v)?'':v.toFixed(2)):Array.isArray(v)?v.join(', '):String(v); }
function tip(p) { const k = TOOLTIP_COLUMNS.length ? TOOLTIP_COLUMNS : Object.keys(p||{}); return k.map(k=>p?.[k]!=null&&String(p[k])!=='null'?`<span class="tt-row"><span class="tt-key">${k}</span><span class="tt-val">${fmt(p[k])}</span></span>`:'').filter(Boolean).join(''); }
function getLayers() { return ['gdf-circle','gdf-fill','gdf-line','gdf-line-only'].filter(id=>{ try{return map.getLayer(id);}catch(e){return false;} }); }

map.on('mousemove', e => {
  const layers = getLayers(); if (!layers.length) return;
  const feats = map.queryRenderedFeatures(e.point, { layers });
  if (feats?.length) {
    const f = feats[0], key = f.id ?? JSON.stringify(f.properties||{});
    if (key === lastKey && popup) { popup.setLngLat(e.lngLat); map.getCanvas().style.cursor='pointer'; return; }
    lastKey = key;
    const html = tip(f.properties||{});
    if (!html) { if(popup){popup.remove();popup=null;lastKey=null;} map.getCanvas().style.cursor=''; return; }
    if (!popup) popup = new mapboxgl.Popup({closeButton:false,closeOnClick:false,offset:10});
    popup.setLngLat(e.lngLat).setHTML(html).addTo(map);
    map.getCanvas().style.cursor='pointer';
  } else { if(popup){popup.remove();popup=null;} lastKey=null; map.getCanvas().style.cursor=''; }
});
map.on('mouseleave', () => { if(popup){popup.remove();popup=null;} lastKey=null; map.getCanvas().style.cursor=''; });

// Fix for tiles not loading in iframes - aggressive tile refresh
map.on('load', () => {
  [100, 300, 600, 1000, 2000].forEach(t => setTimeout(() => { map.resize(); map.triggerRepaint(); }, t));
});
map.on('idle', () => { 
  // Force repaint when map becomes idle to catch missing tiles
  setTimeout(() => map.triggerRepaint(), 50);
});
window.addEventListener('resize', () => { map.resize(); map.triggerRepaint(); });
document.addEventListener('visibilitychange', () => { 
  if (!document.hidden) { 
    setTimeout(() => { map.resize(); map.triggerRepaint(); }, 100);
    setTimeout(() => { map.resize(); map.triggerRepaint(); }, 500);
  }
});
new ResizeObserver(() => { try { map.resize(); map.triggerRepaint(); } catch(e){} }).observe(document.getElementById('map'));
// Periodic check for missing tiles during first 5 seconds
let checks = 0;
const tileCheck = setInterval(() => {
  if (++checks > 10) { clearInterval(tileCheck); return; }
  map.triggerRepaint();
}, 500);

</script>
<script src="https://cdn.jsdelivr.net/npm/@turf/turf@6.5.0/turf.min.js"></script>
</body>
</html>
    """).render(
        mapbox_token=mapbox_token, geojson_obj=geojson_obj, auto_state=auto_state,
        fill_color_config=fill_color_config, color_attr=color_attr, tooltip_columns=tooltip_columns,
        line_width=line_width, point_radius=point_radius, line_color_rgba=line_color_rgba,
        fill_color_rgba=fill_color_rgba, is_filled=is_filled, is_stroked=is_stroked,
        opacity=opacity, style_url=style_url,
    )

    common = fused.load("https://github.com/fusedio/udfs/tree/351515e/public/common/")
    return common.html_to_obj(html)




def deckgl_hex(
    df=None,
    config=None,
    mapbox_token: str = "pk.eyJ1IjoiaXNhYWNmdXNlZGxhYnMiLCJhIjoiY2xicGdwdHljMHQ1bzN4cWhtNThvbzdqcSJ9.73fb6zHMeO_c8eAXpZVNrA",
    basemap: str = "dark",
    debug: bool = False,
    tile_url: str = None,
    layers: list = None,
    highlight_on_click: bool = True,
    on_click: dict = None,
):
    """
    Render H3 hexagon data as an interactive map.
    
    Supports three modes:
    1. Static mode (default): Pass a DataFrame with 'hex' column
    2. Tile mode: Pass tile_url template
    3. Multi-layer mode: Pass layers list with multiple datasets
    
    Args:
        df: DataFrame with 'hex' column containing H3 cell IDs (ignored in tile/multi-layer mode).
        config: Optional config dict with hexLayer settings (getFillColor, getLineColor, etc.)
        mapbox_token: Mapbox access token.
        basemap: 'dark', 'satellite', or custom Mapbox style URL.
        debug: Show debug panel for config tweaking.
        tile_url: XYZ tile URL template with {z}/{x}/{y} placeholders. When provided, enables tile mode.
        layers: List of layer dicts for multi-layer mode. Each dict has:
            - "data": DataFrame with hex column
            - "config": hexLayer config for this layer
            - "name": Display name for layer toggle (optional)
        on_click: Dict to broadcast clicks. Keys: properties, channel, message_type, include_coords, include_layer.
    
    Examples:
        # Static mode (single layer)
        deckgl_hex(df, config, debug=True)
        
        # Tile mode
        deckgl_hex(tile_url="https://example.com/tiles/{z}/{x}/{y}?dtype_out_vector=json", config=config)
        
        # Multi-layer mode
        deckgl_hex(layers=[
            {"data": df1, "config": config1, "name": "Population"},
            {"data": df2, "config": config2, "name": "Income"},
        ], debug=True)
    """
    # Normalize all inputs to layers list format with type="hex"
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
        debug=debug,
        on_click=on_click,
    )


def deckgl_layers(
    layers: list,
    mapbox_token: str = "pk.eyJ1IjoiaXNhYWNmdXNlZGxhYnMiLCJhIjoiY2xicGdwdHljMHQ1bzN4cWhtNThvbzdqcSJ9.73fb6zHMeO_c8eAXpZVNrA",
    basemap: str = "dark",
    highlight_on_click: bool = True,
    debug: bool = False,
    on_click: dict = None,
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
    
    # Basemap setup
    basemap_styles = {
        "dark": "mapbox://styles/mapbox/dark-v11",
        "satellite": "mapbox://styles/mapbox/satellite-streets-v12",
        "light": "mapbox://styles/mapbox/light-v11",
        "streets": "mapbox://styles/mapbox/streets-v12"
    }
    style_url = basemap_styles.get(
        {"sat": "satellite", "satellite-streets": "satellite"}.get((basemap or "dark").lower(), (basemap or "dark").lower()),
        basemap or basemap_styles["dark"]
    )

    # Process each layer
    processed_layers = []
    has_tile_layers = False
    
    for i, layer_def in enumerate(layers):
        layer_type = layer_def.get("type", "hex").lower()
        df = layer_def.get("data")
        tile_url = layer_def.get("tile_url")
        config = layer_def.get("config", {})
        name = layer_def.get("name", f"Layer {i + 1}")
        
        if layer_type == "hex":
            # Process hex layer
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
            if not is_tile_layer and df is not None and hasattr(df, 'to_dict'):
                # Drop geometry column - hex layers use hex ID, not geometry
                df_clean = df.drop(columns=['geometry'], errors='ignore') if hasattr(df, 'drop') else df
                
                # Vectorized hex ID conversion (faster than looping)
                hex_col = next((c for c in ['hex', 'h3', 'index', 'id'] if c in df_clean.columns), None)
                if hex_col:
                    def _to_hex_str(val):
                        if val is None: return None
                        try:
                            if isinstance(val, (int, float)): return format(int(val), 'x')
                            if isinstance(val, str): return format(int(val), 'x') if val.isdigit() else val
                        except (ValueError, OverflowError): pass
                        return None
                    df_clean = df_clean.copy()
                    df_clean['hex'] = df_clean[hex_col].apply(_to_hex_str)
                
                data_records = df_clean.to_dict('records')
            
            tooltip_columns = _extract_tooltip_columns((config, merged_config, hex_layer))
            
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
                "tooltipColumns": tooltip_columns,
                "visible": True,
            })
            
        elif layer_type == "vector":
            # Process vector layer (similar to deckgl_map)
            merged_config = _load_deckgl_config(config, DEFAULT_DECK_CONFIG)
            vector_layer = merged_config.get("vectorLayer", {})
            
            # Convert GeoDataFrame to GeoJSON
            geojson_obj = {"type": "FeatureCollection", "features": []}
            if df is not None and hasattr(df, "to_json"):
                # Reproject to EPSG:4326 if needed
                if hasattr(df, "crs") and df.crs and getattr(df.crs, "to_epsg", lambda: None)() != 4326:
                    try:
                        df = df.to_crs(epsg=4326)
                    except Exception:
                        pass
                
                try:
                    geojson_obj = json.loads(df.to_json())
                except Exception:
                    pass
                
                # Sanitize properties using shared utility
                for feat in geojson_obj.get("features", []):
                    feat["properties"] = {k: _sanitize_geojson_value(v) for k, v in (feat.get("properties") or {}).items()}
            
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
            
            if isinstance(line_color_raw, dict) and line_color_raw.get("@@function") == "colorContinuous":
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
  <script type="module">
    import * as cartocolor from 'https://esm.sh/cartocolor@5.0.2';
    window.cartocolor = cartocolor;
  </script>
  <style>
    html, body { margin:0; height:100%; width:100%; display:flex; overflow:hidden; }
    #map { flex:1; height:100%; }
    #tooltip { position:absolute; pointer-events:none; background:rgba(15,15,15,0.95); color:#fff; padding:10px 14px; border-radius:6px; font-size:12px; display:none; z-index:6; max-width:320px; border:1px solid rgba(255,255,255,0.1); box-shadow:0 4px 16px rgba(0,0,0,0.4); font-family:Inter,'SF Pro Display','Segoe UI',sans-serif; }
    #tooltip .tt-title { display:block; margin-bottom:8px; padding-bottom:6px; border-bottom:1px solid rgba(255,255,255,0.15); font-size:11px; letter-spacing:0.3px; text-transform:uppercase; color:#E8FF59; }
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
      padding: 8px 10px;
      font-family: Inter, 'SF Pro Display', 'Segoe UI', sans-serif;
      color: #f5f5f5;
      min-width: 160px;
      z-index: 100;
      box-shadow: 0 4px 12px rgba(0,0,0,0.3);
    }
    .layer-item {
      display: flex;
      align-items: center;
      gap: 8px;
      padding: 6px 0;
      border-bottom: 1px solid #333;
    }
    .layer-item:last-child { border-bottom: none; }
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
    }
    .layer-item .layer-eye:hover { color: #fff; }
    .layer-item.disabled .layer-eye { color: #555; }
    .layer-item .layer-color {
      width: 12px;
      height: 12px;
      border-radius: 3px;
      border: 1px solid rgba(255,255,255,0.15);
    }
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
      left: 12px;
      bottom: 12px;
      background: rgba(15,15,15,0.9); 
      color: #fff;
      padding: 8px; 
      border-radius: 4px;
      font-size: 11px;
      z-index: 10;
      min-width: 140px;
      border: 1px solid rgba(255,255,255,0.1); 
    }
    .legend-layer { margin-bottom: 16px; padding-bottom: 12px; border-bottom: 1px solid rgba(255,255,255,0.1); }
    .legend-layer:last-child { margin-bottom: 0; padding-bottom: 0; border-bottom: none; }
    .legend-layer .legend-title { margin-bottom: 6px; font-weight: 500; display: flex; align-items: center; gap: 6px; }
    .legend-layer .legend-title .legend-dot { width: 8px; height: 8px; border-radius: 2px; }
    .legend-layer .legend-title .legend-line { width: 20px; height: 3px; border-radius: 1px; }
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
    
    {% if debug %}
    /* Debug Panel */
    #debug-panel {
      position: fixed;
      top: 0;
      right: 0;
      width: 320px;
      height: 100%;
      background: #212121;
      border-left: 1px solid #424242;
      display: flex;
      flex-direction: column;
      font-family: Inter, 'SF Pro Display', 'Segoe UI', sans-serif;
      color: #f5f5f5;
      z-index: 200;
    }
    #layer-panel { right: 332px !important; }
    #debug-header { padding: 12px 16px; background: #1a1a1a; border-bottom: 1px solid #424242; }
    #debug-header h3 { margin: 0; font-size: 12px; font-weight: 600; color: #E8FF59; letter-spacing: 0.5px; text-transform: uppercase; }
    #debug-tabs { display: flex; gap: 6px; padding: 10px 12px; background: #1a1a1a; border-bottom: 1px solid #333; overflow-y: auto; overflow-x: hidden; flex-shrink: 0; flex-wrap: wrap; max-height: 120px; }
    #debug-tabs::-webkit-scrollbar { height: 4px; width: 4px; }
    #debug-tabs::-webkit-scrollbar-thumb { background: rgba(255,255,255,0.2); border-radius: 4px; }
    .debug-tab { padding: 6px 12px; font-size: 11px; font-weight: 500; color: #888; cursor: pointer; border: 1px solid #444; background: #2a2a2a; border-radius: 4px; transition: all 0.15s; white-space: nowrap; display: flex; align-items: center; justify-content: center; gap: 6px; flex: 1 1 calc(50% - 8px); min-width: 120px; }
    .debug-tab:hover { color: #ccc; background: #333; border-color: #555; }
    .debug-tab.active { color: #1a1a1a; background: #E8FF59; border-color: #E8FF59; }
    .debug-tab .tab-dot { width: 8px; height: 8px; border-radius: 2px; flex-shrink: 0; }
    .debug-tab.active .tab-dot { box-shadow: 0 0 0 1px rgba(0,0,0,0.3); }
    #debug-content { flex: 1; overflow-y: auto; padding: 14px 16px; display: flex; flex-direction: column; gap: 14px; }
    .debug-section { background: #1a1a1a; border: 1px solid #424242; border-radius: 8px; padding: 12px; }
    .debug-section h4 { margin: 0 0 10px 0; font-size: 11px; letter-spacing: 0.4px; text-transform: uppercase; color: #bdbdbd; }
    .form-grid { display: grid; grid-template-columns: repeat(2, 1fr); gap: 10px; }
    .form-control { display: flex; flex-direction: column; gap: 4px; font-size: 11px; color: #dcdcdc; }
    .form-control label { font-weight: 600; }
    .form-control input, .form-control select { background: #111; border: 1px solid #333; border-radius: 4px; padding: 6px 8px; font-size: 12px; color: #f5f5f5; outline: none; }
    .form-control input:focus, .form-control select:focus { border-color: #E8FF59; }
    .toggle-row { display: flex; align-items: center; gap: 8px; font-size: 11px; }
    .toggle-row input { width: 16px; height: 16px; accent-color: #E8FF59; }
    .single-column { display: flex; flex-direction: column; gap: 10px; }
    #dbg-config { width: 100%; min-height: 100px; resize: vertical; background: #111; color: #f5f5f5; border: 1px solid #333; border-radius: 6px; padding: 10px; font-family: SFMono-Regular, Consolas, monospace; font-size: 11px; line-height: 1.4; }
    {% endif %}
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
  
  {% if debug %}
  <!-- Debug Panel -->
  <div id="debug-panel">
    <div id="debug-header">
      <h3>Layer Config</h3>
    </div>
    <div id="debug-tabs"></div>
    <div id="debug-content">
      <div class="debug-section">
        <h4>View</h4>
        <div class="form-grid">
          <div class="form-control">
            <label>Longitude</label>
            <input type="number" id="cfg-longitude" data-cfg-input step="0.0001" placeholder="auto" />
          </div>
          <div class="form-control">
            <label>Latitude</label>
            <input type="number" id="cfg-latitude" data-cfg-input step="0.0001" placeholder="auto" />
          </div>
          <div class="form-control">
            <label>Zoom</label>
            <input type="number" id="cfg-zoom" data-cfg-input step="0.1" min="0" placeholder="auto" />
          </div>
          <div class="form-control">
            <label>Pitch</label>
            <input type="number" id="cfg-pitch" data-cfg-input step="1" min="0" max="85" placeholder="0-85" />
          </div>
          <div class="form-control">
            <label>Bearing</label>
            <input type="number" id="cfg-bearing" data-cfg-input step="1" placeholder="deg" />
          </div>
        </div>
      </div>
      <div class="debug-section">
        <h4>Layer</h4>
        <div class="single-column">
          <label class="toggle-row"><input type="checkbox" id="cfg-filled" data-cfg-input checked />Filled</label>
          <label class="toggle-row"><input type="checkbox" id="cfg-extruded" data-cfg-input />Extruded (3D)</label>
          <div class="form-control">
            <label>Elevation Scale</label>
            <input type="number" id="cfg-elev-scale" data-cfg-input step="0.1" min="0" value="1" />
          </div>
        </div>
      </div>
      <div class="debug-section">
        <h4>Fill Color</h4>
        <div class="form-grid">
          <div class="form-control">
            <label>Attribute</label>
            <select id="cfg-attr" data-cfg-input></select>
          </div>
          <div class="form-control">
            <label>Palette</label>
            <select id="cfg-palette" data-cfg-input>
              {% for pal in palettes %}<option value="{{ pal }}">{{ pal }}</option>{% endfor %}
            </select>
          </div>
          <div class="form-control">
            <label>Domain Min</label>
            <input type="number" id="cfg-domain-min" data-cfg-input step="0.1" value="0" />
          </div>
          <div class="form-control">
            <label>Domain Max</label>
            <input type="number" id="cfg-domain-max" data-cfg-input step="0.1" value="100" />
          </div>
        </div>
      </div>
      <div class="debug-section">
        <h4>Config Output</h4>
        <textarea id="dbg-config" readonly></textarea>
      </div>
    </div>
  </div>
  {% endif %}

  <script>
    const MAPBOX_TOKEN = {{ mapbox_token | tojson }};
    const LAYERS_DATA = {{ layers_data | tojson }};
    const HAS_CUSTOM_VIEW = {{ has_custom_view | tojson }};
    
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
        // Auto-detect categories from data if not provided
        let catPairs = cfg.categories 
          ? cfg.categories.map(c => typeof c === 'object' ? c : { value: c, label: c })
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
      const [d0, d1] = cfg.domain, rev = d0 > d1, dom = rev ? [d1, d0] : [d0, d1];
      const steps = cfg.steps || 7, name = cfg.colors || 'TealGrn';
      let cols = getPaletteColors(name, steps);
      if (!cols || !cols.length) {
        return ['interpolate', ['linear'], ['get', cfg.attr], dom[0], 'rgb(237,248,251)', dom[1], 'rgb(0,109,44)'];
      }
      if (rev) cols.reverse();
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

    // ========== Map Setup ==========
    mapboxgl.accessToken = MAPBOX_TOKEN;
    const map = new mapboxgl.Map({ 
      container: 'map', 
      style: {{ style_url | tojson }}, 
      center: [{{ center_lng }}, {{ center_lat }}], 
      zoom: {{ zoom }}, 
      pitch: {{ pitch }}, 
      bearing: {{ bearing }}, 
      projection: 'mercator' 
    });

    // Deck.gl overlay for tile layers
    let deckOverlay = null;

    {% if has_tile_layers %}
    const { TileLayer, MapboxOverlay } = deck;
    const H3HexagonLayer = deck.H3HexagonLayer || (deck.GeoLayers && deck.GeoLayers.H3HexagonLayer);
    const { colorContinuous } = deck.carto;

    function toH3String(hex) {
      try {
        if (hex == null) return null;
        if (typeof hex === 'string') {
          const s = hex.startsWith('0x') ? hex.slice(2) : hex;
          return (/^\d+$/.test(s) ? BigInt(s).toString(16) : s.toLowerCase());
        }
        if (typeof hex === 'number') return BigInt(Math.trunc(hex)).toString(16);
        if (typeof hex === 'bigint') return hex.toString(16);
      } catch (_) {}
      return null;
    }

    function normalizeTileData(raw) {
      const arr = Array.isArray(raw) ? raw : (Array.isArray(raw?.data) ? raw.data : (Array.isArray(raw?.features) ? raw.features : []));
      const rows = arr.map(d => d?.properties ? { ...d.properties } : { ...d });
      return rows.map(p => {
        const hexRaw = p.hex ?? p.h3 ?? p.index ?? p.id;
        const hex = toH3String(hexRaw);
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
      return { attr: cfg.attr, domain, colors: cfg.colors || 'TealGrn', nullColor: cfg.nullColor || [184, 184, 184] };
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
    
    // Calculate domain from visible tile data for autoDomain layers
    function calculateDomainFromTiles(layerId, attr) {
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
      
      // Collect values from tiles that intersect viewport
      const values = [];
      let visibleTileCount = 0;
      
      for (const [tileKey, data] of Object.entries(tileData)) {
        // Parse tile coordinates from key (format: "z/x/y")
        const [z, x, y] = tileKey.split('/').map(Number);
        
        // Get geographic bounds for this tile
        const tileBounds = tileToBounds(x, y, z);
        
        // Only include data from tiles that intersect viewport
        if (boundsIntersect(tileBounds, viewportBounds)) {
          visibleTileCount++;
          for (const item of data) {
            const val = item?.[attr] ?? item?.properties?.[attr];
            if (typeof val === 'number' && isFinite(val)) {
              values.push(val);
            }
          }
        }
      }
      
      console.log(`[AutoDomain] ${layerId}: ${visibleTileCount} visible tiles, ${values.length} values`);
      
      if (!values.length) return null;
      return [Math.min(...values), Math.max(...values)];
    }

    function parseHexLayerConfigForDeck(config, layerId = null) {
      const out = {};
      for (const [k, v] of Object.entries(config || {})) {
        if (k === '@@type') continue;
        if (v && typeof v === 'object' && !Array.isArray(v)) {
          if (v['@@function'] === 'colorContinuous') {
            // Check if autoDomain is enabled
            let computedDomain = null;
            if (v.autoDomain === true && layerId && v.attr) {
              computedDomain = calculateDomainFromTiles(layerId, v.attr);
              if (computedDomain) {
                console.log(`[AutoDomain] ${layerId}: ${v.attr} → [${computedDomain[0].toFixed(2)}, ${computedDomain[1].toFixed(2)}]`);
              }
            }
            out[k] = colorContinuous(processColorContinuousCfg(v, computedDomain));
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
      
      // Check if this layer has autoDomain enabled
      const hasAutoDomain = rawHexLayer.getFillColor?.autoDomain === true;
      
      // Initialize tile data store for this layer if autoDomain is enabled
      if (hasAutoDomain && !TILE_DATA_STORE[layerDef.id]) {
        TILE_DATA_STORE[layerDef.id] = {};
      }

      const layerProps = {
        id: `${layerDef.id}-tiles`,
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
          if (TILE_CACHE.has(cacheKey)) {
            const cachedData = TILE_CACHE.get(cacheKey);
            // Store tile data for autoDomain
            if (hasAutoDomain) {
              const tileKey = `${z}/${x}/${y}`;
              TILE_DATA_STORE[layerDef.id][tileKey] = cachedData;
            }
            return cachedData;
          }
          try {
            const res = await fetch(url, { signal });
            if (!res.ok) return [];
            let text = await res.text();
            text = text.replace(/\"(hex|h3|index)\"\s*:\s*(\d+)/gi, (_m, k, d) => `"${k}":"${d}"`);
            const data = JSON.parse(text);
            const normalized = normalizeTileData(data);
            TILE_CACHE.set(cacheKey, normalized);
            // Store tile data for autoDomain
            if (hasAutoDomain) {
              const tileKey = `${z}/${x}/${y}`;
              TILE_DATA_STORE[layerDef.id][tileKey] = normalized;
            }
            return normalized;
          } catch (e) { return TILE_CACHE.get(cacheKey) || []; }
        },
        renderSubLayers: (props) => {
          const data = props.data || [];
          if (!data.length) return null;
          if (H3HexagonLayer) {
            return new H3HexagonLayer({
              id: `${props.id}-h3`,
              data,
              pickable: true,
              stroked: false,
              filled: true,
              extruded: isExtruded,
              elevationScale: elevationScale,
              coverage: 0.9,
              lineWidthMinPixels: 1,
              opacity: layerOpacity,
              getHexagon: d => d.hex,
              ...hexCfg
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
      
      if (!deckOverlay) {
        deckOverlay = new MapboxOverlay({
          interleaved: true,
          layers: deckLayers
        });
        map.addControl(deckOverlay);
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
      } else {
        deckOverlay.setProps({ layers: deckLayers });
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
        autoDomainTimeout = setTimeout(() => {
          console.log('[AutoDomain] Viewport changed, recalculating domains...');
          rebuildDeckOverlay();
        }, 300);  // Debounce: wait 300ms after movement stops
      });
    }
    {% endif %}

    // ========== Add All Layers ==========
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
        if (l.layerType === 'hex' && l.isTileLayer) return;  // Skip tile layers - handled by Deck.gl
        
        const geojson = layerGeoJSONs[l.id];
        if (!geojson || !geojson.features?.length) return;
        
        map.addSource(l.id, { type: 'geojson', data: geojson });
        const visible = layerVisibility[l.id];
        
        if (l.layerType === 'hex') {
          // Hex layer rendering
          const cfg = l.hexLayer || {};
          const fillColor = Array.isArray(cfg.getFillColor) ? toRgba(cfg.getFillColor, 0.8) : buildColorExpr(cfg.getFillColor, l.data) || 'rgba(0,144,255,0.7)';
          const lineColor = cfg.getLineColor ? (Array.isArray(cfg.getLineColor) ? toRgba(cfg.getLineColor, 1) : buildColorExpr(cfg.getLineColor, l.data)) : 'rgba(255,255,255,0.3)';
          const layerOpacity = (typeof cfg.opacity === 'number' && isFinite(cfg.opacity)) ? Math.max(0, Math.min(1, cfg.opacity)) : 0.8;
          
          if (cfg.extruded) {
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
          }
          map.addLayer({ 
            id: `${l.id}-outline`, 
            type: 'line', 
            source: l.id, 
            paint: { 'line-color': lineColor, 'line-width': cfg.lineWidthMinPixels || 0.5 },
            layout: { 'visibility': visible ? 'visible' : 'none' }
          });
          
        } else if (l.layerType === 'vector') {
          // Vector layer rendering
          const vecData = l.geojson?.features?.map(f => f.properties) || [];
          const fillColorExpr = l.fillColorConfig?.['@@function'] ? buildColorExpr(l.fillColorConfig, vecData) : (l.fillColorRgba || 'rgba(0,144,255,0.6)');
          const lineColorExpr = l.lineColorConfig?.['@@function'] ? buildColorExpr(l.lineColorConfig, vecData) : (l.lineColorRgba || 'rgba(100,100,100,0.8)');
          const lineW = (typeof l.lineWidth === 'number' && isFinite(l.lineWidth)) ? l.lineWidth : 1;
          const layerOpacity = (typeof l.opacity === 'number' && isFinite(l.opacity)) ? Math.max(0, Math.min(1, l.opacity)) : 0.8;
          
          // Detect geometry types
          let hasPoly = false, hasPoint = false, hasLine = false;
          for (const f of geojson.features || []) {
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

    // ========== Layer Panel ==========
    function updateLayerPanel() {
      const list = document.getElementById('layer-list');
      if (!list) return;
      
      list.innerHTML = LAYERS_DATA.map(l => {
        const visible = layerVisibility[l.id];
        let colorPreview = '#0090ff';
        
        if (l.layerType === 'hex') {
          const cfg = l.hexLayer || {};
          const colorCfg = (cfg.filled === false && cfg.getLineColor) ? cfg.getLineColor : cfg.getFillColor;
          if (Array.isArray(colorCfg)) {
            colorPreview = toRgba(colorCfg, 1) || colorPreview;
          } else if (colorCfg?.['@@function'] === 'colorContinuous' || colorCfg?.['@@function'] === 'colorCategories') {
            const paletteName = colorCfg.colors || (colorCfg['@@function'] === 'colorCategories' ? 'Bold' : 'TealGrn');
            const cols = getPaletteColors(paletteName, colorCfg.steps || 7);
            if (cols && cols.length) colorPreview = cols[Math.floor(cols.length / 2)];
          }
        } else if (l.layerType === 'vector') {
          if (l.lineColorRgba && !l.isFilled) colorPreview = l.lineColorRgba;
          else if (l.fillColorRgba) colorPreview = l.fillColorRgba;
          else if (l.fillColorConfig?.colors) {
            const cols = getPaletteColors(l.fillColorConfig.colors, 7);
            if (cols && cols.length) colorPreview = cols[Math.floor(cols.length / 2)];
          }
        }
        
        const eyeIcon = visible 
          ? '<svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M12 4.5C7 4.5 2.73 7.61 1 12c1.73 4.39 6 7.5 11 7.5s9.27-3.11 11-7.5c-1.73-4.39-6-7.5-11-7.5zM12 17c-2.76 0-5-2.24-5-5s2.24-5 5-5 5 2.24 5 5-2.24 5-5 5zm0-8c-1.66 0-3 1.34-3 3s1.34 3 3 3 3-1.34 3-3-1.34-3-3-3z"/></svg>'
          : '<svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M12 7c2.76 0 5 2.24 5 5 0 .65-.13 1.26-.36 1.83l2.92 2.92c1.51-1.26 2.7-2.89 3.43-4.75-1.73-4.39-6-7.5-11-7.5-1.4 0-2.74.25-3.98.7l2.16 2.16C10.74 7.13 11.35 7 12 7zM2 4.27l2.28 2.28.46.46C3.08 8.3 1.78 10.02 1 12c1.73 4.39 6 7.5 11 7.5 1.55 0 3.03-.3 4.38-.84l.42.42L19.73 22 21 20.73 3.27 3 2 4.27zM7.53 9.8l1.55 1.55c-.05.21-.08.43-.08.65 0 1.66 1.34 3 3 3 .22 0 .44-.03.65-.08l1.55 1.55c-.67.33-1.41.53-2.2.53-2.76 0-5-2.24-5-5 0-.79.2-1.53.53-2.2zm4.31-.78l3.15 3.15.02-.16c0-1.66-1.34-3-3-3l-.17.01z"/></svg>';
        return `
          <div class="layer-item ${visible ? '' : 'disabled'}" data-layer-id="${l.id}">
            <span class="layer-eye" onclick="toggleLayerVisibility('${l.id}', ${!visible})">${eyeIcon}</span>
            <div class="layer-color" style="background:${colorPreview};"></div>
            <span class="layer-name">${l.name}</span>
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
          colorCfg = l.fillColorConfig;
          
          // Show simple line legend for stroke-only vector layers
          if (!colorCfg?.['@@function'] && l.lineColorRgba && !l.isFilled) {
            html += `
              <div class="legend-layer">
                <div class="legend-title">
                  <span class="legend-line" style="background:${l.lineColorRgba};"></span>
                  ${l.name}
                </div>
              </div>
            `;
            return;
          }
        }
        
        // Only show legend for layers with explicit color functions
        const fnType = colorCfg?.['@@function'];
        if (!fnType || !colorCfg?.attr) return;
        if (fnType !== 'colorContinuous' && fnType !== 'colorCategories') return;
        const paletteName = colorCfg.colors || (fnType === 'colorCategories' ? 'Bold' : 'TealGrn');
        
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
              <div class="legend-title">
                <span class="legend-dot" style="background:${cols[0]};"></span>
                ${l.name}: ${titleAttr}
              </div>
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
        
        const [d0, d1] = colorCfg.domain;
        const isReversed = d0 > d1;
        const steps = colorCfg.steps || 7;
        
        let cols = getPaletteColors(paletteName, steps);
        if (!cols || !cols.length) {
          cols = ['#e0f3db','#ccebc5','#a8ddb5','#7bccc4','#4eb3d3','#2b8cbe','#0868ac','#084081'];
        }
        if (isReversed) cols = [...cols].reverse();
        
        const gradient = `linear-gradient(to right, ${cols.map((c, i) => `${c} ${i / (cols.length - 1) * 100}%`).join(', ')})`;
        const dotColor = cols[Math.floor(cols.length / 2)];
        
        html += `
          <div class="legend-layer">
            <div class="legend-title">
              <span class="legend-dot" style="background:${dotColor};"></span>
              ${l.name}: ${colorCfg.attr}
            </div>
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
          layerIds.push(`${l.id}-outline`);
        } else if (l.layerType === 'vector') {
          layerIds.push(`${l.id}-fill`, `${l.id}-outline`, `${l.id}-circle`, `${l.id}-line`);
        }
        layerIds.forEach(layerId => {
          allQueryableLayers.push({ layerId, layerDef: l });
        });
      });
      
      map.on('mousemove', (e) => { 
        const queryIds = allQueryableLayers.map(x => x.layerId).filter(id => map.getLayer(id));
        if (!queryIds.length) return;
        
        const features = map.queryRenderedFeatures(e.point, { layers: queryIds });
        if (!features?.length) {
          tt.style.display = 'none';
          map.getCanvas().style.cursor = '';
          return;
        }
        
        let topFeature = null;
        let topLayerDef = null;
        for (const f of features) {
          const match = allQueryableLayers.find(x => x.layerId === f.layer.id);
          if (match && layerVisibility[match.layerDef.id]) {
            topFeature = f;
            topLayerDef = match.layerDef;
            break;
          }
        }
        
        if (!topFeature || !topLayerDef) {
          tt.style.display = 'none';
          map.getCanvas().style.cursor = '';
          return;
        }

        map.getCanvas().style.cursor = 'pointer';
        const p = topFeature.properties;
        const cols = topLayerDef.tooltipColumns || [];
        const lines = cols.length 
          ? cols.map(k => p[k] != null ? `<span class="tt-row"><span class="tt-key">${k}</span><span class="tt-val">${typeof p[k]==='number'?p[k].toFixed(2):p[k]}</span></span>` : '').filter(Boolean) 
          : (p.hex ? [`<span class="tt-row"><span class="tt-key">hex</span><span class="tt-val">${p.hex.slice(0,12)}...</span></span>`] : Object.keys(p).slice(0, 5).map(k => `<span class="tt-row"><span class="tt-key">${k}</span><span class="tt-val">${p[k]}</span></span>`));
        if (lines.length) { 
          tt.innerHTML = `<strong class="tt-title">${topLayerDef.name}</strong>` + lines.join(''); 
          tt.style.left = `${e.point.x+10}px`; 
          tt.style.top = `${e.point.y+10}px`; 
          tt.style.display = 'block';
        } else {
          tt.style.display = 'none';
        }
      });
      
      map.on('mouseleave', () => { 
        map.getCanvas().style.cursor = '';
        tt.style.display = 'none'; 
      });

      {% if has_tile_layers %}
      map.on('mousemove', (e) => { 
        if (!deckOverlay) return;
        const info = deckOverlay.pickObject({ x: e.point.x, y: e.point.y, radius: 4 });
        if (info?.object) {
          const layerId = info.layer?.id?.split('-tiles-')[0];
          const layerDef = LAYERS_DATA.find(l => l.id === layerId || info.layer?.id?.startsWith(l.id));
          if (layerDef && layerVisibility[layerDef.id]) {
            map.getCanvas().style.cursor = 'pointer';
            const obj = info.object;
            const p = obj?.properties || obj || {};
            const cols = layerDef.tooltipColumns || [];
            const colorAttr = layerDef.hexLayer?.getFillColor?.attr || 'metric';
            const hexVal = p.hex || obj.hex;
            const lines = hexVal ? [`<span class="tt-row"><span class="tt-key">hex</span><span class="tt-val">${String(hexVal).slice(0, 12)}...</span></span>`] : [];
            
            if (cols.length) {
              cols.forEach(col => {
                const val = p[col] ?? obj[col];
                if (val !== undefined && val !== null) {
                  lines.push(`<span class="tt-row"><span class="tt-key">${col}</span><span class="tt-val">${typeof val === 'number' ? val.toFixed(2) : val}</span></span>`);
                }
              });
            } else if (p[colorAttr] != null || obj[colorAttr] != null) {
              const val = p[colorAttr] ?? obj[colorAttr];
              lines.push(`<span class="tt-row"><span class="tt-key">${colorAttr}</span><span class="tt-val">${Number(val).toFixed(2)}</span></span>`);
            }
            
            if (lines.length) {
              tt.innerHTML = `<strong class="tt-title">${layerDef.name}</strong>` + lines.join('');
              tt.style.left = `${e.point.x + 10}px`;
              tt.style.top = `${e.point.y + 10}px`;
              tt.style.display = 'block';
            }
            return;
          }
        }
        if (!map.queryRenderedFeatures(e.point).length) {
          tt.style.display = 'none';
          map.getCanvas().style.cursor = '';
        }
      });
      {% endif %}
    }

    map.on('load', tryInit);
    map.on('load', () => { [100, 500, 1000].forEach(t => setTimeout(() => map.resize(), t)); });
    window.addEventListener('resize', () => map.resize());
    document.addEventListener('visibilitychange', () => { if (!document.hidden) setTimeout(() => map.resize(), 100); });
    
    {% if highlight_on_click %}
    // Click-to-highlight (hex and vector)
    (function() {
      const HL_FILL = 'rgba(255,255,0,0.3)', HL_LINE = 'rgba(255,255,0,1)', HL_LW = 3;
      let selected = null, added = false;
      
      function toH3(hex) {
        if (!hex) return null;
        try {
          if (typeof hex === 'string') return /^\d+$/.test(hex) ? BigInt(hex).toString(16) : hex.toLowerCase();
          return BigInt(Math.trunc(hex)).toString(16);
        } catch(e) { return null; }
      }
      
      function highlight(feature) {
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
          // Otherwise use the feature's actual geometry (for vectors)
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
        if (typeof LAYERS_DATA !== 'undefined') LAYERS_DATA.forEach(l => {
          if (l.isTileLayer) return;
          const ids = l.layerType === 'vector' 
            ? [`${l.id}-fill`, `${l.id}-circle`, `${l.id}-line`]
            : [l.hexLayer?.extruded ? `${l.id}-extrusion` : `${l.id}-fill`];
          ids.forEach(id => { try { if (map.getLayer(id)) layers.push(id); } catch(e){} });
        });
        ['gdf-fill','gdf-circle','hex-fill'].forEach(id => { try { if (map.getLayer(id)) layers.push(id); } catch(e){} });
        return layers;
      }
      
      map.on('load', () => {
        map.on('click', e => {
          const queryLayers = getLayers();
          if (!queryLayers.length) return;
          
          let feats = [];
          try { feats = map.queryRenderedFeatures(e.point, { layers: queryLayers }) || []; } catch(err) {}
          
          if (feats.length > 0) {
            highlight(feats[0]);
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
        
        // Fallback: legacy single-layer IDs
        ['gdf-fill', 'gdf-circle', 'hex-fill'].forEach(id => {
          try {
            if (map.getLayer(id) && !layerIds.includes(id)) {
              layerIds.push(id);
            }
          } catch(e) {}
        });
        
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
        console.log('[ClickBroadcast] Feature clicked:', message);
      }
      
      // Initialize click handler when map is ready
      map.on('load', () => {
        map.on('click', handleClick);
        console.log('[ClickBroadcast] Initialized on channel:', CHANNEL);
      });
    })();
    {% endif %}
    
    {% if debug %}
    // ========== Debug Panel Functions ==========
    let activeLayerIdx = 0;
    
    function getNumericAttrs(layerIdx) {
      const layer = LAYERS_DATA[layerIdx];
      if (!layer) return [];
      const data = layer.data || (layer.geojson?.features?.map(f => f.properties) || []);
      if (!data.length) return [];
      const sample = data.slice(0, 50);
      const attrs = new Set();
      sample.forEach(d => {
        Object.keys(d || {}).forEach(k => {
          if (k !== 'hex' && k !== 'geometry' && typeof d[k] === 'number') attrs.add(k);
        });
      });
      return [...attrs].sort();
    }
    
    function autoDomainForAttr(layerIdx, attr) {
      const layer = LAYERS_DATA[layerIdx];
      if (!layer || !attr) return null;
      const data = layer.data || (layer.geojson?.features?.map(f => f.properties) || []);
      const vals = data.map(d => d?.[attr]).filter(v => typeof v === 'number' && isFinite(v));
      if (!vals.length) return null;
      return [Math.min(...vals), Math.max(...vals)];
    }
    
    function buildDebugTabs() {
      const container = document.getElementById('debug-tabs');
      if (!container) return;
      container.innerHTML = '';
      LAYERS_DATA.forEach((l, i) => {
        const tab = document.createElement('div');
        tab.className = 'debug-tab' + (i === activeLayerIdx ? ' active' : '');
        const colorCfg = l.hexLayer?.getFillColor || l.hexLayer?.getLineColor || l.fillColorConfig;
        const palName = colorCfg?.colors || 'Magenta';
        const colors = getPaletteColors(palName, 5) || ['#666'];
        const midColor = colors[Math.floor(colors.length / 2)];
        tab.innerHTML = `<span class="tab-dot" style="background:${midColor}"></span>${l.name}`;
        tab.onclick = () => { activeLayerIdx = i; buildDebugTabs(); updateFormFromLayer(i); };
        container.appendChild(tab);
      });
    }
    
    function updateFormFromLayer(idx) {
      const layer = LAYERS_DATA[idx];
      if (!layer) return;
      const hex = layer.hexLayer || {};
      const colorCfg = hex.getFillColor || hex.getLineColor || {};
      const viewCfg = layer.config?.initialViewState || {};
      
      const attrs = getNumericAttrs(idx);
      const attrSelect = document.getElementById('cfg-attr');
      if (attrSelect) {
        attrSelect.innerHTML = attrs.map(a => `<option value="${a}">${a}</option>`).join('');
        attrSelect.value = colorCfg.attr || attrs[0] || '';
      }
      
      document.getElementById('cfg-palette').value = colorCfg.colors || 'Magenta';
        document.getElementById('cfg-domain-min').value = colorCfg.domain?.[0] ?? 0;
        document.getElementById('cfg-domain-max').value = colorCfg.domain?.[1] ?? 100;
      document.getElementById('cfg-filled').checked = hex.filled !== false;
      document.getElementById('cfg-extruded').checked = hex.extruded === true;
      document.getElementById('cfg-elev-scale').value = hex.elevationScale || 1;
      document.getElementById('cfg-longitude').value = viewCfg.longitude || '';
      document.getElementById('cfg-latitude').value = viewCfg.latitude || '';
      document.getElementById('cfg-zoom').value = viewCfg.zoom || '';
      document.getElementById('cfg-pitch').value = viewCfg.pitch || 0;
      document.getElementById('cfg-bearing').value = viewCfg.bearing || 0;
      
      updateConfigOutput();
    }
    
    function applyViewToMap(viewCfg) {
      if (viewCfg.longitude && viewCfg.latitude) map.setCenter([viewCfg.longitude, viewCfg.latitude]);
      if (viewCfg.zoom) map.setZoom(viewCfg.zoom);
      if (viewCfg.pitch != null) map.setPitch(viewCfg.pitch);
      if (viewCfg.bearing != null) map.setBearing(viewCfg.bearing);
    }
    
    function buildConfigFromForm() {
      const isFilled = document.getElementById('cfg-filled').checked;
      const colorConfig = {
        '@@function': 'colorContinuous',
        attr: document.getElementById('cfg-attr').value,
        colors: document.getElementById('cfg-palette').value,
        domain: [parseFloat(document.getElementById('cfg-domain-min').value) || 0, parseFloat(document.getElementById('cfg-domain-max').value) || 100],
        steps: 7
      };
      const view = {};
      const lng = parseFloat(document.getElementById('cfg-longitude').value);
      const lat = parseFloat(document.getElementById('cfg-latitude').value);
      const zm = parseFloat(document.getElementById('cfg-zoom').value);
      const pt = parseFloat(document.getElementById('cfg-pitch').value);
      const br = parseFloat(document.getElementById('cfg-bearing').value);
      if (!isNaN(lng)) view.longitude = lng;
      if (!isNaN(lat)) view.latitude = lat;
      if (!isNaN(zm)) view.zoom = zm;
      if (!isNaN(pt)) view.pitch = pt;
      if (!isNaN(br)) view.bearing = br;
      const hex = {
        filled: isFilled,
        extruded: document.getElementById('cfg-extruded').checked,
        elevationScale: parseFloat(document.getElementById('cfg-elev-scale').value) || 1,
      };
      if (isFilled) hex.getFillColor = colorConfig;
      else hex.getLineColor = colorConfig;
      const cfg = { hexLayer: hex };
      if (Object.keys(view).length) cfg.initialViewState = view;
      return cfg;
    }
    
    function toPython(value, indent = 4, level = 0) {
      const pad = ' '.repeat(indent * level);
      const padNext = ' '.repeat(indent * (level + 1));
      if (value === null) return 'None';
      if (typeof value === 'boolean') return value ? 'True' : 'False';
      if (typeof value === 'number') return String(value);
      if (typeof value === 'string') return JSON.stringify(value);
      if (Array.isArray(value)) return !value.length ? '[]' : '[' + value.map(v => toPython(v, indent, level)).join(', ') + ']';
      if (typeof value === 'object') {
        const keys = Object.keys(value);
        if (!keys.length) return '{}';
        return '{\n' + keys.map(key => padNext + JSON.stringify(key) + ': ' + toPython(value[key], indent, level + 1)).join(',\n') + '\n' + pad + '}';
      }
      return 'None';
    }
    
    function updateConfigOutput() {
      const cfg = buildConfigFromForm();
      const output = document.getElementById('dbg-config');
      if (output) output.value = 'config = ' + toPython(cfg);
    }
    
    function applyDebugConfig() {
      const layer = LAYERS_DATA[activeLayerIdx];
      if (!layer || layer.layerType === 'vector') return;
      const builtConfig = buildConfigFromForm();
      const newHex = builtConfig.hexLayer;
      const newView = builtConfig.initialViewState || {};
      layer.hexLayer = { ...layer.hexLayer, ...newHex };
      layer.config.hexLayer = layer.hexLayer;
      if (Object.keys(newView).length) {
        layer.config.initialViewState = newView;
        applyViewToMap(newView);
      }
      if (layer.isTileLayer) {
        {% if has_tile_layers %}rebuildDeckOverlay();{% endif %}
      } else {
        layersReady = false;
        tryInit();
      }
      updateLegend();
      updateLayerPanel();
      buildDebugTabs();
    }
    
    let applyTimer;
    function scheduleApply() {
      clearTimeout(applyTimer);
      applyTimer = setTimeout(() => { applyDebugConfig(); updateConfigOutput(); }, 150);
    }
    
    document.querySelectorAll('#debug-panel [data-cfg-input]').forEach(el => {
      el.addEventListener('input', scheduleApply);
      el.addEventListener('change', scheduleApply);
    });
    const attrSelectEl = document.getElementById('cfg-attr');
    if (attrSelectEl) {
      attrSelectEl.addEventListener('change', (e) => {
        const domain = autoDomainForAttr(activeLayerIdx, e.target.value);
        if (domain) {
          document.getElementById('cfg-domain-min').value = domain[0];
          document.getElementById('cfg-domain-max').value = domain[1];
        }
      });
    }
    map.on('load', () => { setTimeout(() => { buildDebugTabs(); updateFormFromLayer(0); }, 100); });
    {% endif %}
  </script>
</body>
</html>
""").render(
        mapbox_token=mapbox_token,
        layers_data=processed_layers,
        style_url=style_url,
        center_lng=center_lng,
        center_lat=center_lat,
        zoom=zoom,
        pitch=pitch,
        bearing=bearing,
        has_custom_view=has_custom_view,
        has_tile_layers=has_tile_layers,
        highlight_on_click=highlight_on_click,
        debug=debug,
        palettes=sorted(KNOWN_CARTOCOLOR_PALETTES),
        on_click=on_click or {},
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
  style: 'mapbox://styles/mapbox/dark-v11',
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
  
  console.log('[map_broadcast] Initialized on channel:', CHANNEL);
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
        common = fused.load("https://github.com/fusedio/udfs/tree/f430c25/public/common/")
        return common.html_to_obj(injected_html)
    return injected_html


def enable_location_listener(html_input, channel: str = "fused-bus"):
    """
    Inject location listener into any Mapbox GL map HTML.
    
    When a location_selector broadcasts a location change, this map will
    automatically pan/zoom to fit the specified bounds.
    
    Args:
        html_input: HTML string or response object containing a Mapbox GL map
        channel: BroadcastChannel name (must match location_selector's channel)
    
    Returns:
        Modified HTML with location listener capability
    
    Usage:
        html = deckgl_hex(df, config)
        map_with_listener = enable_location_listener(html, channel="fused-bus")
        return map_with_listener
    """
    html_string, response_mode = _normalize_html_input(html_input)
    
    listener_script = f"""
<script>
(function() {{
  if (typeof map === 'undefined') return;
  
  const CHANNEL = {json.dumps(channel)};
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
          
          // Fit map to bounds
          map.fitBounds(
            [[west, south], [east, north]],
            {{
              padding: 50,
              duration: 1000,
              maxZoom: 15
            }}
          );
          
          console.log('[map_location_listener] Panned to:', loc.name, loc.bounds);
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
  
  console.log('[map_location_listener] Initialized on channel:', CHANNEL);
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
        common = fused.load("https://github.com/fusedio/udfs/tree/f430c25/public/common/")
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
    console.log('[hex_click_broadcast] Clicked hex:', hexId, propsToSend);
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
        console.log('[hex_click_broadcast] Clicked tile hex:', hexId, propsToSend);
      }}
    }});
  }}
  
  console.log('[hex_click_broadcast] Initialized on channel:', CHANNEL);
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
        common = fused.load("https://github.com/fusedio/udfs/tree/f430c25/public/common/")
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
        common = fused.load("https://github.com/fusedio/udfs/tree/f430c25/public/common/")
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


def pydeck_point(gdf, config=None):
    """
    Pydeck based maps. Use this to render HTML interactive maps from data.

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
    Pydeck based maps. Use this to render HTML interactive maps from data.

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