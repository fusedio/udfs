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

    config_errors = []

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

    # Sanitize properties
    def sanitize_value(v):
        if isinstance(v, (float, int, str, bool)) or v is None:
            return None if isinstance(v, float) and (math.isnan(v) or math.isinf(v)) else v
        if isinstance(v, (_np.floating, _np.integer, _np.bool_)):
            try:
                return v.item()
            except Exception:
                return str(v)
        if isinstance(v, (list, tuple, set)):
                return ", ".join(str(s) for s in v)
        return str(v) if v is not None else None

    for feat in geojson_obj.get("features", []):
        feat["properties"] = {k: sanitize_value(v) for k, v in (feat.get("properties") or {}).items()}

    # Auto-center
    auto_center = (0.0, 0.0)
    if hasattr(gdf, "total_bounds"):
        try:
            minx, miny, maxx, maxy = gdf.total_bounds
            auto_center = ((minx + maxx) / 2.0, (miny + maxy) / 2.0)
        except Exception:
            pass

    # Config processing
    merged_config = _load_deckgl_config(config, DEFAULT_DECK_CONFIG, "deckgl_map", config_errors)
    initial_view_state = _validate_initial_view_state(merged_config, DEFAULT_DECK_CONFIG["initialViewState"], "deckgl_map", config_errors)
    vector_layer = _ensure_layer_config(merged_config, "vectorLayer", DEFAULT_DECK_CONFIG["vectorLayer"], "deckgl_map", config_errors)

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
    if isinstance(fill_color_raw, dict) and fill_color_raw.get("@@function") == "colorContinuous":
        fill_color_config, color_attr = fill_color_raw, fill_color_raw.get("attr")
        # Validate palette name
        palette_name = fill_color_raw.get("colors", "TealGrn")
        if palette_name and palette_name not in KNOWN_CARTOCOLOR_PALETTES:
            suggestion = get_close_matches(palette_name, list(KNOWN_CARTOCOLOR_PALETTES), n=1, cutoff=0.5)
            if suggestion:
                print(f"[deckgl_map] Warning: Palette '{palette_name}' not found. Did you mean '{suggestion[0]}'?")
                config_errors.append(f"Palette '{palette_name}' not found. Did you mean '{suggestion[0]}'?")
            else:
                print(f"[deckgl_map] Warning: Palette '{palette_name}' not found. Using fallback colors.")
                print(f"  Valid palettes: {', '.join(sorted(list(KNOWN_CARTOCOLOR_PALETTES))[:15])}...")
                config_errors.append(f"Palette '{palette_name}' not found. Valid: {', '.join(sorted(list(KNOWN_CARTOCOLOR_PALETTES))[:10])}...")
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
    .mapboxgl-popup-content { font-family: monospace; font-size: 12px; background: rgba(0,0,0,0.85); color:#fff; padding:6px 8px; border-radius:6px; }
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
const CONFIG_ERRORS = {{ config_errors | tojson }};

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
function tip(p) { const k = TOOLTIP_COLUMNS.length ? TOOLTIP_COLUMNS : Object.keys(p||{}); return k.map(k=>p?.[k]!=null&&String(p[k])!=='null'?`${k}: ${fmt(p[k])}`:'').filter(Boolean).join(' • '); }
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

// Config errors display
if (CONFIG_ERRORS?.length) {
  const box = document.getElementById('config-error');
  if (box) { box.innerHTML = CONFIG_ERRORS.join('<br>'); box.style.display = 'block'; }
}
</script>
<script src="https://cdn.jsdelivr.net/npm/@turf/turf@6.5.0/turf.min.js"></script>
</body>
</html>
    """).render(
        mapbox_token=mapbox_token, geojson_obj=geojson_obj, auto_state=auto_state,
        fill_color_config=fill_color_config, color_attr=color_attr, tooltip_columns=tooltip_columns,
        line_width=line_width, point_radius=point_radius, line_color_rgba=line_color_rgba,
        fill_color_rgba=fill_color_rgba, is_filled=is_filled, is_stroked=is_stroked,
        opacity=opacity, style_url=style_url, config_errors=config_errors,
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
    # Route to multi-layer mode if layers is provided
    if layers:
        return _deckgl_hex_multi(
            layers=layers,
            mapbox_token=mapbox_token,
            basemap=basemap,
        )
    
    # Route to tile mode if tile_url is provided
    if tile_url:
        return _deckgl_hex_tiles(
            tile_url=tile_url,
            config=config,
            mapbox_token=mapbox_token,
            basemap=basemap,
            debug=debug,
        )
    # Basemap setup
    basemap_styles = {"dark": "mapbox://styles/mapbox/dark-v11", "satellite": "mapbox://styles/mapbox/satellite-streets-v12", "light": "mapbox://styles/mapbox/light-v11", "streets": "mapbox://styles/mapbox/streets-v12"}
    style_url = basemap_styles.get({"sat": "satellite", "satellite-streets": "satellite"}.get((basemap or "dark").lower(), (basemap or "dark").lower()), basemap or basemap_styles["dark"])

    # Config processing
    config_errors = []
    original_config = deepcopy(config) if config else {}
    merged_config = _load_deckgl_config(config, DEFAULT_DECK_HEX_CONFIG, "deckgl_hex", config_errors)
    hex_layer = merged_config.get("hexLayer", {})

    # Validate hexLayer property names
    invalid_props = [key for key in list(hex_layer.keys()) if key not in VALID_HEX_LAYER_PROPS]
    for prop in invalid_props:
        suggestion = get_close_matches(prop, list(VALID_HEX_LAYER_PROPS), n=1, cutoff=0.6)
        if suggestion:
            print(f"[deckgl_hex] Warning: Property '{prop}' not recognized. Did you mean '{suggestion[0]}'?")
            config_errors.append(f"Property '{prop}' not recognized. Did you mean '{suggestion[0]}'?")
        else:
            print(f"[deckgl_hex] Warning: Property '{prop}' not recognized by H3HexagonLayer.")
            config_errors.append(f"Property '{prop}' not recognized.")
        hex_layer.pop(prop, None)

    # Validate palette name for getFillColor
    fill_color_cfg = hex_layer.get("getFillColor", {})
    if isinstance(fill_color_cfg, dict) and fill_color_cfg.get("@@function") == "colorContinuous":
        palette_name = fill_color_cfg.get("colors", "TealGrn")
        if palette_name and palette_name not in KNOWN_CARTOCOLOR_PALETTES:
            suggestion = get_close_matches(palette_name, list(KNOWN_CARTOCOLOR_PALETTES), n=1, cutoff=0.5)
            if suggestion:
                print(f"[deckgl_hex] Warning: Palette '{palette_name}' not found. Did you mean '{suggestion[0]}'?")
                config_errors.append(f"Palette '{palette_name}' not found. Did you mean '{suggestion[0]}'?")
            else:
                print(f"[deckgl_hex] Warning: Palette '{palette_name}' not found. Using fallback colors.")
                print(f"  Valid palettes: {', '.join(sorted(list(KNOWN_CARTOCOLOR_PALETTES))[:15])}...")
                config_errors.append(f"Palette '{palette_name}' not found. Valid: {', '.join(sorted(list(KNOWN_CARTOCOLOR_PALETTES))[:10])}...")

    # Validate palette name for getLineColor (if colorContinuous)
    line_color_cfg = hex_layer.get("getLineColor", {})
    if isinstance(line_color_cfg, dict) and line_color_cfg.get("@@function") == "colorContinuous":
        palette_name = line_color_cfg.get("colors", "TealGrn")
        if palette_name and palette_name not in KNOWN_CARTOCOLOR_PALETTES:
            suggestion = get_close_matches(palette_name, list(KNOWN_CARTOCOLOR_PALETTES), n=1, cutoff=0.5)
            if suggestion:
                print(f"[deckgl_hex] Warning: Line palette '{palette_name}' not found. Did you mean '{suggestion[0]}'?")
                config_errors.append(f"Line palette '{palette_name}' not found. Did you mean '{suggestion[0]}'?")
            else:
                print(f"[deckgl_hex] Warning: Line palette '{palette_name}' not found. Using fallback colors.")
                config_errors.append(f"Line palette '{palette_name}' not found.")

    # Convert dataframe to records, handling hex ID conversion
    data_records = []
    if hasattr(df, 'to_dict'):
        data_records = df.to_dict('records')
        for record in data_records:
            hex_val = record.get('hex') or record.get('h3') or record.get('index') or record.get('id')
            if hex_val is not None:
                try:
                    if isinstance(hex_val, (int, float)):
                        record['hex'] = format(int(hex_val), 'x')
                    elif isinstance(hex_val, str) and hex_val.isdigit():
                        record['hex'] = format(int(hex_val), 'x')
                    else:
                        record['hex'] = hex_val
                except (ValueError, OverflowError):
                    record['hex'] = None
        
    # Auto-center from data
    auto_center, auto_zoom = (-119.4179, 36.7783), 5
    if data_records and 'lat' in data_records[0] and 'lng' in data_records[0]:
        lats = [r["lat"] for r in data_records if "lat" in r]
        lngs = [r["lng"] for r in data_records if "lng" in r]
        if lats and lngs:
            auto_center = (sum(lngs) / len(lngs), sum(lats) / len(lats))
            auto_zoom = 8

    # View state
    ivs = merged_config.get('initialViewState', {})
    center_lng = ivs.get('longitude') or auto_center[0]
    center_lat = ivs.get('latitude') or auto_center[1]
    zoom = ivs.get('zoom') or auto_zoom
    pitch = ivs.get('pitch', 0)
    bearing = ivs.get('bearing', 0)
    user_initial_state = {}
    if isinstance(original_config, dict):
        user_initial_state = original_config.get('initialViewState', {}) or {}
    has_custom_view = any(
        user_initial_state.get(key) is not None
        for key in ('longitude', 'latitude', 'zoom', 'pitch', 'bearing')
    )

    # Tooltip columns
    tooltip_columns = _extract_tooltip_columns((merged_config, hex_layer))
    if not tooltip_columns and data_records:
        tooltip_columns = [k for k in data_records[0].keys() if k not in ['hex', 'lat', 'lng']]

    html = Template(r"""
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <link href="https://api.mapbox.com/mapbox-gl-js/v3.2.0/mapbox-gl.css" rel="stylesheet" />
  <script src="https://api.mapbox.com/mapbox-gl-js/v3.2.0/mapbox-gl.js"></script>
  <script src="https://unpkg.com/h3-js@4.1.0/dist/h3-js.umd.js"></script>
  <script type="module">
    import * as cartocolor from 'https://esm.sh/cartocolor@5.0.2';
    window.cartocolor = cartocolor;
  </script>
  <style>
    html, body { margin:0; height:100%; width:100%; display:flex; overflow:hidden; }
    #map { flex:1; height:100%; }
    #tooltip { position:absolute; pointer-events:none; background:rgba(0,0,0,0.7); color:#fff; padding:4px 8px; border-radius:4px; font-size:12px; display:none; z-index:6; }
    .config-error { position:fixed; right:12px; bottom:12px; background:rgba(180,30,30,0.92); color:#fff; padding:6px 10px; border-radius:4px; font-size:11px; display:none; z-index:8; max-width:260px; }
    .color-legend { position:fixed; left:12px; bottom:12px; background:rgba(15,15,15,0.9); color:#fff; padding:8px; border-radius:4px; font-size:11px; display:none; z-index:10; min-width:140px; border:1px solid rgba(255,255,255,0.1); }
    .color-legend .legend-title { margin-bottom:6px; font-weight:500; }
    .color-legend .legend-gradient { height:12px; border-radius:2px; margin-bottom:4px; border:1px solid rgba(255,255,255,0.2); }
    .color-legend .legend-labels { display:flex; justify-content:space-between; font-size:10px; color:#ccc; }
    /* Debug Panel - Material Grey + Workbench Theme */
    #debug-panel { width:400px; height:100%; background:#212121; border-left:1px solid #424242; display:flex; flex-direction:column; font-family:Inter, 'SF Pro Display', 'Segoe UI', sans-serif; color:#f5f5f5; }
    #debug-header { padding:12px 18px; background:#1a1a1a; border-bottom:1px solid #424242; }
    #debug-header h3 { margin:0; font-size:12px; font-weight:600; color:#E8FF59; letter-spacing:0.5px; text-transform:uppercase; }
    #debug-content { flex:1; overflow-y:auto; padding:14px 18px; display:flex; flex-direction:column; gap:16px; }
    .debug-section { background:#1a1a1a; border:1px solid #424242; border-radius:8px; padding:12px; }
    .debug-section h4 { margin:0 0 10px 0; font-size:11px; letter-spacing:0.4px; text-transform:uppercase; color:#bdbdbd; }
    .form-grid { display:grid; grid-template-columns:repeat(2, minmax(0,1fr)); gap:10px; }
    .form-control { display:flex; flex-direction:column; gap:4px; font-size:11px; color:#dcdcdc; }
    .form-control label { font-weight:600; }
    .form-control input, .form-control select { background:#111; border:1px solid #333; border-radius:4px; padding:6px 8px; font-size:12px; color:#f5f5f5; outline:none; }
    .form-control input:focus, .form-control select:focus { border-color:#E8FF59; }
    .toggle-row { display:flex; align-items:center; gap:8px; font-size:11px; }
    .toggle-row input { width:16px; height:16px; }
    .single-column { display:flex; flex-direction:column; gap:10px; }
    #debug-buttons { display:flex; gap:8px; padding:12px 18px; background:#1a1a1a; border-top:1px solid #424242; }
    .dbtn { flex:1; border:none; border-radius:4px; padding:10px; font-size:11px; font-weight:600; cursor:pointer; text-transform:uppercase; letter-spacing:0.5px; }
    .dbtn.secondary { background:#424242; color:#fff; }
    .dbtn.secondary:hover { background:#616161; }
    .dbtn.ghost { background:transparent; color:#E8FF59; border:1px solid rgba(232,255,89,0.3); }
    .dbtn.ghost:hover { border-color:#E8FF59; }
    .toast { position:fixed; top:20px; left:50%; transform:translateX(-50%); background:#E8FF59; color:#1a1a1a; padding:10px 20px; border-radius:4px; font-weight:600; font-size:12px; z-index:999; animation:fade 2s forwards; }
    @keyframes fade { 0%,70%{opacity:1} 100%{opacity:0} }
    #cfg-output { width:100%; min-height:120px; resize:vertical; background:#111; color:#f5f5f5; border:1px solid #333; border-radius:6px; padding:10px; font-family:SFMono-Regular,Consolas,monospace; font-size:11px; line-height:1.4; }
  </style>
</head>
<body>
  <div id="map"></div>
  <div id="tooltip"></div>
  <div id="config-error" class="config-error"></div>
  <div id="color-legend" class="color-legend"><div class="legend-title"></div><div class="legend-gradient"></div><div class="legend-labels"><span class="legend-min"></span><span class="legend-max"></span></div></div>
  
  {% if debug %}
  <div id="debug-panel">
    <div id="debug-header">
      <h3>Config</h3>
    </div>
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
          <label class="toggle-row"><input type="checkbox" id="cfg-filled" data-cfg-input checked />Filled polygons</label>
          <label class="toggle-row"><input type="checkbox" id="cfg-extruded" data-cfg-input />Extruded (3D)</label>
          <label class="toggle-row"><input type="checkbox" id="cfg-pickable" data-cfg-input checked />Pickable / tooltips</label>
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
              {% for pal in palettes %}<option value="{{ pal }}">{{ pal }}</option>
              {% endfor %}
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
          <div class="form-control">
            <label>Steps</label>
            <input type="number" id="cfg-steps" data-cfg-input min="2" max="20" value="7" />
          </div>
        </div>
      </div>
      <div class="debug-section">
        <h4>Config Output</h4>
        <textarea id="cfg-output" readonly></textarea>
      </div>
    </div>
    <div id="debug-buttons">
      <button class="dbtn secondary" onclick="resetConfig()">Reset</button>
    </div>
  </div>
  {% endif %}

  <script>
    const MAPBOX_TOKEN = {{ mapbox_token | tojson }};
    const DATA = {{ data_records | tojson }};
    let CONFIG = {{ config | tojson }};
    const USER_CONFIG = {{ user_config | tojson }};
    const ORIGINAL_CONFIG = USER_CONFIG && Object.keys(USER_CONFIG).length ? JSON.parse(JSON.stringify(USER_CONFIG)) : JSON.parse(JSON.stringify(CONFIG));
    const TOOLTIP_COLUMNS = {{ tooltip_columns | tojson }};
    const CONFIG_ERRORS = {{ config_errors | tojson }};
    const DEBUG_MODE = {{ debug | tojson }};
    const HAS_CUSTOM_VIEW = {{ has_custom_view | tojson }};

// H3 ID conversion
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

// Convert to GeoJSON
function toGeoJSON(data) {
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

// Get palette colors from cartocolor
function getPaletteColors(name, steps) {
  const pal = window.cartocolor?.[name];
  if (!pal) return null;
  const keys = Object.keys(pal).map(Number).filter(n => !isNaN(n)).sort((a, b) => a - b);
  const best = keys.find(n => n >= steps) || keys[keys.length - 1];
  return pal[best] ? [...pal[best]] : null;
}

// Build color expression
function buildColorExpr(cfg) {
  if (!cfg || cfg['@@function'] !== 'colorContinuous' || !cfg.attr || !cfg.domain?.length) return 'rgba(0,144,255,0.7)';
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

const geojson = toGeoJSON(DATA);

    mapboxgl.accessToken = MAPBOX_TOKEN;
const map = new mapboxgl.Map({ container: 'map', style: {{ style_url | tojson }}, center: [{{ center_lng }}, {{ center_lat }}], zoom: {{ zoom }}, pitch: {{ pitch }}, bearing: {{ bearing }}, projection: 'mercator' });

// Convert [r,g,b] or [r,g,b,a] array to rgba string
function toRgba(arr, defaultAlpha) {
  if (!Array.isArray(arr) || arr.length < 3) return null;
  const [r, g, b] = arr;
  const a = arr.length >= 4 ? arr[3] / 255 : (defaultAlpha ?? 1);
  return `rgba(${r},${g},${b},${a})`;
}

function addLayers() {
  ['hex-fill','hex-extrusion','hex-outline'].forEach(id => { try { if(map.getLayer(id)) map.removeLayer(id); } catch(e){} });
  try { if(map.getSource('hex-source')) map.removeSource('hex-source'); } catch(e){}
  
  map.addSource('hex-source', { type: 'geojson', data: geojson });
  const cfg = CONFIG.hexLayer || {};
  const fillColor = Array.isArray(cfg.getFillColor) ? toRgba(cfg.getFillColor, 0.8) : buildColorExpr(cfg.getFillColor);
  const lineColor = cfg.getLineColor ? (Array.isArray(cfg.getLineColor) ? toRgba(cfg.getLineColor, 1) : buildColorExpr(cfg.getLineColor)) : 'rgba(255,255,255,0.3)';
  const layerOpacity = (typeof cfg.opacity === 'number' && isFinite(cfg.opacity)) ? Math.max(0, Math.min(1, cfg.opacity)) : 0.8;
  
  if (cfg.extruded) {
    const elev = cfg.elevationScale || 1;
    map.addLayer({
      id:'hex-extrusion',
      type:'fill-extrusion',
      source:'hex-source',
      paint:{
        'fill-extrusion-color':fillColor,
        'fill-extrusion-height':cfg.getFillColor?.attr ? ['*',['get',cfg.getFillColor.attr],elev] : 100,
        'fill-extrusion-base':0,
        'fill-extrusion-opacity':layerOpacity
      }
    });
    // Optional outline for extruded view so users can see stroke color
        map.addLayer({
      id:'hex-outline',
      type:'line',
      source:'hex-source',
      paint:{
        'line-color':lineColor,
        'line-width':cfg.lineWidthMinPixels || 0.5
      }
    });
  } else {
    if (cfg.filled !== false) map.addLayer({ id:'hex-fill', type:'fill', source:'hex-source', paint:{ 'fill-color':fillColor, 'fill-opacity':layerOpacity }});
    map.addLayer({ id:'hex-outline', type:'line', source:'hex-source', paint:{ 'line-color':lineColor, 'line-width':cfg.lineWidthMinPixels || 0.5 }});
  }
}

function showLegend() {
  const cfg = CONFIG.hexLayer || {};
  const colorCfg = cfg.filled === false ? (cfg.getLineColor || cfg.getFillColor) : (cfg.getFillColor || cfg.getLineColor);
  if (!colorCfg || colorCfg['@@function'] !== 'colorContinuous' || !colorCfg.attr || !colorCfg.domain?.length) return;
  
  const [d0, d1] = colorCfg.domain;
  const isReversed = d0 > d1;
  const steps = colorCfg.steps || 7;
  const paletteName = colorCfg.colors || 'TealGrn';
  
  let cols = getPaletteColors(paletteName, steps);
  if (!cols || !cols.length) {
    cols = ['#e0f3db','#ccebc5','#a8ddb5','#7bccc4','#4eb3d3','#2b8cbe','#0868ac','#084081'];
  }
  
  if (isReversed) cols = [...cols].reverse();
  
  const leg = document.getElementById('color-legend');
  leg.querySelector('.legend-title').textContent = colorCfg.attr;
  leg.querySelector('.legend-gradient').style.background = `linear-gradient(to right, ${cols.map((c, i) => `${c} ${i / (cols.length - 1) * 100}%`).join(', ')})`;
  leg.querySelector('.legend-min').textContent = d0.toFixed(1);
  leg.querySelector('.legend-max').textContent = d1.toFixed(1);
  leg.style.display = 'block';
}

let layersReady = false;
let autoFitDone = false;
function tryInit() {
  if (layersReady) return;
  const cfg = CONFIG.hexLayer || {};
  const colorCfg = cfg.filled === false ? cfg.getLineColor : cfg.getFillColor;
  const needsCartocolor = colorCfg && colorCfg['@@function'] === 'colorContinuous';
  
  if (needsCartocolor && !window.cartocolor) {
    setTimeout(tryInit, 50);
        return;
      }
      
  layersReady = true;
  addLayers();
  showLegend();
  
  // Auto-fit to data bounds if no custom view specified
  if (!HAS_CUSTOM_VIEW && !autoFitDone && geojson.features.length) {
    const bounds = new mapboxgl.LngLatBounds();
    geojson.features.forEach(f => f.geometry.coordinates[0].forEach(c => bounds.extend(c)));
    if (!bounds.isEmpty()) {
      map.fitBounds(bounds, { padding: 50, maxZoom: 15, duration: 500 });
      autoFitDone = true;
    }
  }
  
  const layer = cfg.extruded ? 'hex-extrusion' : cfg.filled !== false ? 'hex-fill' : 'hex-outline';
  const tt = document.getElementById('tooltip');
  
  map.on('mousemove', layer, (e) => {
    if (!e.features?.length) return;
    map.getCanvas().style.cursor = 'pointer';
    const p = e.features[0].properties;
    const lines = TOOLTIP_COLUMNS.length ? TOOLTIP_COLUMNS.map(k => p[k] != null ? `${k}: ${typeof p[k]==='number'?p[k].toFixed(2):p[k]}` : '').filter(Boolean) : (p.hex ? [`hex: ${p.hex.slice(0,12)}...`] : []);
    if (lines.length) { tt.innerHTML = lines.join(' &bull; '); tt.style.left = `${e.point.x+10}px`; tt.style.top = `${e.point.y+10}px`; tt.style.display = 'block'; }
  });
  
  map.on('mouseleave', layer, () => { map.getCanvas().style.cursor = ''; tt.style.display = 'none'; });
}

map.on('load', tryInit);

// Fix for tiles not loading in iframes
map.on('load', () => { [100, 500, 1000].forEach(t => setTimeout(() => map.resize(), t)); });
window.addEventListener('resize', () => map.resize());
document.addEventListener('visibilitychange', () => { if (!document.hidden) setTimeout(() => map.resize(), 100); });

if (CONFIG_ERRORS?.length) {
  const box = document.getElementById('config-error');
  box.innerHTML = CONFIG_ERRORS.join('<br>');
  box.style.display = 'block';
}

function applyViewState(cfg) {
  const ivs = cfg?.initialViewState || {};
  // Check if user actually specified any view state values
  const hasCustomView = ['longitude','latitude','zoom','pitch','bearing'].some(k => typeof ivs[k] === 'number');
  if (!hasCustomView) return; // Don't override auto-fit if no custom view
  
  const currentCenter = map.getCenter();
  const lng = (typeof ivs.longitude === 'number') ? ivs.longitude : currentCenter.lng;
  const lat = (typeof ivs.latitude === 'number') ? ivs.latitude : currentCenter.lat;
  const zoom = (typeof ivs.zoom === 'number') ? ivs.zoom : map.getZoom();
  const pitch = (typeof ivs.pitch === 'number') ? Math.min(85, Math.max(0, ivs.pitch)) : map.getPitch();
  const bearing = (typeof ivs.bearing === 'number') ? ivs.bearing : map.getBearing();
  map.easeTo({ center: [lng, lat], zoom, pitch, bearing, duration: 500 });
  autoFitDone = true; // Only set after actually applying custom view
}

// Debug Panel - Form controls
if (DEBUG_MODE) {
  const attrSelect = document.getElementById('cfg-attr');
  const numericAttrs = (() => {
    if (!DATA.length) return [];
    const sample = DATA[0];
    return Object.keys(sample).filter(key => typeof sample[key] === 'number' && !['hex','lat','lng'].includes(key));
  })();

  function populateAttrOptions(selected) {
    if (!attrSelect) return;
    attrSelect.innerHTML = '';
    if (!numericAttrs.length) {
      const opt = document.createElement('option');
      opt.value = '';
      opt.textContent = 'No numeric columns';
      attrSelect.appendChild(opt);
      attrSelect.disabled = true;
      return;
    }
    numericAttrs.forEach(attr => {
      const opt = document.createElement('option');
      opt.value = attr;
      opt.textContent = attr;
      attrSelect.appendChild(opt);
    });
    if (selected && numericAttrs.includes(selected)) {
      attrSelect.value = selected;
    } else {
      attrSelect.value = numericAttrs[0];
    }
  }

  const BASE_HEX = JSON.parse(JSON.stringify(CONFIG.hexLayer || {}));

  function setValue(id, value) {
    const el = document.getElementById(id);
    if (!el) return;
    if (value === undefined || value === null || Number.isNaN(value)) {
      el.value = '';
    } else {
      el.value = value;
    }
  }

  function setCheckbox(id, checked) {
    const el = document.getElementById(id);
    if (el) el.checked = Boolean(checked);
  }

  function getNumber(id) {
    const el = document.getElementById(id);
    if (!el) return null;
    const val = parseFloat(el.value);
    return Number.isFinite(val) ? val : null;
  }

  function getCheckbox(id) {
    const el = document.getElementById(id);
    return el ? el.checked : false;
  }

  function updateFormFromConfig(cfg) {
    const ivs = cfg.initialViewState || {};
    setValue('cfg-longitude', ivs.longitude);
    setValue('cfg-latitude', ivs.latitude);
    setValue('cfg-zoom', ivs.zoom);
    setValue('cfg-pitch', ivs.pitch);
    setValue('cfg-bearing', ivs.bearing);

    const hex = cfg.hexLayer || {};
    setCheckbox('cfg-filled', hex.filled !== false);
    setCheckbox('cfg-extruded', hex.extruded === true);
    setCheckbox('cfg-pickable', hex.pickable !== false);
    setValue('cfg-elev-scale', hex.elevationScale ?? 1);

    const color = hex.getFillColor;
    const paletteEl = document.getElementById('cfg-palette');
    if (color && color['@@function'] === 'colorContinuous') {
      populateAttrOptions(color.attr);
      if (attrSelect && color.attr) {
        if (![...attrSelect.options].some(opt => opt.value === color.attr)) {
          const opt = document.createElement('option');
          opt.value = color.attr;
          opt.textContent = color.attr;
          attrSelect.appendChild(opt);
        }
        attrSelect.value = color.attr;
      }
      setValue('cfg-domain-min', color.domain?.[0]);
      setValue('cfg-domain-max', color.domain?.[1]);
      setValue('cfg-steps', color.steps ?? 7);
      if (paletteEl && color.colors) paletteEl.value = color.colors;
      } else {
      populateAttrOptions(numericAttrs[0]);
      setValue('cfg-domain-min', 0);
      setValue('cfg-domain-max', 100);
      setValue('cfg-steps', 7);
      if (paletteEl) paletteEl.value = 'TealGrn';
    }
  }

  function toPython(value, indent = 4, level = 0) {
    const pad = ' '.repeat(indent * level);
    const padNext = ' '.repeat(indent * (level + 1));
    if (value === null) return 'None';
    if (typeof value === 'boolean') return value ? 'True' : 'False';
    if (typeof value === 'number') return String(value);
    if (typeof value === 'string') return JSON.stringify(value);
    if (Array.isArray(value)) {
      if (!value.length) return '[]';
      const items = value.map(v => padNext + toPython(v, indent, level + 1));
      return '[\n' + items.join(',\n') + '\n' + pad + ']';
    }
    if (typeof value === 'object') {
      const keys = Object.keys(value);
      if (!keys.length) return '{}';
      const entries = keys.map(key => padNext + JSON.stringify(key) + ': ' + toPython(value[key], indent, level + 1));
      return '{\n' + entries.join(',\n') + '\n' + pad + '}';
    }
    return 'None';
  }

  function buildConfigFromForm() {
    const cfg = {};
    const ivs = {};
    const lon = getNumber('cfg-longitude');
    const lat = getNumber('cfg-latitude');
    const zoom = getNumber('cfg-zoom');
    const pitch = getNumber('cfg-pitch');
    const bearing = getNumber('cfg-bearing');
    if (lon !== null) ivs.longitude = lon;
    if (lat !== null) ivs.latitude = lat;
    if (zoom !== null) ivs.zoom = zoom;
    if (pitch !== null) ivs.pitch = Math.min(85, Math.max(0, pitch));
    if (bearing !== null) ivs.bearing = bearing;
    if (Object.keys(ivs).length) cfg.initialViewState = ivs;

    const hex = {
      '@@type': 'H3HexagonLayer',
      filled: getCheckbox('cfg-filled'),
      extruded: getCheckbox('cfg-extruded'),
      pickable: getCheckbox('cfg-pickable'),
      elevationScale: getNumber('cfg-elev-scale') ?? 1,
    };

    const attrValue = attrSelect?.value || numericAttrs[0];
    const domainMin = getNumber('cfg-domain-min');
    const domainMax = getNumber('cfg-domain-max');
    const steps = getNumber('cfg-steps') ?? 7;
    const paletteEl = document.getElementById('cfg-palette');
    const palette = paletteEl ? paletteEl.value : 'TealGrn';

    hex.getFillColor = {
      '@@function': 'colorContinuous',
      attr: attrValue,
      domain: [
        domainMin ?? 0,
        domainMax ?? ((domainMin ?? 0) + 1),
      ],
      steps: steps,
      colors: palette,
    };

    const preservedKeys = ['getHexagon', 'getElevation', 'lineWidthMinPixels', 'getLineColor', 'tooltipColumns'];
    preservedKeys.forEach(key => {
      if (BASE_HEX[key] !== undefined && hex[key] === undefined) {
        hex[key] = BASE_HEX[key];
      }
    });
    if (!hex.getHexagon) hex.getHexagon = '@@=properties.hex';
    if (!hex.getElevation) hex.getElevation = BASE_HEX.getElevation || '@@=properties.data_avg';

    cfg.hexLayer = hex;
    return cfg;
  }

  function applyConfig(cfg) {
    CONFIG = JSON.parse(JSON.stringify(cfg));
    applyViewState(CONFIG);
    layersReady = false;
    tryInit();
  }

  // Update config output textarea
  const outputArea = document.getElementById('cfg-output');
  function updateConfigOutput() {
    if (!outputArea) return;
    const cfg = buildConfigFromForm();
    outputArea.value = 'config = ' + toPython(cfg);
  }

  function scheduleApply() {
    clearTimeout(scheduleApply.timer);
    scheduleApply.timer = setTimeout(() => {
      const cfg = buildConfigFromForm();
      applyConfig(cfg);
      updateConfigOutput();
    }, 200);
  }

  // Initial config output
  updateConfigOutput();

  window.resetConfig = function() {
    const original = JSON.parse(JSON.stringify(ORIGINAL_CONFIG));
    updateFormFromConfig(original);
    applyConfig(original);
  };

  populateAttrOptions(ORIGINAL_CONFIG?.hexLayer?.getFillColor?.attr);
  updateFormFromConfig(ORIGINAL_CONFIG);
  
  // Only bind form events - don't call applyConfig on init, let map.on('load') handle initial render
  document.querySelectorAll('#debug-panel [data-cfg-input]').forEach(el => {
    el.addEventListener('input', scheduleApply);
    el.addEventListener('change', scheduleApply);
  });
    }
  </script>
</body>
</html>
""").render(
        mapbox_token=mapbox_token, data_records=data_records, config=merged_config,
        tooltip_columns=tooltip_columns, center_lng=center_lng, center_lat=center_lat,
        zoom=zoom, pitch=pitch, bearing=bearing, config_errors=config_errors, style_url=style_url, debug=debug,
        user_config=original_config if original_config else merged_config,
        has_custom_view=has_custom_view, palettes=sorted(KNOWN_CARTOCOLOR_PALETTES),
    )

    common = fused.load("https://github.com/fusedio/udfs/tree/f430c25/public/common/")
    return common.html_to_obj(html)


def _deckgl_hex_multi(
    layers: list,
    mapbox_token: str = "pk.eyJ1IjoiaXNhYWNmdXNlZGxhYnMiLCJhIjoiY2xicGdwdHljMHQ1bzN4cWhtNThvbzdqcSJ9.73fb6zHMeO_c8eAXpZVNrA",
    basemap: str = "dark",
):
    """
    Render multiple H3 hexagon layers with a layer toggle panel.
    
    Args:
        layers: List of layer dicts, each with:
            - "data": DataFrame with hex column
            - "config": hexLayer config for this layer
            - "name": Display name for layer toggle (optional)
        mapbox_token: Mapbox access token.
        basemap: 'dark', 'satellite', or custom Mapbox style URL.
    """
    # Basemap setup
    basemap_styles = {"dark": "mapbox://styles/mapbox/dark-v11", "satellite": "mapbox://styles/mapbox/satellite-streets-v12", "light": "mapbox://styles/mapbox/light-v11", "streets": "mapbox://styles/mapbox/streets-v12"}
    style_url = basemap_styles.get({"sat": "satellite", "satellite-streets": "satellite"}.get((basemap or "dark").lower(), (basemap or "dark").lower()), basemap or basemap_styles["dark"])

    # Process each layer
    processed_layers = []
    all_bounds = []
    
    for i, layer_def in enumerate(layers):
        df = layer_def.get("data")
        config = layer_def.get("config", {})
        name = layer_def.get("name", f"Layer {i + 1}")
        
        # Config processing
        config_errors = []
        original_config = deepcopy(config) if config else {}
        merged_config = _load_deckgl_config(config, DEFAULT_DECK_HEX_CONFIG, f"deckgl_hex_layer_{i}", config_errors)
        hex_layer = merged_config.get("hexLayer", {})
        
        # Validate hexLayer property names
        invalid_props = [key for key in list(hex_layer.keys()) if key not in VALID_HEX_LAYER_PROPS]
        for prop in invalid_props:
            suggestion = get_close_matches(prop, list(VALID_HEX_LAYER_PROPS), n=1, cutoff=0.6)
            if suggestion:
                print(f"[deckgl_hex] Warning: Layer '{name}' property '{prop}' not recognized. Did you mean '{suggestion[0]}'?")
            hex_layer.pop(prop, None)
        
        # Convert dataframe to records
        data_records = []
        if hasattr(df, 'to_dict'):
            data_records = df.to_dict('records')
            for record in data_records:
                hex_val = record.get('hex') or record.get('h3') or record.get('index') or record.get('id')
                if hex_val is not None:
                    try:
                        if isinstance(hex_val, (int, float)):
                            record['hex'] = format(int(hex_val), 'x')
                        elif isinstance(hex_val, str) and hex_val.isdigit():
                            record['hex'] = format(int(hex_val), 'x')
                        else:
                            record['hex'] = hex_val
                    except (ValueError, OverflowError):
                        record['hex'] = None
        
        # Extract tooltip columns
        tooltip_columns = _extract_tooltip_columns((merged_config, hex_layer))
        if not tooltip_columns and data_records:
            tooltip_columns = [k for k in data_records[0].keys() if k not in ['hex', 'lat', 'lng']]
        
        processed_layers.append({
            "id": f"layer-{i}",
            "name": name,
            "data": data_records,
            "config": merged_config,
            "hexLayer": hex_layer,
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
        for key in ('longitude', 'latitude', 'zoom', 'pitch', 'bearing')
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
  <script type="module">
    import * as cartocolor from 'https://esm.sh/cartocolor@5.0.2';
    window.cartocolor = cartocolor;
  </script>
  <style>
    html, body { margin:0; height:100%; width:100%; display:flex; overflow:hidden; }
    #map { flex:1; height:100%; }
    #tooltip { position:absolute; pointer-events:none; background:rgba(0,0,0,0.7); color:#fff; padding:4px 8px; border-radius:4px; font-size:12px; display:none; z-index:6; }
    
    /* Layer Toggle Panel */
    #layer-panel {
      position: fixed;
      top: 12px;
      right: 12px;
      background: rgba(26, 26, 26, 0.95);
      border: 1px solid #424242;
      border-radius: 8px;
      padding: 12px;
      font-family: Inter, 'SF Pro Display', 'Segoe UI', sans-serif;
      color: #f5f5f5;
      min-width: 180px;
      z-index: 100;
      box-shadow: 0 4px 12px rgba(0,0,0,0.3);
    }
    #layer-panel h4 {
      margin: 0 0 10px 0;
      font-size: 11px;
      letter-spacing: 0.4px;
      text-transform: uppercase;
      color: #E8FF59;
      font-weight: 600;
    }
    .layer-item {
      display: flex;
      align-items: center;
      gap: 10px;
      padding: 8px 0;
      border-bottom: 1px solid #333;
    }
    .layer-item:last-child { border-bottom: none; }
    .layer-item input[type="checkbox"] {
      width: 16px;
      height: 16px;
      cursor: pointer;
      accent-color: #E8FF59;
    }
    .layer-item .layer-color {
      width: 14px;
      height: 14px;
      border-radius: 3px;
      border: 1px solid rgba(255,255,255,0.2);
    }
    .layer-item .layer-name {
      flex: 1;
      font-size: 12px;
      color: #dcdcdc;
    }
    .layer-item.disabled .layer-name {
      color: #666;
      text-decoration: line-through;
    }
    
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
    .legend-layer { margin-bottom: 12px; }
    .legend-layer:last-child { margin-bottom: 0; }
    .legend-layer .legend-title { margin-bottom: 6px; font-weight: 500; display: flex; align-items: center; gap: 6px; }
    .legend-layer .legend-title .legend-dot { width: 8px; height: 8px; border-radius: 2px; }
    .legend-layer .legend-gradient { height: 10px; border-radius: 2px; margin-bottom: 4px; border: 1px solid rgba(255,255,255,0.2); }
    .legend-layer .legend-labels { display: flex; justify-content: space-between; font-size: 10px; color: #ccc; }
  </style>
</head>
<body>
  <div id="map"></div>
  <div id="tooltip"></div>
  
  <!-- Layer Toggle Panel -->
  <div id="layer-panel">
    <h4>Layers</h4>
    <div id="layer-list"></div>
  </div>
  
  <!-- Legend -->
  <div id="color-legend" class="color-legend" style="display:none;"></div>

  <script>
    const MAPBOX_TOKEN = {{ mapbox_token | tojson }};
    const LAYERS_DATA = {{ layers_data | tojson }};
    const HAS_CUSTOM_VIEW = {{ has_custom_view | tojson }};
    
    // Track layer visibility
    const layerVisibility = {};
    LAYERS_DATA.forEach(l => { layerVisibility[l.id] = l.visible; });

    // H3 ID conversion
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

    // Convert to GeoJSON
    function toGeoJSON(data) {
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

    // Get palette colors from cartocolor
    function getPaletteColors(name, steps) {
      const pal = window.cartocolor?.[name];
      if (!pal) return null;
      const keys = Object.keys(pal).map(Number).filter(n => !isNaN(n)).sort((a, b) => a - b);
      const best = keys.find(n => n >= steps) || keys[keys.length - 1];
      return pal[best] ? [...pal[best]] : null;
    }

    // Build color expression
    function buildColorExpr(cfg) {
      if (!cfg || cfg['@@function'] !== 'colorContinuous' || !cfg.attr || !cfg.domain?.length) return 'rgba(0,144,255,0.7)';
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

    // Convert [r,g,b] or [r,g,b,a] array to rgba string
    function toRgba(arr, defaultAlpha) {
      if (!Array.isArray(arr) || arr.length < 3) return null;
      const [r, g, b] = arr;
      const a = arr.length >= 4 ? arr[3] / 255 : (defaultAlpha ?? 1);
      return `rgba(${r},${g},${b},${a})`;
    }

    // Precompute GeoJSON for each layer
    const layerGeoJSONs = {};
    LAYERS_DATA.forEach(l => {
      layerGeoJSONs[l.id] = toGeoJSON(l.data);
    });

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

    function addAllLayers() {
      // Remove existing layers
      LAYERS_DATA.forEach(l => {
        [`${l.id}-fill`, `${l.id}-extrusion`, `${l.id}-outline`].forEach(id => { 
          try { if(map.getLayer(id)) map.removeLayer(id); } catch(e){} 
        });
        try { if(map.getSource(l.id)) map.removeSource(l.id); } catch(e){}
      });
      
      // Add layers in reverse menu order so top of menu renders on top of map
      const renderOrder = [...LAYERS_DATA].reverse();
      renderOrder.forEach((l) => {
        const geojson = layerGeoJSONs[l.id];
        map.addSource(l.id, { type: 'geojson', data: geojson });
        
        const cfg = l.hexLayer || {};
        const fillColor = Array.isArray(cfg.getFillColor) ? toRgba(cfg.getFillColor, 0.8) : buildColorExpr(cfg.getFillColor);
        const lineColor = cfg.getLineColor ? (Array.isArray(cfg.getLineColor) ? toRgba(cfg.getLineColor, 1) : buildColorExpr(cfg.getLineColor)) : 'rgba(255,255,255,0.3)';
        const layerOpacity = (typeof cfg.opacity === 'number' && isFinite(cfg.opacity)) ? Math.max(0, Math.min(1, cfg.opacity)) : 0.8;
        const visible = layerVisibility[l.id];
        
        if (cfg.extruded) {
          // 3D extrusion mode
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
          map.addLayer({ 
            id: `${l.id}-outline`, 
            type: 'line', 
            source: l.id, 
            paint: { 'line-color': lineColor, 'line-width': cfg.lineWidthMinPixels || 0.5 },
            layout: { 'visibility': visible ? 'visible' : 'none' }
          });
        } else {
          // Flat 2D mode
          if (cfg.filled !== false) {
            map.addLayer({ 
              id: `${l.id}-fill`, 
              type: 'fill', 
              source: l.id, 
              paint: { 'fill-color': fillColor, 'fill-opacity': layerOpacity },
              layout: { 'visibility': visible ? 'visible' : 'none' }
            });
          }
          map.addLayer({ 
            id: `${l.id}-outline`, 
            type: 'line', 
            source: l.id, 
            paint: { 'line-color': lineColor, 'line-width': cfg.lineWidthMinPixels || 0.5 },
            layout: { 'visibility': visible ? 'visible' : 'none' }
          });
        }
      });
    }

    function toggleLayerVisibility(layerId, visible) {
      layerVisibility[layerId] = visible;
      [`${layerId}-fill`, `${layerId}-extrusion`, `${layerId}-outline`].forEach(id => {
        try { 
          if(map.getLayer(id)) {
            map.setLayoutProperty(id, 'visibility', visible ? 'visible' : 'none');
          }
        } catch(e){}
      });
      updateLegend();
      updateLayerPanel();
    }

    function updateLayerPanel() {
      const list = document.getElementById('layer-list');
      if (!list) return;
      
      list.innerHTML = LAYERS_DATA.map(l => {
        const visible = layerVisibility[l.id];
        const cfg = l.hexLayer || {};
        const fillCfg = cfg.getFillColor;
        let colorPreview = '#0090ff';
        if (Array.isArray(fillCfg)) {
          colorPreview = toRgba(fillCfg, 1) || colorPreview;
        } else if (fillCfg && fillCfg['@@function'] === 'colorContinuous') {
          const cols = getPaletteColors(fillCfg.colors || 'TealGrn', fillCfg.steps || 7);
          if (cols && cols.length) colorPreview = cols[Math.floor(cols.length / 2)];
        }
        
        return `
          <div class="layer-item ${visible ? '' : 'disabled'}">
            <input type="checkbox" ${visible ? 'checked' : ''} onchange="toggleLayerVisibility('${l.id}', this.checked)" />
            <div class="layer-color" style="background:${colorPreview};"></div>
            <span class="layer-name">${l.name}</span>
          </div>
        `;
      }).join('');
    }

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
        const cfg = l.hexLayer || {};
        const colorCfg = cfg.getFillColor;
        if (!colorCfg || colorCfg['@@function'] !== 'colorContinuous' || !colorCfg.attr || !colorCfg.domain?.length) return;
        
        const [d0, d1] = colorCfg.domain;
        const isReversed = d0 > d1;
        const steps = colorCfg.steps || 7;
        const paletteName = colorCfg.colors || 'TealGrn';
        
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

    let layersReady = false;
    let autoFitDone = false;
    
    function tryInit() {
      if (layersReady) return;
      
      // Check if cartocolor is loaded (needed for color interpolation)
      const needsCartocolor = LAYERS_DATA.some(l => {
        const cfg = l.hexLayer || {};
        const colorCfg = cfg.getFillColor;
        return colorCfg && colorCfg['@@function'] === 'colorContinuous';
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
          const geojson = layerGeoJSONs[l.id];
          if (geojson.features.length) {
            geojson.features.forEach(f => f.geometry.coordinates[0].forEach(c => bounds.extend(c)));
          }
        });
        if (!bounds.isEmpty()) {
          map.fitBounds(bounds, { padding: 50, maxZoom: 15, duration: 500 });
          autoFitDone = true;
        }
      }
      
      // Setup tooltips for all layers
      const tt = document.getElementById('tooltip');
      LAYERS_DATA.forEach(l => {
        const cfg = l.hexLayer || {};
        // Include extrusion layer for 3D mode
        const layerIds = cfg.extruded 
          ? [`${l.id}-extrusion`, `${l.id}-outline`]
          : [`${l.id}-fill`, `${l.id}-outline`];
        
        layerIds.forEach(layerId => {
          if (!map.getLayer(layerId)) return;
          
          map.on('mousemove', layerId, (e) => {
            if (!e.features?.length || !layerVisibility[l.id]) return;
            map.getCanvas().style.cursor = 'pointer';
            const p = e.features[0].properties;
            const cols = l.tooltipColumns || [];
            const lines = cols.length 
              ? cols.map(k => p[k] != null ? `${k}: ${typeof p[k]==='number'?p[k].toFixed(2):p[k]}` : '').filter(Boolean) 
              : (p.hex ? [`hex: ${p.hex.slice(0,12)}...`] : []);
            if (lines.length) { 
              tt.innerHTML = `<strong>${l.name}</strong><br>` + lines.join(' &bull; '); 
              tt.style.left = `${e.point.x+10}px`; 
              tt.style.top = `${e.point.y+10}px`; 
              tt.style.display = 'block'; 
            }
          });
          
          map.on('mouseleave', layerId, () => { 
            map.getCanvas().style.cursor = ''; 
            tt.style.display = 'none'; 
          });
        });
      });
    }

    map.on('load', tryInit);
    map.on('load', () => { [100, 500, 1000].forEach(t => setTimeout(() => map.resize(), t)); });
    window.addEventListener('resize', () => map.resize());
    document.addEventListener('visibilitychange', () => { if (!document.hidden) setTimeout(() => map.resize(), 100); });
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
    )

    common = fused.load("https://github.com/fusedio/udfs/tree/f430c25/public/common/")
    return common.html_to_obj(html)


def _deckgl_hex_tiles(
    tile_url: str,
    config=None,
    mapbox_token: str = "pk.eyJ1IjoiaXNhYWNmdXNlZGxhYnMiLCJhIjoiY2xicGdwdHljMHQ1bzN4cWhtNThvbzdqcSJ9.73fb6zHMeO_c8eAXpZVNrA",
    basemap: str = "dark",
    debug: bool = False,
):
    """
    Internal: Render H3 hexagon tiles from an XYZ tile URL using Deck.gl TileLayer + H3HexagonLayer.
    Called by deckgl_hex when tile_url is provided.
    """
    # Basemap setup
    basemap_styles = {"dark": "mapbox://styles/mapbox/dark-v11", "satellite": "mapbox://styles/mapbox/satellite-streets-v12", "light": "mapbox://styles/mapbox/light-v11", "streets": "mapbox://styles/mapbox/streets-v12"}
    style_url = basemap_styles.get({"sat": "satellite", "satellite-streets": "satellite"}.get((basemap or "dark").lower(), (basemap or "dark").lower()), basemap or basemap_styles["dark"])

    # Config processing
    config_errors = []
    original_config = deepcopy(config) if config else {}
    merged_config = _load_deckgl_config(config, DEFAULT_DECK_HEX_CONFIG, "deckgl_hex", config_errors)
    hex_layer = merged_config.get("hexLayer", {})
    tile_layer = merged_config.get("tileLayer", {})

    # Validate hexLayer property names
    invalid_props = [key for key in list(hex_layer.keys()) if key not in VALID_HEX_LAYER_PROPS]
    for prop in invalid_props:
        suggestion = get_close_matches(prop, list(VALID_HEX_LAYER_PROPS), n=1, cutoff=0.6)
        if suggestion:
            config_errors.append(f"Property '{prop}' not recognized. Did you mean '{suggestion[0]}'?")
        else:
            config_errors.append(f"Property '{prop}' not recognized.")
        hex_layer.pop(prop, None)

    # Validate palette name for getFillColor
    fill_color_cfg = hex_layer.get("getFillColor", {})
    if isinstance(fill_color_cfg, dict) and fill_color_cfg.get("@@function") == "colorContinuous":
        palette_name = fill_color_cfg.get("colors", "TealGrn")
        if palette_name and palette_name not in KNOWN_CARTOCOLOR_PALETTES:
            suggestion = get_close_matches(palette_name, list(KNOWN_CARTOCOLOR_PALETTES), n=1, cutoff=0.5)
            if suggestion:
                config_errors.append(f"Palette '{palette_name}' not found. Did you mean '{suggestion[0]}'?")
            else:
                config_errors.append(f"Palette '{palette_name}' not found.")

    # View state
    ivs = merged_config.get('initialViewState', {})
    center_lng = ivs.get('longitude') or -119.4179
    center_lat = ivs.get('latitude') or 36.7783
    zoom = ivs.get('zoom') or 5
    pitch = ivs.get('pitch', 0)
    bearing = ivs.get('bearing', 0)

    # Tooltip columns
    tooltip_columns = _extract_tooltip_columns((merged_config, hex_layer))

    html = Template(r"""
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <link href="https://api.mapbox.com/mapbox-gl-js/v3.2.0/mapbox-gl.css" rel="stylesheet" />
  <script src="https://api.mapbox.com/mapbox-gl-js/v3.2.0/mapbox-gl.js"></script>
  <script src="https://unpkg.com/h3-js@4.1.0/dist/h3-js.umd.js"></script>
  <script src="https://unpkg.com/deck.gl@9.1.3/dist.min.js"></script>
  <script src="https://unpkg.com/@deck.gl/geo-layers@9.1.3/dist.min.js"></script>
  <script src="https://unpkg.com/@deck.gl/carto@9.1.3/dist.min.js"></script>
  <script type="module">
    import * as cartocolor from 'https://esm.sh/cartocolor@5.0.2';
    window.cartocolor = cartocolor;
  </script>
  <style>
    html, body { margin:0; height:100%; width:100%; display:flex; overflow:hidden; }
    #map { flex:1; height:100%; }
    #tooltip { position:absolute; pointer-events:none; background:rgba(0,0,0,0.7); color:#fff; padding:4px 8px; border-radius:4px; font-size:12px; display:none; z-index:6; }
    .config-error { position:fixed; right:12px; bottom:12px; background:rgba(180,30,30,0.92); color:#fff; padding:6px 10px; border-radius:4px; font-size:11px; display:none; z-index:8; max-width:260px; }
    .color-legend { position:fixed; left:12px; bottom:12px; background:rgba(15,15,15,0.9); color:#fff; padding:8px; border-radius:4px; font-size:11px; display:none; z-index:10; min-width:140px; border:1px solid rgba(255,255,255,0.1); }
    .color-legend .legend-title { margin-bottom:6px; font-weight:500; }
    .color-legend .legend-gradient { height:12px; border-radius:2px; margin-bottom:4px; border:1px solid rgba(255,255,255,0.2); }
    .color-legend .legend-labels { display:flex; justify-content:space-between; font-size:10px; color:#ccc; }
    /* Debug Panel */
    #debug-panel { width:400px; height:100%; background:#212121; border-left:1px solid #424242; display:flex; flex-direction:column; font-family:Inter, 'SF Pro Display', 'Segoe UI', sans-serif; color:#f5f5f5; }
    #debug-header { padding:12px 18px; background:#1a1a1a; border-bottom:1px solid #424242; }
    #debug-header h3 { margin:0; font-size:12px; font-weight:600; color:#E8FF59; letter-spacing:0.5px; text-transform:uppercase; }
    #debug-content { flex:1; overflow-y:auto; padding:14px 18px; display:flex; flex-direction:column; gap:16px; }
    .debug-section { background:#1a1a1a; border:1px solid #424242; border-radius:8px; padding:12px; }
    .debug-section h4 { margin:0 0 10px 0; font-size:11px; letter-spacing:0.4px; text-transform:uppercase; color:#bdbdbd; }
    .form-grid { display:grid; grid-template-columns:repeat(2, minmax(0,1fr)); gap:10px; }
    .form-control { display:flex; flex-direction:column; gap:4px; font-size:11px; color:#dcdcdc; }
    .form-control label { font-weight:600; }
    .form-control input, .form-control select { background:#111; border:1px solid #333; border-radius:4px; padding:6px 8px; font-size:12px; color:#f5f5f5; outline:none; }
    .form-control input:focus, .form-control select:focus { border-color:#E8FF59; }
    .toggle-row { display:flex; align-items:center; gap:8px; font-size:11px; }
    .toggle-row input { width:16px; height:16px; }
    .single-column { display:flex; flex-direction:column; gap:10px; }
    #debug-buttons { display:flex; gap:8px; padding:12px 18px; background:#1a1a1a; border-top:1px solid #424242; }
    .dbtn { flex:1; border:none; border-radius:4px; padding:10px; font-size:11px; font-weight:600; cursor:pointer; text-transform:uppercase; letter-spacing:0.5px; }
    .dbtn.secondary { background:#424242; color:#fff; }
    .dbtn.secondary:hover { background:#616161; }
    .dbtn.ghost { background:transparent; color:#E8FF59; border:1px solid rgba(232,255,89,0.3); }
    .dbtn.ghost:hover { border-color:#E8FF59; }
    #cfg-output { width:100%; min-height:120px; resize:vertical; background:#111; color:#f5f5f5; border:1px solid #333; border-radius:6px; padding:10px; font-family:SFMono-Regular,Consolas,monospace; font-size:11px; line-height:1.4; }
  </style>
</head>
<body>
  <div id="map"></div>
  <div id="tooltip"></div>
  <div id="config-error" class="config-error"></div>
  <div id="color-legend" class="color-legend"><div class="legend-title"></div><div class="legend-gradient"></div><div class="legend-labels"><span class="legend-min"></span><span class="legend-max"></span></div></div>
  
  {% if debug %}
  <div id="debug-panel">
    <div id="debug-header">
      <h3>Config (Tile Mode)</h3>
    </div>
    <div id="debug-content">
      <div class="debug-section">
        <h4>View</h4>
        <div class="form-grid">
          <div class="form-control">
            <label>Longitude</label>
            <input type="number" id="cfg-longitude" step="0.0001" value="{{ center_lng }}" />
          </div>
          <div class="form-control">
            <label>Latitude</label>
            <input type="number" id="cfg-latitude" step="0.0001" value="{{ center_lat }}" />
          </div>
          <div class="form-control">
            <label>Zoom</label>
            <input type="number" id="cfg-zoom" step="0.1" min="0" value="{{ zoom }}" />
          </div>
          <div class="form-control">
            <label>Pitch</label>
            <input type="number" id="cfg-pitch" step="1" min="0" max="85" value="{{ pitch }}" />
          </div>
          <div class="form-control">
            <label>Bearing</label>
            <input type="number" id="cfg-bearing" step="1" value="{{ bearing }}" />
          </div>
        </div>
      </div>
      <div class="debug-section">
        <h4>Fill Color</h4>
        <div class="form-grid">
          <div class="form-control">
            <label>Attribute</label>
            <input type="text" id="cfg-attr" value="{{ color_attr }}" />
          </div>
          <div class="form-control">
            <label>Palette</label>
            <select id="cfg-palette">
              {% for pal in palettes %}<option value="{{ pal }}" {% if pal == palette_name %}selected{% endif %}>{{ pal }}</option>
              {% endfor %}
            </select>
          </div>
          <div class="form-control">
            <label>Domain Min</label>
            <input type="number" id="cfg-domain-min" step="0.1" value="{{ domain_min }}" />
          </div>
          <div class="form-control">
            <label>Domain Max</label>
            <input type="number" id="cfg-domain-max" step="0.1" value="{{ domain_max }}" />
          </div>
          <div class="form-control">
            <label>Steps</label>
            <input type="number" id="cfg-steps" min="2" max="20" value="{{ steps }}" />
          </div>
        </div>
      </div>
      <div class="debug-section">
        <h4>Config Output</h4>
        <textarea id="cfg-output" readonly></textarea>
      </div>
    </div>
    <div id="debug-buttons">
      <button class="dbtn secondary" onclick="resetConfig()">Reset</button>
    </div>
  </div>
  {% endif %}

  <script>
    const MAPBOX_TOKEN = {{ mapbox_token | tojson }};
    const TILE_URL = {{ tile_url | tojson }};
    const CONFIG = {{ config | tojson }};
    const TOOLTIP_COLUMNS = {{ tooltip_columns | tojson }};
    const CONFIG_ERRORS = {{ config_errors | tojson }};
    const DEBUG_MODE = {{ debug | tojson }};

    const { TileLayer, PolygonLayer, MapboxOverlay } = deck;
    const H3HexagonLayer = deck.H3HexagonLayer || (deck.GeoLayers && deck.GeoLayers.H3HexagonLayer);
    const { colorContinuous } = deck.carto;

    const $tooltip = () => document.getElementById('tooltip');

    // @@= expression evaluator
    function evalExpression(expr, object) {
      if (typeof expr === 'string' && expr.startsWith('@@=')) {
        const code = expr.slice(3);
        try {
          const fn = new Function('object', `const properties = object?.properties || object || {}; return (${code});`);
          return fn(object);
        } catch (e) { return null; }
      }
      return expr;
    }

    // colorContinuous domain expansion
    function processColorContinuous(cfg) {
      let domain = cfg.domain;
      if (domain && domain.length === 2) {
        const [min, max] = domain;
        const steps = cfg.steps ?? 20;
        const stepSize = (max - min) / (steps - 1);
        domain = Array.from({ length: steps }, (_, i) => min + stepSize * i);
      }
      return { attr: cfg.attr, domain, colors: cfg.colors || 'TealGrn', nullColor: cfg.nullColor || [184, 184, 184] };
    }

    function parseHexLayerConfig(config) {
      const out = {};
      for (const [k, v] of Object.entries(config || {})) {
        if (k === '@@type') continue;
        if (v && typeof v === 'object' && !Array.isArray(v)) {
          if (v['@@function'] === 'colorContinuous') {
            out[k] = colorContinuous(processColorContinuous(v));
          } else {
            out[k] = v;
          }
        } else if (typeof v === 'string' && v.startsWith('@@=')) {
          out[k] = (obj) => evalExpression(v, obj);
        } else {
          out[k] = v;
        }
      }
      return out;
    }

    // H3 ID safety (handles string/number/bigint/[hi,lo])
    function toH3String(hex) {
      try {
        if (hex == null) return null;
        if (typeof hex === 'string') {
          const s = hex.startsWith('0x') ? hex.slice(2) : hex;
          return (/^\d+$/.test(s) ? BigInt(s).toString(16) : s.toLowerCase());
        }
        if (typeof hex === 'number') return BigInt(Math.trunc(hex)).toString(16);
        if (typeof hex === 'bigint') return hex.toString(16);
        if (Array.isArray(hex) && hex.length === 2) {
          const a = (BigInt(hex[0]) << 32n) | BigInt(hex[1]);
          const b = (BigInt(hex[1]) << 32n) | BigInt(hex[0]);
          const sa = a.toString(16), sb = b.toString(16);
          if (h3.isValidCell?.(sa)) return sa;
          if (h3.isValidCell?.(sb)) return sb;
          return sa;
        }
      } catch (_) {}
      return null;
    }

    // Normalize tile JSON to data rows
    function normalize(raw) {
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

    // Mapbox init
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

    // Fetch tile data
    async function getTileData({ index, signal }) {
      const { x, y, z } = index;
      const url = TILE_URL.replace('{z}', z).replace('{x}', x).replace('{y}', y);
      try {
        const res = await fetch(url, { signal });
        if (!res.ok) throw new Error(String(res.status));
        let text = await res.text();
        text = text.replace(/\"(hex|h3|index)\"\s*:\s*(\d+)/gi, (_m, k, d) => `"${k}":"${d}"`);
        const data = JSON.parse(text);
        const out = normalize(data);
        return out;
      } catch (e) {
        return [];
      }
    }

    // Build layers
    const tileCfg = CONFIG.tileLayer || {};
    const hexCfg = parseHexLayerConfig(CONFIG.hexLayer || {});
    const colorAttr = CONFIG.hexLayer?.getFillColor?.attr || 'metric';

    // Get palette colors from cartocolor
    function getPaletteColors(name, steps) {
      const pal = window.cartocolor?.[name];
      if (!pal) return null;
      const keys = Object.keys(pal).map(Number).filter(n => !isNaN(n)).sort((a, b) => a - b);
      const best = keys.find(n => n >= steps) || keys[keys.length - 1];
      return pal[best] ? [...pal[best]] : null;
    }

    // Build legend gradient from palette
    function updateLegend(attr, domain, paletteName, steps) {
      const leg = document.getElementById('color-legend');
      if (!leg) return;

      const [d0, d1] = domain || [0, 100];
      const isReversed = d0 > d1;
      let cols = getPaletteColors(paletteName, steps || 7);
      
      // Fallback colors if palette not found
      if (!cols || !cols.length) {
        cols = ['#e0f3db','#ccebc5','#a8ddb5','#7bccc4','#4eb3d3','#2b8cbe','#0868ac','#084081'];
      }
      
      if (isReversed) cols = [...cols].reverse();

      leg.querySelector('.legend-title').textContent = attr;
      leg.querySelector('.legend-gradient').style.background = `linear-gradient(to right, ${cols.map((c, i) => `${c} ${i / (cols.length - 1) * 100}%`).join(', ')})`;
      leg.querySelector('.legend-min').textContent = d0.toFixed(1);
      leg.querySelector('.legend-max').textContent = d1.toFixed(1);
      leg.style.display = 'block';
    }

    // Initial legend setup (wait for cartocolor to load)
    function initLegend() {
      const colorCfg = CONFIG.hexLayer?.getFillColor;
      if (!colorCfg || colorCfg['@@function'] !== 'colorContinuous') return;
      
      if (!window.cartocolor) {
        setTimeout(initLegend, 50);
        return;
      }
      
      updateLegend(
        colorCfg.attr || 'metric',
        colorCfg.domain || [0, 100],
        colorCfg.colors || 'Magenta',
        colorCfg.steps || 20
      );
    }
    initLegend();

    // Current hex layer config (mutable for debug panel)
    let currentHexCfg = hexCfg;
    let currentColorAttr = colorAttr;

    function buildTileLayer() {
      return new TileLayer({
        id: 'hex-tiles-' + Date.now(),
        data: TILE_URL,
        tileSize: tileCfg.tileSize ?? 256,
        minZoom: tileCfg.minZoom ?? 0,
        maxZoom: tileCfg.maxZoom ?? 12,
        pickable: tileCfg.pickable ?? true,
        maxRequests: 6,
        // Keep tiles visible when zoomed past maxZoom
        extent: null,
        zoomOffset: 0,
        refinementStrategy: 'best-available',
        getTileData,
        renderSubLayers: (props) => {
          const data = props.data || [];
          if (!data.length) return null;
          if (H3HexagonLayer) {
            return new H3HexagonLayer({
              id: `${props.id}-h3`,
              data,
              pickable: true, stroked: false, filled: true, extruded: false,
              coverage: 0.9, lineWidthMinPixels: 1,
              getHexagon: d => d.hex,
              ...currentHexCfg
            });
          }
          // Fallback: PolygonLayer
          const polys = data.map(d => {
            const ring = h3.cellToBoundary(d.hex, true).map(([lat, lng]) => [lng, lat]);
            return { ...d, polygon: ring };
          });
          return new PolygonLayer({
            id: `${props.id}-poly-fallback`,
            data: polys,
            pickable: true, stroked: true, filled: true, extruded: false,
            getPolygon: d => d.polygon,
            getFillColor: [184, 184, 184, 220],
            getLineColor: [200, 200, 200, 255],
            lineWidthMinPixels: 1
          });
        }
      });
    }

    let overlay = new MapboxOverlay({
      interleaved: false,
      layers: [buildTileLayer()]
    });
    map.addControl(overlay);

    // Function to rebuild overlay with new config
    function rebuildOverlay(newHexCfg, newColorAttr) {
      currentHexCfg = newHexCfg;
      currentColorAttr = newColorAttr || currentColorAttr;
      overlay.setProps({ layers: [buildTileLayer()] });
    }

    // Tooltip on hover
    map.on('mousemove', (e) => {
      const info = overlay.pickObject({ x: e.point.x, y: e.point.y, radius: 4 });
      if (info?.object) {
        map.getCanvas().style.cursor = 'pointer';
        const p = info.object;
        const lines = [`hex: ${p.hex?.slice(0, 12)}...`];
        if (TOOLTIP_COLUMNS?.length) {
          TOOLTIP_COLUMNS.forEach(col => {
            if (p[col] !== undefined) {
              const val = p[col];
              lines.push(`${col}: ${typeof val === 'number' ? val.toFixed(2) : val}`);
            }
          });
        } else {
          const val = p[currentColorAttr] ?? p.metric;
          if (val != null) lines.push(`${currentColorAttr}: ${Number(val).toFixed(2)}`);
        }
        const tt = $tooltip();
        tt.innerHTML = lines.join(' &bull; ');
        tt.style.left = `${e.point.x + 10}px`;
        tt.style.top = `${e.point.y + 10}px`;
        tt.style.display = 'block';
      } else {
        map.getCanvas().style.cursor = '';
        $tooltip().style.display = 'none';
      }
    });

    // Config errors
    if (CONFIG_ERRORS?.length) {
      const box = document.getElementById('config-error');
      box.innerHTML = CONFIG_ERRORS.join('<br>');
      box.style.display = 'block';
    }

    // Debug panel functions
    if (DEBUG_MODE) {
      function toPython(value, indent = 4, level = 0) {
        const pad = ' '.repeat(indent * level);
        const padNext = ' '.repeat(indent * (level + 1));
        if (value === null) return 'None';
        if (typeof value === 'boolean') return value ? 'True' : 'False';
        if (typeof value === 'number') return String(value);
        if (typeof value === 'string') return JSON.stringify(value);
        if (Array.isArray(value)) {
          if (!value.length) return '[]';
          const items = value.map(v => padNext + toPython(v, indent, level + 1));
          return '[\n' + items.join(',\n') + '\n' + pad + ']';
        }
        if (typeof value === 'object') {
          const keys = Object.keys(value);
          if (!keys.length) return '{}';
          const entries = keys.map(key => padNext + JSON.stringify(key) + ': ' + toPython(value[key], indent, level + 1));
          return '{\n' + entries.join(',\n') + '\n' + pad + '}';
        }
        return 'None';
      }

      function buildConfigFromForm() {
        const cfg = { initialViewState: {}, hexLayer: { '@@type': 'H3HexagonLayer' } };
        const lon = parseFloat(document.getElementById('cfg-longitude')?.value);
        const lat = parseFloat(document.getElementById('cfg-latitude')?.value);
        const zoom = parseFloat(document.getElementById('cfg-zoom')?.value);
        const pitch = parseFloat(document.getElementById('cfg-pitch')?.value);
        const bearing = parseFloat(document.getElementById('cfg-bearing')?.value);
        if (!isNaN(lon)) cfg.initialViewState.longitude = lon;
        if (!isNaN(lat)) cfg.initialViewState.latitude = lat;
        if (!isNaN(zoom)) cfg.initialViewState.zoom = zoom;
        if (!isNaN(pitch)) cfg.initialViewState.pitch = pitch;
        if (!isNaN(bearing)) cfg.initialViewState.bearing = bearing;

        const attr = document.getElementById('cfg-attr')?.value || 'metric';
        const palette = document.getElementById('cfg-palette')?.value || 'Magenta';
        const domainMin = parseFloat(document.getElementById('cfg-domain-min')?.value) || 0;
        const domainMax = parseFloat(document.getElementById('cfg-domain-max')?.value) || 100;
        const steps = parseInt(document.getElementById('cfg-steps')?.value) || 20;

        cfg.hexLayer.getFillColor = {
          '@@function': 'colorContinuous',
          attr, domain: [domainMin, domainMax], steps, colors: palette
        };
        return cfg;
      }

      // Build new hexCfg from form and apply to overlay
      function applyConfigFromForm() {
        const attr = document.getElementById('cfg-attr')?.value || 'metric';
        const palette = document.getElementById('cfg-palette')?.value || 'Magenta';
        const domainMin = parseFloat(document.getElementById('cfg-domain-min')?.value) || 0;
        const domainMax = parseFloat(document.getElementById('cfg-domain-max')?.value) || 100;
        const steps = parseInt(document.getElementById('cfg-steps')?.value) || 20;

        // Build colorContinuous config
        const colorCfg = {
          '@@function': 'colorContinuous',
          attr,
          domain: [domainMin, domainMax],
          steps,
          colors: palette
        };

        // Process into deck.gl accessor
        const newHexCfg = {
          getFillColor: colorContinuous(processColorContinuous(colorCfg))
        };

        // Apply view state changes
        const lon = parseFloat(document.getElementById('cfg-longitude')?.value);
        const lat = parseFloat(document.getElementById('cfg-latitude')?.value);
        const zoom = parseFloat(document.getElementById('cfg-zoom')?.value);
        const pitch = parseFloat(document.getElementById('cfg-pitch')?.value);
        const bearing = parseFloat(document.getElementById('cfg-bearing')?.value);

        if (!isNaN(lon) && !isNaN(lat)) {
          map.easeTo({
            center: [lon, lat],
            zoom: isNaN(zoom) ? map.getZoom() : zoom,
            pitch: isNaN(pitch) ? map.getPitch() : Math.min(85, Math.max(0, pitch)),
            bearing: isNaN(bearing) ? map.getBearing() : bearing,
            duration: 500
          });
        }

        // Update legend with proper gradient
        updateLegend(attr, [domainMin, domainMax], palette, steps);

        // Rebuild overlay with new config
        rebuildOverlay(newHexCfg, attr);
      }

      // Update config output textarea
      const outputArea = document.getElementById('cfg-output');
      function updateConfigOutput() {
        if (!outputArea) return;
        const cfg = buildConfigFromForm();
        outputArea.value = 'config = ' + toPython(cfg);
      }

      // Debounced apply
      let applyTimer = null;
      function scheduleApply() {
        clearTimeout(applyTimer);
        applyTimer = setTimeout(() => {
          applyConfigFromForm();
          updateConfigOutput();
        }, 300);
      }

      // Bind form inputs
      document.querySelectorAll('#debug-panel input, #debug-panel select').forEach(el => {
        el.addEventListener('input', scheduleApply);
        el.addEventListener('change', scheduleApply);
      });

      // Initial config output
      updateConfigOutput();

      window.resetConfig = function() {
        document.getElementById('cfg-longitude').value = {{ center_lng }};
        document.getElementById('cfg-latitude').value = {{ center_lat }};
        document.getElementById('cfg-zoom').value = {{ zoom }};
        document.getElementById('cfg-pitch').value = {{ pitch }};
        document.getElementById('cfg-bearing').value = {{ bearing }};
        document.getElementById('cfg-attr').value = {{ color_attr | tojson }};
        document.getElementById('cfg-palette').value = {{ palette_name | tojson }};
        document.getElementById('cfg-domain-min').value = {{ domain_min }};
        document.getElementById('cfg-domain-max').value = {{ domain_max }};
        document.getElementById('cfg-steps').value = {{ steps }};
        applyConfigFromForm();
        updateConfigOutput();
      };
    }
  </script>
</body>
</html>
""").render(
        mapbox_token=mapbox_token,
        tile_url=tile_url,
        config=merged_config,
        tooltip_columns=tooltip_columns or [],
        center_lng=center_lng,
        center_lat=center_lat,
        zoom=zoom,
        pitch=pitch,
        bearing=bearing,
        config_errors=config_errors,
        style_url=style_url,
        debug=debug,
        palettes=sorted(KNOWN_CARTOCOLOR_PALETTES),
        color_attr=fill_color_cfg.get("attr", "metric") if isinstance(fill_color_cfg, dict) else "metric",
        palette_name=fill_color_cfg.get("colors", "Magenta") if isinstance(fill_color_cfg, dict) else "Magenta",
        domain_min=fill_color_cfg.get("domain", [0, 100])[0] if isinstance(fill_color_cfg, dict) else 0,
        domain_max=fill_color_cfg.get("domain", [0, 100])[1] if isinstance(fill_color_cfg, dict) else 100,
        steps=fill_color_cfg.get("steps", 20) if isinstance(fill_color_cfg, dict) else 20,
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
    DEPRECATED: This is a deprecated map utility and will be removed soon.
    Only use this if user specifically asks for pydeck maps.
    Use deckgl_map() instead for modern Deck.GL-based point rendering.
    
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
    DEPRECATED: This is a deprecated map utility and will be removed soon.
    Only use this if user specifically asks for pydeck maps.
    Use deckgl_hex() instead for modern Deck.GL-based hexagon rendering.
    
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
    DEPRECATED: This is a deprecated map utility and will be removed soon.
    Only use this if user specifically asks for pydeck maps.
    Use deckgl_map() instead for modern Deck.GL-based polygon rendering.
    
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