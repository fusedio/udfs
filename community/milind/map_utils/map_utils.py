import json
import geopandas as gpd
import pandas as pd
import pydeck as pdk
from shapely.geometry import mapping
import folium
import numpy as np
import typing  # added for Union typing

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



from jinja2 import Template
import json


DEFAULT_DECK_CONFIG = {
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


import json
from jinja2 import Template

DEFAULT_DECK_CONFIG = {
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
        "tooltipAttrs": ["house_age", "mrt_distance", "price"]
    }
}

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
    from copy import deepcopy

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

    if config is None or config == "":
        user_config = {}
    elif isinstance(config, str):
        try:
            user_config = json.loads(config)
        except json.JSONDecodeError as exc:
            print(f"[deckgl_map] Failed to parse config JSON: {exc}")
            config_errors.append(f"Failed to parse config JSON: {exc}")
            user_config = {}
    else:
        if isinstance(config, dict):
            user_config = config
        else:
            config_errors.append("Config must be a dict or JSON string. Falling back to defaults.")
            user_config = {}

    if not isinstance(user_config, dict):
        config_errors.append("Config must be a dict. Falling back to defaults.")
        user_config = {}

    # Validate expected structure
    merged_config = deepcopy(DEFAULT_DECK_CONFIG)
    if isinstance(user_config, dict):
        try:
            def _merge(base: dict, extra: dict) -> dict:
                for k, v in extra.items():
                    if isinstance(v, dict) and isinstance(base.get(k), dict):
                        _merge(base[k], v)
                    else:
                        base[k] = v
                return base

            merged_config = _merge(merged_config, deepcopy(user_config))
        except Exception as exc:  # pragma: no cover
            config_errors.append(f"Failed to merge config overrides: {exc}. Using defaults.")
            merged_config = deepcopy(DEFAULT_DECK_CONFIG)
    else:
        merged_config = deepcopy(DEFAULT_DECK_CONFIG)

    initial_view_state = merged_config.get("initialViewState")
    if not isinstance(initial_view_state, dict):
        config_errors.append("initialViewState must be an object.")
        merged_config["initialViewState"] = deepcopy(DEFAULT_DECK_CONFIG["initialViewState"])
    else:
        if not isinstance(initial_view_state.get("zoom"), (int, float)):
            config_errors.append("initialViewState.zoom must be numeric. Using default zoom.")
            merged_config["initialViewState"]["zoom"] = DEFAULT_DECK_CONFIG["initialViewState"]["zoom"]

    vector_layer = merged_config.get("vectorLayer")
    if not isinstance(vector_layer, dict):
        config_errors.append("vectorLayer must be an object; using defaults.")
        merged_config["vectorLayer"] = deepcopy(DEFAULT_DECK_CONFIG["vectorLayer"])
        vector_layer = merged_config["vectorLayer"]
    else:
        fill_cfg = vector_layer.get("getFillColor")
        if isinstance(fill_cfg, dict):
            if fill_cfg.get("@@function") != "colorContinuous":
                config_errors.append("vectorLayer.getFillColor.@@function must be 'colorContinuous'. Resetting to defaults.")
                vector_layer["getFillColor"] = deepcopy(DEFAULT_DECK_CONFIG["vectorLayer"]["getFillColor"])
            else:
                if not isinstance(fill_cfg.get("attr"), str):
                    config_errors.append("vectorLayer.getFillColor.attr must be a string column. Using default attr.")
                    vector_layer["getFillColor"]["attr"] = DEFAULT_DECK_CONFIG["vectorLayer"]["getFillColor"]["attr"]
                domain = fill_cfg.get("domain")
                if not (isinstance(domain, (list, tuple)) and len(domain) == 2):
                    config_errors.append("vectorLayer.getFillColor.domain must be [min, max]. Using default domain.")
                    vector_layer["getFillColor"]["domain"] = DEFAULT_DECK_CONFIG["vectorLayer"]["getFillColor"]["domain"]
                steps = fill_cfg.get("steps")
                if steps is not None and (not isinstance(steps, int) or steps <= 0):
                    config_errors.append("vectorLayer.getFillColor.steps must be a positive integer. Using default steps.")
                    vector_layer["getFillColor"]["steps"] = DEFAULT_DECK_CONFIG["vectorLayer"]["getFillColor"]["steps"]
        elif fill_cfg is not None:
            config_errors.append("vectorLayer.getFillColor must be an object; using defaults.")
            vector_layer["getFillColor"] = deepcopy(DEFAULT_DECK_CONFIG["vectorLayer"]["getFillColor"])

        tooltip_attrs = vector_layer.get("tooltipAttrs")
        if tooltip_attrs is not None and not isinstance(tooltip_attrs, (list, tuple)):
            config_errors.append("vectorLayer.tooltipAttrs must be an array of column names.")
            vector_layer["tooltipAttrs"] = DEFAULT_DECK_CONFIG["vectorLayer"]["tooltipAttrs"]

    def _merge_dict(base: dict, extra: dict) -> dict:
        for key, value in extra.items():
            if isinstance(value, dict) and isinstance(base.get(key), dict):
                _merge_dict(base[key], value)
            else:
                base[key] = value
        return base

    initial_view_state = merged_config.get("initialViewState", {})
    auto_state = {
        "longitude": float(auto_center[0]) if auto_center else 0.0,
        "latitude": float(auto_center[1]) if auto_center else 0.0,
        "zoom": initial_view_state.get("zoom", 11),
    }

    # Extract fill color config
    vector_layer = merged_config.get("vectorLayer", {})
    fill_color_config = vector_layer.get("getFillColor", {})
    tooltip_config = None
    if isinstance(merged_config, dict):
        tooltip_config = merged_config.get("tooltipColumns")
    if tooltip_config is None and isinstance(vector_layer, dict):
        tooltip_config = vector_layer.get("tooltipColumns")

    if isinstance(tooltip_config, str):
        tooltip_columns = [tooltip_config]
    elif isinstance(tooltip_config, (list, tuple, set)):
        tooltip_columns = [
            str(item)
            for item in tooltip_config
            if isinstance(item, str) and item.strip()
        ]
    else:
        tooltip_columns = []

    html = Template(r"""
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="initial-scale=1, maximum-scale=1, user-scalable=no" />
  <link href="https://api.mapbox.com/mapbox-gl-js/v2.15.0/mapbox-gl.css" rel="stylesheet" />
  <script src="https://api.mapbox.com/mapbox-gl-js/v2.15.0/mapbox-gl.js"></script>
  <style>
    html, body { margin: 0; height: 100%; background: #000; }
    #map { position: absolute; inset: 0; }
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
  </style>
</head>
<body>
<div id="map"></div>
<div id="tooltip"></div>
<div id="config-error" class="config-error"></div>
<script>
const MAPBOX_TOKEN = {{ mapbox_token | tojson }};
const GEOJSON = {{ geojson_obj | tojson }};
const AUTO_STATE = {{ auto_state | tojson }};
const FILL_COLOR_CONFIG = {{ fill_color_config | tojson }};
const TOOLTIP_COLUMNS = {{ tooltip_columns | tojson }};
const CONFIG_ERROR = {{ config_error | tojson }};

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

let initialBounds = null;
if (GEOJSON.features && GEOJSON.features.length > 0) {
  try {
    const bounds = new mapboxgl.LngLatBounds();
    GEOJSON.features.forEach(feature => {
      if (feature.geometry.type === 'Point') {
        bounds.extend(feature.geometry.coordinates);
      } else if (feature.geometry.type === 'Polygon') {
        feature.geometry.coordinates[0].forEach(coord => bounds.extend(coord));
      } else if (feature.geometry.type === 'MultiPolygon') {
        feature.geometry.coordinates.forEach(poly => {
          poly[0].forEach(coord => bounds.extend(coord));
        });
      }
    });
    initialBounds = bounds;
  } catch (err) {}
}

mapboxgl.accessToken = MAPBOX_TOKEN;

const map = new mapboxgl.Map({
  container: 'map',
  style: 'mapbox://styles/mapbox/dark-v10',
  center: [AUTO_STATE.longitude, AUTO_STATE.latitude],
  zoom: AUTO_STATE.zoom,
  preserveDrawingBuffer: true,
  bounds: initialBounds,
  fitBoundsOptions: {
    padding: 50,
    maxZoom: 15
  }
});

map.on('load', () => {
  map.addSource('gdf-source', {
    type: 'geojson',
    data: { type: 'FeatureCollection', features: [] }
  });
  // Defer data injection so the basemap renders immediately.
  requestAnimationFrame(() => {
    const source = map.getSource('gdf-source');
    if (source) {
      source.setData(GEOJSON);
    }
  });

  const firstFeature = GEOJSON.features && GEOJSON.features[0];
  const geomType = firstFeature && firstFeature.geometry && firstFeature.geometry.type;
  const hoverLayers = [];
  if (geomType === 'Point' || geomType === 'MultiPoint') {
    map.addLayer({
      id: 'gdf-layer',
      type: 'circle',
      source: 'gdf-source',
      paint: {
        'circle-radius': 6,
        'circle-color': '#0090ff',
        'circle-opacity': 0.8,
        'circle-stroke-width': 1,
        'circle-stroke-color': '#ffffff'
      }
    });
    hoverLayers.push('gdf-layer');
  } else if (geomType === 'LineString' || geomType === 'MultiLineString') {
    map.addLayer({
      id: 'gdf-layer',
      type: 'line',
      source: 'gdf-source',
      paint: {
        'line-color': '#0090ff',
        'line-width': 2,
        'line-opacity': 0.8
      }
    });
    hoverLayers.push('gdf-layer');
  } else {
    // Polygon or MultiPolygon
    map.addLayer({
      id: 'gdf-layer-fill',
      type: 'fill',
      source: 'gdf-source',
      paint: {
        'fill-color': '#0090ff',
        'fill-opacity': 0.6
      }
    });
    map.addLayer({
      id: 'gdf-layer-outline',
      type: 'line',
      source: 'gdf-source',
      paint: {
        'line-color': '#ffffff',
        'line-width': 1,
        'line-opacity': 0.8
      }
    });
    hoverLayers.push('gdf-layer-fill', 'gdf-layer-outline');
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
    if (!hoverLayers.length) return;
    const feats = map.queryRenderedFeatures(e.point, { layers: hoverLayers });
    if (feats && feats.length) {
      map.getCanvas().style.cursor = 'pointer';
      const props = feats[0].properties || {};
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
    from jinja2 import Template
    import pandas as pd
    import json
    from copy import deepcopy

    config_errors = []

    # Handle config: None, JSON string, or dict
    if config is None or config == "":
        config = deepcopy(DEFAULT_DECK_HEX_CONFIG)
    elif isinstance(config, str):
        try:
            parsed = json.loads(config)
        except json.JSONDecodeError as exc:
            config_errors.append(f"Failed to parse config JSON: {exc}")
            parsed = {}
        if isinstance(parsed, dict):
            merged = deepcopy(DEFAULT_DECK_HEX_CONFIG)
            merged.update(parsed)
            config = merged
        else:
            config_errors.append("Parsed config is not a JSON object. Falling back to defaults.")
            config = deepcopy(DEFAULT_DECK_HEX_CONFIG)
    elif isinstance(config, dict):
        merged = deepcopy(DEFAULT_DECK_HEX_CONFIG)
        merged.update(config)
        config = merged
    else:
        config_errors.append("Config must be a dict or JSON string. Falling back to defaults.")
        config = deepcopy(DEFAULT_DECK_HEX_CONFIG)

    # Validate critical config pieces against expected structure
    hex_layer = config.get("hexLayer")
    if not isinstance(hex_layer, dict):
        config_errors.append("Config.hexLayer must be an object; falling back to defaults.")
        config["hexLayer"] = deepcopy(DEFAULT_DECK_HEX_CONFIG["hexLayer"])
        hex_layer = config["hexLayer"]

    if not isinstance(hex_layer.get("getHexagon"), str) or not hex_layer.get("getHexagon"):
        config_errors.append("hexLayer.getHexagon must be a string expression (e.g. '@@=properties.hex'). Falling back to default accessor.")
        hex_layer["getHexagon"] = DEFAULT_DECK_HEX_CONFIG["hexLayer"]["getHexagon"]

    fill_cfg = hex_layer.get("getFillColor")
    if not isinstance(fill_cfg, dict):
        config_errors.append("hexLayer.getFillColor must be an object with @@function, attr, and domain. Using defaults.")
        hex_layer["getFillColor"] = deepcopy(DEFAULT_DECK_HEX_CONFIG["hexLayer"]["getFillColor"])
        fill_cfg = hex_layer["getFillColor"]
    else:
        if fill_cfg.get("@@function") != "colorContinuous":
            config_errors.append("hexLayer.getFillColor.@@function must be 'colorContinuous'. Resetting to defaults.")
            hex_layer["getFillColor"] = deepcopy(DEFAULT_DECK_HEX_CONFIG["hexLayer"]["getFillColor"])
            fill_cfg = hex_layer["getFillColor"]
        else:
            if not isinstance(fill_cfg.get("attr"), str):
                config_errors.append("hexLayer.getFillColor.attr must be a string column name. Using default attr.")
                fill_cfg["attr"] = DEFAULT_DECK_HEX_CONFIG["hexLayer"]["getFillColor"]["attr"]
        domain = fill_cfg.get("domain")
        if not (isinstance(domain, (list, tuple)) and len(domain) == 2):
            config_errors.append("hexLayer.getFillColor.domain must be a [min, max] array. Using default domain.")
            fill_cfg["domain"] = DEFAULT_DECK_HEX_CONFIG["hexLayer"]["getFillColor"]["domain"]

    tooltip_columns = []
    tooltip_config = None
    if isinstance(config, dict):
        tooltip_config = (
            config.get("tooltipColumns")
            or config.get("tooltip_columns")
            or config.get("tooltipAttrs")
            or config.get("hexLayer", {}).get("tooltipColumns")
            or config.get("hexLayer", {}).get("tooltipAttrs")
        )

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
        if tooltip_config is not None:
            if isinstance(tooltip_config, str):
                tooltip_columns = [tooltip_config]
            elif isinstance(tooltip_config, (list, tuple, set)):
                tooltip_columns = [
                    str(col) for col in tooltip_config if isinstance(col, str) and col.strip()
                ]
            else:
                tooltip_columns = []
        else:
            tooltip_columns = []

        if tooltip_config is not None:
            available_keys = set(data_records[0].keys())
            missing_tooltips = [col for col in tooltip_columns if col not in available_keys]
            if missing_tooltips:
                print(f"[deckgl_hex] Warning: tooltip columns not found in data: {missing_tooltips}")
            tooltip_columns = [col for col in tooltip_columns if col in available_keys]
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
  <script src="https://unpkg.com/deck.gl@9.1.3/dist.min.js"></script>
  <script src="https://unpkg.com/@deck.gl/geo-layers@9.1.3/dist.min.js"></script>
  <script src="https://unpkg.com/@deck.gl/carto@9.1.3/dist.min.js"></script>
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
  </style>
</head>
<body>
  <div id="map"></div>
  <div id="tooltip"></div>
  <div id="config-error"></div>

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
      layers: [createHexLayer([])]
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
  <script src="https://unpkg.com/deck.gl@latest/dist.min.js"></script>
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