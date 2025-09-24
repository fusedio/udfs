@fused.udf(cache_max_age=0)
def udf(
    url: str = "https://unstable.fused.io/server/v1/realtime-shared/fsh_7QRjYJnhT9zE7pspQ18O6D/run/file?dtype_out_raster=png&dtype_out_vector=parquet",
    mapbox_token: str = "pk.eyJ1IjoiaXNhYWNmdXNlZGxhYnMiLCJhIjoiY2xicGdwdHljMHQ1bzN4cWhtNThvbzdqcSJ9.73fb6zHMeO_c8eAXpZVNrA",
    style_url: str = "mapbox://styles/mapbox/dark-v10",
    center_lng: float = -121.16450354933122,
    center_lat: float = 38.44272969483187,
    zoom: float = 8.59,
    layer_id: str = "investment-polys",
    outline_color: str = "#111111",
    fill_opacity: float = 0.65,
    tooltip_keys: list = None,
):
    import io, json, math, numpy as np, pandas as pd, geopandas as gpd
    from shapely import wkb
    from jinja2 import Template
    import fused

    data_url = fused.api.sign_url(url) if str(url).lower().startswith("s3://") else url
    df = pd.read_parquet(data_url)

    geom_col = "geometry" if "geometry" in df.columns else None
    if geom_col is None:
        raise ValueError("No 'geometry' column found.")

    def load_wkb(val):
        if val is None or (isinstance(val, float) and math.isnan(val)):
            return None
        if isinstance(val, (bytes, bytearray)):
            return wkb.loads(val)
        if isinstance(val, str):
            try:
                return wkb.loads(bytes.fromhex(val))
            except Exception:
                return None
        return None

    gseries = df[geom_col].apply(load_wkb)
    gdf = gpd.GeoDataFrame(df.drop(columns=[geom_col]), geometry=gseries, crs="EPSG:4326")
    gdf = gdf[~gdf.geometry.is_empty & gdf.geometry.notnull()].copy()

    if "investment_score" not in gdf.columns:
        raise ValueError("Missing 'investment_score'")

    gdf["investment_score"] = (
        pd.to_numeric(gdf["investment_score"], errors="coerce")
        .replace([np.inf, -np.inf], np.nan)
    )
    gdf = gdf[gdf["investment_score"].notna()].copy()
    vmin, vmax = float(gdf["investment_score"].min()), float(gdf["investment_score"].max())
    if vmin == vmax:
        vmin, vmax = vmin - 1, vmax + 1

    prop_cols = [c for c in gdf.columns if c != "geometry"]
    gdf[prop_cols] = gdf[prop_cols].where(pd.notna(gdf[prop_cols]), None)

    if not tooltip_keys:
        defaults = ["GEOID", "STATEFP", "COUNTYFP", "TRACTCE", "BLKGRPCE", "MEDINC", "POP", "investment_score"]
        tooltip_keys = [k for k in defaults if k in gdf.columns] or ["investment_score"]

    config = dict(
        geojson=json.loads(gdf.to_json()),
        mapbox_token=mapbox_token,
        style_url=style_url,
        center=[center_lng, center_lat],
        zoom=zoom,
        layer_id=layer_id,
        outline_color=outline_color,
        fill_opacity=max(0.0, min(1.0, fill_opacity)),
        vmin=vmin,
        vmax=vmax,
        tooltip_keys=tooltip_keys,
        ramp=["#2b65a0", "#35af6d", "#e8ff59"],
    )

    html = Template(r"""
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>investment_score map</title>
  <meta name="viewport" content="initial-scale=1,maximum-scale=1,user-scalable=no"/>
  <link href="https://api.mapbox.com/mapbox-gl-js/v3.15.0/mapbox-gl.css" rel="stylesheet"/>
  <script src="https://api.mapbox.com/mapbox-gl-js/v3.15.0/mapbox-gl.js"></script>
  <style>
    html,body{margin:0;height:100%;background:#0b0b0b}
    #map{position:absolute;inset:0}
    .legend{
      position:absolute;z-index:1;bottom:12px;left:12px;
      background:rgba(0,0,0,.55);color:#fff;padding:8px 10px;
      border-radius:8px;font:12px/1.3 sans-serif;
      border:1px solid rgba(255,255,255,.12);min-width:180px;
    }
    .legend .bar{height:10px;border-radius:5px;margin:6px 0 4px 0;
      background:linear-gradient(90deg, {{ config.ramp[0] }}, {{ config.ramp[1] }}, {{ config.ramp[2] }});}
    .legend .row{display:flex;justify-content:space-between;opacity:.9}
    /* Dark popup */
    .mapboxgl-popup { max-width:280px; font:12px/1.4 sans-serif; }
    .mapboxgl-popup-content {
      padding:6px 8px; background:#1a1a1a; color:#eee;
      border:1px solid #444; border-radius:6px;
    }
    .mapboxgl-popup-content table { border-collapse:collapse;width:100%; }
    .mapboxgl-popup-content td { padding:2px 4px;border-bottom:1px solid #333; }
    .mapboxgl-popup-content td.key { font-weight:600; color:#e8ff59; width:40%; }
    .mapboxgl-popup-content td.val { color:#eee; }
  </style>
</head>
<body>
  <div id="map"></div>
  <div class="legend">
    <div>investment_score</div>
    <div class="bar"></div>
    <div class="row"><span id="minv"></span><span id="maxv"></span></div>
  </div>
  <script src="https://cdn.jsdelivr.net/npm/@turf/turf@6/turf.min.js"></script>
  <script>
    const CFG = {{ config | tojson }};
    mapboxgl.accessToken = CFG.mapbox_token;
    const map = new mapboxgl.Map({ container:'map', style:CFG.style_url, center:CFG.center, zoom:CFG.zoom });

    document.getElementById('minv').textContent = CFG.vmin.toFixed(3);
    document.getElementById('maxv').textContent = CFG.vmax.toFixed(3);

    map.on('load', () => {
      map.addSource('geom-src', {type:'geojson', data:CFG.geojson});

      map.addLayer({
        id: CFG.layer_id,
        type: 'fill',
        source: 'geom-src',
        filter: ["==","$type","Polygon"],
        paint: {
          'fill-color': ['interpolate',['linear'],['to-number',['get','investment_score']],
            CFG.vmin, '{{ config.ramp[0] }}',
            (CFG.vmin+CFG.vmax)/2, '{{ config.ramp[1] }}',
            CFG.vmax, '{{ config.ramp[2] }}'],
          'fill-opacity': CFG.fill_opacity
        }
      });
      map.addLayer({
        id: CFG.layer_id+'-outline',
        type: 'line',
        source: 'geom-src',
        filter: ["==","$type","Polygon"],
        paint: {'line-color':CFG.outline_color,'line-width':0.5,'line-opacity':0.9}
      });

      const bbox = turf.bbox(CFG.geojson);
      map.fitBounds([[bbox[0],bbox[1]],[bbox[2],bbox[3]]],{padding:28,duration:600});

      const popup=new mapboxgl.Popup({closeButton:false,closeOnClick:false});
      map.on('mousemove', CFG.layer_id, e=>{
        map.getCanvas().style.cursor='pointer';
        const f=e.features[0];
        let html="<table>";
        for(const k of CFG.tooltip_keys){
          if(f.properties.hasOwnProperty(k)){
            html+=`<tr><td class="key">${k}</td><td class="val">${f.properties[k]}</td></tr>`;
          }
        }
        html+="</table>";
        popup.setLngLat(e.lngLat).setHTML(html).addTo(map);
      });
      map.on('mouseleave', CFG.layer_id, ()=>{
        map.getCanvas().style.cursor='';
        popup.remove();
      });
    });
  </script>
</body>
</html>
""").render(config=config)

    common = fused.load("https://github.com/fusedio/udfs/tree/351515e/public/common/")
    return common.html_to_obj(html)
