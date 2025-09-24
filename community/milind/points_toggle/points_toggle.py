@fused.udf(cache_max_age=0)
def udf(
    host: str = "https://unstable.fused.io",
    token_a: str = "fsh_43k8h2pn3FkyPaR6KIXxff",
    token_b: str = "fsh_43k8h2pn3FkyPaR6KIXxff",
    mapbox_token: str = "pk.eyJ1IjoiaXNhYWNmdXNlZGxhYnMiLCJhIjoiY2xicGdwdHljMHQ1bzN4cWhtNThvbzdqcSJ9.73fb6zHMeO_c8eAXpZVNrA",
    style_url: str = "mapbox://styles/mapbox/dark-v10",
    center_lng: float = -121.16450354933122,
    center_lat: float = 38.44272969483187,
    zoom: float = 8.59,
    point_color_a: str = "#E8FF59",
    point_color_b: str = "#FF6DB8",
    layer_name_a: str = "Layer A",
    layer_name_b: str = "Layer B",
):
    from jinja2 import Template
    import fused

    def url_from_token(h, tok):
        return f"{h}/server/v1/realtime-shared/{tok}/run/file?dtype_out_vector=json"

    config = dict(
        url_a=url_from_token(host, token_a),
        url_b=url_from_token(host, token_b),
        mapbox_token=mapbox_token,
        style_url=style_url,
        center=[center_lng, center_lat],
        zoom=zoom,
        colors=dict(a=point_color_a, b=point_color_b),
        names=dict(a=layer_name_a, b=layer_name_b),
        src_a="src-a", src_b="src-b",
        lyr_a="pt-a", lyr_b="pt-b",
    )

    html = Template(r"""
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>Two FILE Layers · Checkboxes + Proper Hover Tooltip</title>
  <meta name="viewport" content="initial-scale=1,maximum-scale=1,user-scalable=no"/>
  <link href="https://api.mapbox.com/mapbox-gl-js/v3.15.0/mapbox-gl.css" rel="stylesheet"/>
  <script src="https://api.mapbox.com/mapbox-gl-js/v3.15.0/mapbox-gl.js"></script>
  <style>
    html, body { margin:0; height:100%; }
    #map { position:absolute; inset:0; }
    #controls {
      position:absolute; z-index:1; top:10px; right:10px;
      background:#fff; border:1px solid rgba(0,0,0,.3); border-radius:8px;
      font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Ubuntu,'Helvetica Neue',Arial,sans-serif;
      padding:8px 10px; min-width:160px; box-shadow:0 2px 8px rgba(0,0,0,.1);
    }
    .row { display:flex; align-items:center; gap:8px; padding:6px 2px; }
    .row + .row { border-top:1px solid rgba(0,0,0,.08); }
    label { font-size:13px; color:#333; cursor:pointer; user-select:none; }
    input[type="checkbox"] { transform: scale(1.1); cursor:pointer; }
    .dot { width:10px; height:10px; border-radius:50%; border:1px solid #111; }
    .mapboxgl-popup { max-width:260px; font:12px/1.4 sans-serif; }
    .mapboxgl-popup-content { padding:6px 8px; }
    .mapboxgl-popup-content table { border-collapse:collapse; width:100%; }
    .mapboxgl-popup-content td { padding:2px 4px; border-bottom:1px solid #eee; vertical-align:top; }
    .mapboxgl-popup-content td.key { font-weight:bold; color:#333; width:35%; }
    .mapboxgl-popup-content td.val { color:#555; }
  </style>
</head>
<body>
  <div id="map"></div>
  <div id="controls">
    <div class="row">
      <input id="chkA" type="checkbox" checked />
      <div id="dotA" class="dot"></div>
      <label for="chkA" id="lblA"></label>
    </div>
    <div class="row">
      <input id="chkB" type="checkbox" checked />
      <div id="dotB" class="dot"></div>
      <label for="chkB" id="lblB"></label>
    </div>
  </div>

  <script>
    const CFG = {{ config | tojson }};
    mapboxgl.accessToken = CFG.mapbox_token;

    // init labels & color dots
    lblA.textContent = CFG.names.a; lblB.textContent = CFG.names.b;
    dotA.style.background = CFG.colors.a; dotB.style.background = CFG.colors.b;

    const map = new mapboxgl.Map({ container:'map', style:CFG.style_url, center:CFG.center, zoom:CFG.zoom });

    async function fetchGeoJSON(url){ const r=await fetch(url); if(!r.ok) throw new Error(r.status); return r.json(); }

    function addPointLayer(sourceId, layerId, color) {
      if (!map.getSource(sourceId)) {
        map.addSource(sourceId, { type:"geojson", data:{ type:"FeatureCollection", features:[] }});
      }
      if (!map.getLayer(layerId)) {
        map.addLayer({
          id: layerId,
          type: "circle",
          source: sourceId,
          filter: ["any", ["==", ["geometry-type"], "Point"], ["==", ["geometry-type"], "MultiPoint"]],
          paint: {
            "circle-radius": 4,
            "circle-color": color,
            "circle-stroke-width": 1,
            "circle-stroke-color": "#1a1a1a"
          }
        });
      }
    }

    // ---- Robust hover popup (singleton per map) ----
    const hoverPopup = new mapboxgl.Popup({ closeButton:false, closeOnClick:false });
    let popupVisible = false;

    function buildPropsTable(props) {
      let html = "<table>";
      for (const [k,v] of Object.entries(props || {})) {
        html += `<tr><td class="key">${k}</td><td class="val">${String(v)}</td></tr>`;
      }
      return html + "</table>";
    }

    function bindHover(layerId) {
      map.on("mousemove", layerId, (e) => {
        map.getCanvas().style.cursor = "pointer";
        const f = e.features && e.features[0];
        if (!f) return;
        const coords = f.geometry.type === "Point" ? f.geometry.coordinates.slice() : [e.lngLat.lng, e.lngLat.lat];
        const html = buildPropsTable(f.properties || {});
        if (!popupVisible) { hoverPopup.addTo(map); popupVisible = true; }
        hoverPopup.setLngLat(coords).setHTML(html);
      });
      map.on("mouseleave", layerId, () => {
        map.getCanvas().style.cursor = "";
        if (popupVisible) { hoverPopup.remove(); popupVisible = false; }
      });
    }

    function setLayerVisibility(layerId, visible) {
      if (map.getLayer(layerId)) map.setLayoutProperty(layerId, "visibility", visible ? "visible" : "none");
      // also clear popup if hiding
      if (!visible && popupVisible) { hoverPopup.remove(); popupVisible = false; }
    }

    map.on("load", async () => {
      addPointLayer(CFG.src_a, CFG.lyr_a, CFG.colors.a);
      addPointLayer(CFG.src_b, CFG.lyr_b, CFG.colors.b);

      // load data
      try {
        const [gA, gB] = await Promise.all([ fetchGeoJSON(CFG.url_a), fetchGeoJSON(CFG.url_b) ]);
        map.getSource(CFG.src_a).setData(gA);
        map.getSource(CFG.src_b).setData(gB);
      } catch (e) { console.error(e); alert("Load failed: " + e.message); }

      // bind hover handlers
      bindHover(CFG.lyr_a);
      bindHover(CFG.lyr_b);

      // checkboxes
      chkA.addEventListener("change", () => setLayerVisibility(CFG.lyr_a, chkA.checked));
      chkB.addEventListener("change", () => setLayerVisibility(CFG.lyr_b, chkB.checked));

      // if mouse leaves the whole map, ensure popup is removed
      map.getCanvas().addEventListener("mouseleave", () => {
        if (popupVisible) { hoverPopup.remove(); popupVisible = false; }
      });
    });
  </script>
</body>
</html>
""").render(config=config)

    common = fused.load("https://github.com/fusedio/udfs/tree/351515e/public/common/")
    return common.html_to_obj(html)
