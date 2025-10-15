import fused

DEFAULT_NODES_JSON = r"""[
  {"id":"TSMC","name":"TSMC","country":"Taiwan","role":"Produces 90%+ of the world’s most advanced chips","lat":24.78,"lng":121.00,"category":"Fabrication","weight":5},
  {"id":"ASML","name":"ASML","country":"Netherlands","role":"EUV lithography machines supplier to TSMC","lat":51.42,"lng":5.40,"category":"Equipment","weight":4},
  {"id":"ZEISS","name":"Carl Zeiss SMT","country":"Germany","role":"Only firm making mirrors precise enough for ASML’s EUV","lat":48.79,"lng":10.11,"category":"Optics","weight":3},
  {"id":"CYMER","name":"Cymer (ASML)","country":"USA (San Diego)","role":"EUV light source","lat":32.89,"lng":-117.14,"category":"Light Source","weight":3},
  {"id":"JSR","name":"JSR","country":"Japan","role":"Photoresists for lithography","lat":35.68,"lng":139.76,"category":"Chemicals","weight":3},
  {"id":"TOK","name":"Tokyo Ohka Kogyo","country":"Japan","role":"Advanced photoresists","lat":35.53,"lng":139.70,"category":"Chemicals","weight":3},
  {"id":"SPRUCE","name":"Spruce Pine Mine","country":"USA","role":"Ultra-pure quartz for silicon wafers","lat":35.91,"lng":-82.06,"category":"Materials","weight":3},
  {"id":"CHILE","name":"Chile (Cu/Li)","country":"Chile","role":"Copper and lithium mining","lat":-33.45,"lng":-70.66,"category":"Materials","weight":2},
  {"id":"CONGO","name":"DR Congo (RE)","country":"DRC","role":"Rare-earth & critical minerals","lat":-4.32,"lng":15.31,"category":"Materials","weight":2},
  {"id":"CHINA","name":"China (Refining)","country":"China","role":"Rare-earth & copper refining","lat":35.86,"lng":104.19,"category":"Materials","weight":3},
  {"id":"UKR","name":"Ukraine (Neon)","country":"Ukraine","role":"Neon gas for lithography","lat":49.0,"lng":32.0,"category":"Gases","weight":2},

  {"id":"NVDA","name":"NVIDIA","country":"USA","role":"Chip design","lat":37.37,"lng":-121.96,"category":"Design","weight":4},
  {"id":"AMD","name":"AMD","country":"USA","role":"Chip design","lat":37.40,"lng":-121.96,"category":"Design","weight":3},
  {"id":"AAPL","name":"Apple","country":"USA","role":"Chip design (Apple Silicon)","lat":37.33,"lng":-122.03,"category":"Design","weight":3},
  {"id":"SNPS","name":"Synopsys","country":"USA","role":"EDA software","lat":37.39,"lng":-122.03,"category":"Software","weight":2},
  {"id":"CDNS","name":"Cadence","country":"USA","role":"EDA software","lat":37.41,"lng":-121.95,"category":"Software","weight":2}
]"""

DEFAULT_LINKS_JSON = r"""[
  {"source":"SNPS","target":"NVDA","desc":"EDA → NVIDIA"},
  {"source":"CDNS","target":"AMD","desc":"EDA → AMD"},
  {"source":"NVDA","target":"TSMC","desc":"NVIDIA designs → TSMC fabs"},
  {"source":"AMD","target":"TSMC","desc":"AMD designs → TSMC fabs"},
  {"source":"AAPL","target":"TSMC","desc":"Apple Silicon → TSMC fabs"},

  {"source":"ZEISS","target":"ASML","desc":"ZEISS mirrors → ASML EUV"},
  {"source":"CYMER","target":"ASML","desc":"Cymer light source → ASML EUV"},
  {"source":"ASML","target":"TSMC","desc":"ASML EUV → TSMC"},

  {"source":"JSR","target":"TSMC","desc":"Photoresists → TSMC"},
  {"source":"TOK","target":"TSMC","desc":"Photoresists → TSMC"},
  {"source":"SPRUCE","target":"TSMC","desc":"Quartz → wafers → TSMC"},
  {"source":"CHILE","target":"TSMC","desc":"Copper/Lithium → TSMC"},
  {"source":"CONGO","target":"TSMC","desc":"Critical minerals → TSMC"},
  {"source":"CHINA","target":"TSMC","desc":"Refined materials → TSMC"},
  {"source":"UKR","target":"ASML","desc":"Neon gas → ASML"}
]"""

CATEGORY_COLORS = r"""{
  "Design": [0, 220, 255],
  "Software": [0, 255, 180],
  "Equipment": [255, 210, 0],
  "Fabrication": [255, 160, 0],
  "Chemicals": [200, 0, 255],
  "Materials": [255, 90, 90],
  "Gases": [180, 120, 255],
  "Optics": [255, 120, 40],
  "Light Source": [255, 200, 120]
}"""

@fused.udf(cache_max_age=0)
def udf(
    nodes_json: str = DEFAULT_NODES_JSON,
    links_json: str = DEFAULT_LINKS_JSON,
    category_colors_json: str = CATEGORY_COLORS,
    mapbox_token: str = "pk.eyJ1IjoiaXNhYWNmdXNlZGxhYnMiLCJhIjoiY2xicGdwdHljMHQ1bzN4cWhtNThvbzdqcSJ9.73fb6zHMeO_c8eAXpZVNrA",
    center_lng: float = 15.0,
    center_lat: float = 25.0,
    zoom: float = 1.6
):
    from jinja2 import Template

    html = Template(r"""
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Semiconductor Supply Chain — Global Interdependence</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />

  <link href="https://api.mapbox.com/mapbox-gl-js/v2.15.0/mapbox-gl.css" rel="stylesheet" />
  <script src="https://api.mapbox.com/mapbox-gl-js/v2.15.0/mapbox-gl.js"></script>

  <script src="https://unpkg.com/deck.gl@9.1.3/dist.min.js"></script>
  <script src="https://unpkg.com/@deck.gl/geo-layers@9.1.3/dist.min.js"></script>

  <style>
    html, body { margin:0; height:100%; width:100%; background:#000; overflow:hidden; }
    #root { position:relative; display:grid; grid-template-columns: 1fr 420px; height:100vh; }
    #map { height:100%; width:100%; }

    #hud {
      position:absolute; top:10px; left:10px; z-index:5; color:#eaeaea;
      background:rgba(0,0,0,.6); border:1px solid rgba(255,255,255,.1); border-radius:10px;
      padding:10px 12px; font: 12px/1.5 system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial;
      max-width: 360px; backdrop-filter: blur(4px);
      box-shadow: 0 8px 24px rgba(0,0,0,.4);
    }
    #hud h3 { margin:0 0 6px 0; font-size:14px; color:#7dd3fc; }
    #hud .row { display:flex; flex-wrap:wrap; gap:8px; margin-top:6px; }
    #hud .btn {
      border:1px solid rgba(255,255,255,.15); border-radius:8px; padding:6px 10px; cursor:pointer; user-select:none;
      background:rgba(255,255,255,.06);
    }
    #note { color:#a3e635; }
    #footer {
      position:absolute; right:10px; bottom:10px; z-index:5; color:#d4d4d4; background:rgba(0,0,0,.5);
      padding:6px 10px; border-radius:8px; border:1px solid rgba(255,255,255,.1);
      font:12px system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial;
    }
    #mode-pill {
      position:absolute; top:10px; right:440px; z-index:6;
      background:rgba(0,0,0,.55); color:#eee; padding:6px 10px; border-radius:999px;
      border:1px solid rgba(255,255,255,.1); font:11px system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial;
      display:none;
    }

    /* Process panel */
    #process {
      height:100%; color:#e5e5e5; background:linear-gradient(180deg, rgba(20,20,20,.88), rgba(10,10,10,.88));
      border-left:1px solid rgba(255,255,255,.08);
      font: 13px/1.5 system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial;
      display:flex; flex-direction:column; overflow:hidden;
    }
    #process header {
      position:sticky; top:0; z-index:2;
      padding:12px; border-bottom:1px solid rgba(255,255,255,.08);
      background:linear-gradient(180deg, rgba(20,20,20,.92), rgba(20,20,20,.90));
      backdrop-filter: blur(4px);
    }
    #process h4 { margin:0; font-size:14px; color:#a7f3d0; }
    #process p.sub { margin:6px 0 12px 0; color:#cbd5e1; font-size:12px; }

    #selectors { display:flex; flex-wrap:wrap; gap:8px; align-items:center; margin-bottom:8px; }
    #selectors .field { display:inline-flex; align-items:center; gap:6px; }
    #selectors select, #selectors input[type="checkbox"] { cursor:pointer; }
    #legend { display:flex; flex-wrap:wrap; gap:6px; margin-top:6px; }
    .chip { display:inline-flex; align-items:center; gap:6px; padding:2px 6px; border-radius:999px; border:1px solid rgba(255,255,255,.2); }
    .dot { width:10px; height:10px; border-radius:50%; display:inline-block; }

    #story { flex:1; overflow:auto; padding:10px 12px 24px 12px; }
    ol.flow { list-style:decimal; margin:0; padding-left:18px; display:flex; flex-direction:column; gap:10px; }
    ol.flow li { padding:10px 10px; border-radius:10px; border:1px solid rgba(255,255,255,.06); background:rgba(255,255,255,.03); }
    ol.flow li .meta { display:block; color:#93c5fd; font-size:11px; margin-top:4px; }
    ol.flow li .sources { display:inline-flex; gap:6px; margin-left:6px; }
    .src { color:#60a5fa; text-decoration:none; border-bottom:1px dotted rgba(96,165,250,.6); font-size:12px; }
    .src:hover { text-decoration:underline; }
    ol.flow li.active { border-color: rgba(99,102,241,.45); box-shadow: 0 0 0 2px rgba(99,102,241,.25) inset; background:rgba(99,102,241,.08); }

    #proc-controls { display:flex; gap:8px; padding:10px 12px; border-top:1px solid rgba(255,255,255,.08); }
    #proc-controls .btn {
      border:1px solid rgba(255,255,255,.15); border-radius:8px; padding:6px 10px; cursor:pointer; user-select:none;
      background:rgba(255,255,255,.06); text-align:center; flex:1;
    }
  </style>
</head>
<body>
  <div id="root">
    <div style="position:relative;">
      <div id="map"></div>

      <div id="hud">
        <h3>Semiconductor Supply Chain</h3>
        <div>Remove any single piece and the system strains or collapses.</div>
        <div class="row" style="margin-top:8px;">
          <div class="btn" id="play-seq">▶ Play Sequence</div>
          <div class="btn" id="reset">⟲ Reset</div>
        </div>
        <div style="margin-top:8px;"><b>Status:</b> <span id="note">ready</span></div>
      </div>

      <div id="mode-pill">Sequence focus mode</div>
      <div id="footer">Click any node to highlight its dependencies. Hover for details.</div>
    </div>

    <aside id="process">
      <header>
        <h4>How the chips get made</h4>
        <p class="sub">Current step highlights during sequence.</p>

        <div id="selectors">
          <div class="field">Focus:
            <select id="focus"><option value="">(none)</option></select>
          </div>
          <div class="field">Remove:
            <select id="remove"><option value="">(none)</option></select>
          </div>
          <label class="field"><input type="checkbox" id="toggle-arcs" checked /> Show flows</label>
          <label class="field"><input type="checkbox" id="dim-others" checked /> Dim unrelated</label>
        </div>

        <div id="legend"></div>
      </header>

      <div id="story">
        <ol class="flow" id="flow-list"></ol>
      </div>

      <div id="proc-controls">
        <div class="btn" id="prev-step">◀ Prev</div>
        <div class="btn" id="next-step">Next ▶</div>
      </div>
    </aside>
  </div>

  <script>
    const MAPBOX_TOKEN = {{ mapbox_token | tojson }};
    const STYLE_URL = "mapbox://styles/mapbox/dark-v10";
    const NODES = JSON.parse({{ nodes_json | tojson }});
    const LINKS = JSON.parse({{ links_json | tojson }});
    const CATEGORY_COLORS = JSON.parse({{ category_colors_json | tojson }});

    const nodeById = Object.fromEntries(NODES.map(n => [n.id, n]));
    const outEdges = {}; const inEdges = {};
    for (const n of NODES){ outEdges[n.id]=[]; inEdges[n.id]=[]; }
    for (const e of LINKS){
      if (nodeById[e.source] && nodeById[e.target]) {
        outEdges[e.source].push(e.target);
        inEdges[e.target].push(e.source);
      }
    }

    // === Your exact narrative with non-LinkedIn sources ===
    const PROCESS_STEPS = [
      {
        id:"TSMC",
        text:"TSMC, a company on a small island, produces over 90% of the world’s most advanced chips",
        sources:[{label:"[1]", url:"https://en.wikipedia.org/wiki/TSMC"}]
      },
      {
        id:"ASML",
        text:"TSMC relies on Dutch company ASML for EUV lithography machines",
        sources:[{label:"[2]", url:"https://www.asml.com/products/euv-lithography-systems"}]
      },
      {
        id:"ZEISS",
        text:"ASML depends on German company Carl Zeiss, the only firm capable of making mirrors precise enough for ASML’s requirements",
        sources:[{label:"[3]", url:"https://www.zeiss.com/semiconductor-manufacturing-technology/smt-magazine/euv-lithography-as-an-european-joint-project.html"}]
      },
      {
        id:"CYMER",
        text:"The light source for ASML’s EUV machines is produced by a single company in San Diego",
        sources:[{label:"[4]", url:"https://www.asml.com/company/about-asml/locations/san-diego"}]
      },
      {
        id:"JSR",
        text:"The photoresists used to print transistor patterns are produced by Japanese firms like JSR and Tokyo Ohka Kogyo",
        sources:[
          {label:"[5]", url:"https://www.tok-pr.com/en/products/photoresist.html"},
          {label:"[6]", url:"https://www.tok.co.jp/eng/products/semiconductor-pre"}
        ]
      },
      {
        id:"SPRUCE",
        text:"The ultra-pure quartz needed to make silicon wafers comes entirely from a single mine in Spruce Pine, North Carolina",
        sources:[
          {label:"[7]", url:"https://en.wikipedia.org/wiki/Spruce_Pine_Mining_District"},
          {label:"[8]", url:"https://www.sibelco.com/en/150-years/spruce-pine"}
        ]
      },
      {
        id:"CHILE",
        text:"The copper and rare-earth materials inside chips are mined and refined across Chile, the Congo, and China",
        sources:[
          {label:"[9]", url:"https://theconversation.com/china-and-the-us-are-in-a-race-for-critical-minerals-african-countries-need-to-make-the-rules-265318"},
          {label:"[10]", url:"https://www.npr.org/2025/03/16/nx-s1-5327095/copper-rare-earth-minerals-mining-electronics"}
        ]
      },
      {
        id:"UKR",
        text:"The specialized gases used in chipmaking, like neon and fluorine, largely come from Ukraine and Japan",
        sources:[
          {label:"[11]", url:"https://www.reuters.com/technology/exclusive-ukraine-halts-half-worlds-neon-output-chips-clouding-outlook-2022-03-11/"},
          {label:"[12]", url:"https://www.csis.org/blogs/perspectives-innovation/russias-invasion-ukraine-impacts-gas-markets-critical-chip-production"}
        ]
      },
      {
        id:"NVDA",
        text:"The design blueprints for these chips often come from American companies like NVIDIA, AMD, and Apple, which rely on software tools from U.S. firms like Synopsys and Cadence",
        sources:[{label:"[1]", url:"https://en.wikipedia.org/wiki/TSMC"}]
      }
    ].filter(s => nodeById[s.id]);

    // We keep the longer visualization sequence, but process list sticks to the points above.
    const sequence = ["TSMC","ASML","ZEISS","CYMER","JSR","SPRUCE","CHILE","UKR","NVDA"].filter(id => nodeById[id]);

    // UI
    const $ = sel => document.querySelector(sel);
    const $note = $("#note");
    const $pill = $("#mode-pill");
    const flowList = $("#flow-list");
    function setNote(t){ $note.textContent = t; }

    const focusSel = $("#focus");
    const removeSel = $("#remove");
    for (const n of NODES) {
      const opt1 = document.createElement('option'); opt1.value = n.id; opt1.textContent = n.name;
      const opt2 = document.createElement('option'); opt2.value = n.id; opt2.textContent = n.name;
      focusSel.appendChild(opt1); removeSel.appendChild(opt2);
    }

    // Legend
    const legend = $("#legend");
    const activeCats = new Set(Object.keys(CATEGORY_COLORS));
    for (const [cat, col] of Object.entries(CATEGORY_COLORS)) {
      const chip = document.createElement('label');
      chip.className = 'chip';
      chip.innerHTML = '<span class="dot" style="background:rgb('+col[0]+','+col[1]+','+col[2]+')"></span>'+cat+
        ' <input type="checkbox" checked data-cat="'+cat+'" style="margin-left:6px;">';
      legend.appendChild(chip);
    }
    legend.addEventListener('change', (e)=>{
      const t = e.target;
      if (t && t.type === 'checkbox' && t.dataset.cat) {
        if (t.checked) activeCats.add(t.dataset.cat); else activeCats.delete(t.dataset.cat);
        redraw();
      }
    });

    const showArcsEl = $("#toggle-arcs");
    const dimOthersEl = $("#dim-others");

    // Map
    mapboxgl.accessToken = MAPBOX_TOKEN;
    const map = new mapboxgl.Map({ container:'map', style:STYLE_URL, center:[{{ center_lng }}, {{ center_lat }}], zoom: {{ zoom }}, pitch: 35, bearing: 0 });
    const overlay = new deck.MapboxOverlay({ interleaved:false, layers:[] });
    map.addControl(overlay);

    // state
    let focusId = "";
    let removedId = "";
    let tStart = Date.now();
    let seqTimer = null;
    let seqIndex = -1;
    let isSeqPlaying = false;

    // Render process list with sources
    function renderFlowList(activeId=""){
      flowList.innerHTML = "";
      PROCESS_STEPS.forEach((s) => {
        const li = document.createElement('li');
        li.dataset.id = s.id;

        // sources anchors
        const srcHtml = (s.sources||[]).map(src => `<a class="src" href="${src.url}" target="_blank" rel="noopener">${src.label}</a>`).join(' ');

        li.innerHTML = `<b>${nodeById[s.id].name}</b>: ${s.text}
                        <span class="sources">${srcHtml}</span>
                        <span class="meta">${nodeById[s.id].country} · ${nodeById[s.id].category}</span>`;
        if (s.id === activeId) li.classList.add('active');
        li.addEventListener('click', ()=>{
          focusId = s.id; focusSel.value = focusId;
          isSeqPlaying = false; $pill.style.display = 'none';
          setNote('focus → '+nodeById[focusId].name);
          renderFlowList(focusId);
          redraw();
        });
        flowList.appendChild(li);
      });
      if (activeId){
        const el = flowList.querySelector('li.active');
        el && el.scrollIntoView({block:'center', behavior:'smooth'});
      }
    }
    renderFlowList();

    // visuals (crisp points, sequence focus mode)
    function categoryColor(cat){ return CATEGORY_COLORS[cat] || [255,255,255]; }
    function relatedToFocus(id){ return focusId && (id===focusId || outEdges[focusId].includes(id) || inEdges[focusId].includes(id)); }
    function visibleNode(n){
      if (!activeCats.has(n.category) || n.id === removedId) return false;
      if (isSeqPlaying && focusId) return relatedToFocus(n.id);
      return true;
    }
    function linkVisible(e){
      if (removedId && (e.source===removedId || e.target===removedId)) return false;
      const s = nodeById[e.source], t = nodeById[e.target];
      if (!visibleNode(s) || !visibleNode(t)) return false;
      if (isSeqPlaying && focusId) return (e.source===focusId || e.target===focusId);
      return true;
    }
    function relativeness(obj){
      if (isSeqPlaying) return 1.0;
      if (!dimOthersEl.checked || !focusId) return 1.0;
      if (obj.type === 'node') {
        if (obj.id === focusId) return 1.0;
        if (outEdges[focusId].includes(obj.id) || inEdges[focusId].includes(obj.id)) return 0.95;
        return 0.25;
      } else {
        if (obj.sourceId===focusId || obj.targetId===focusId) return 1.0;
        if (outEdges[focusId].includes(obj.sourceId) || inEdges[focusId].includes(obj.targetId)) return 0.85;
        return 0.18;
      }
    }
    function pulsingWidth(base){ const t = (Date.now() - tStart) / 1000; return base * (1.0 + 0.25 * Math.sin(t*2.0)); }
    function markerSizePx(weight){ return 5 + weight * 2; }
    function haloSizePx(weight){ return 10 + weight * 3.2; }
    function ringSizePx(weight){ return 12 + weight * 4.0; }

    function buildLayers(){
      const nodes = NODES.filter(visibleNode).map(n => ({
        ...n,
        position: [n.lng, n.lat],
        color: categoryColor(n.category),
        size_px: markerSizePx(n.weight),
        halo_px: haloSizePx(n.weight),
        ring_px: ringSizePx(n.weight)
      }));

      const arcsRaw = LINKS.filter(linkVisible);
      const arcs = arcsRaw.map(e => {
        const s = nodeById[e.source], t = nodeById[e.target];
        return {
          sourcePosition: [s.lng, s.lat],
          targetPosition: [t.lng, t.lat],
          sourceId: s.id, targetId: t.id,
          desc: e.desc,
          sName: s.name, tName: t.name
        };
      });

      const halos = new deck.ScatterplotLayer({
        id:'node-halos',
        data: nodes,
        pickable: false,
        radiusUnits: 'pixels',
        getPosition: d => d.position,
        getRadius: d => d.halo_px,
        stroked: false,
        filled: true,
        getFillColor: d => {
          const a = Math.floor(255 * 0.18 * relativeness({type:'node', id:d.id}));
          return [d.color[0], d.color[1], d.color[2], a];
        }
      });

      const scatter = new deck.ScatterplotLayer({
        id:'nodes',
        data: nodes,
        pickable: true,
        radiusUnits: 'pixels',
        getPosition: d => d.position,
        getRadius: d => d.size_px,
        filled: true,
        stroked: true,
        getFillColor: d => {
          const alpha = Math.floor(255 * Math.max(0.8, relativeness({type:'node', id:d.id})));
          return [d.color[0], d.color[1], d.color[2], alpha];
        },
        getLineColor: [255,255,255,140],
        lineWidthMinPixels: 1.2
      });

      const rings = new deck.ScatterplotLayer({
        id:'node-rings',
        data: nodes,
        pickable: false,
        radiusUnits: 'pixels',
        getPosition: d => d.position,
        getRadius: d => d.id===focusId ? pulsingWidth(d.ring_px) : 0,
        stroked: true,
        filled: false,
        getLineColor: d => [255,255,255,200],
        lineWidthMinPixels: 1.2
      });

      const text = new deck.TextLayer({
        id:'labels',
        data: nodes,
        pickable: false,
        getPosition: d => d.position,
        getText: d => d.name,
        getSize: 12,
        getColor: d => {
          const alpha = Math.floor(255 * Math.max(0.6, relativeness({type:'node', id:d.id})));
          return [235,235,235, alpha];
        },
        getTextAnchor: 'start',
        getAlignmentBaseline: 'center',
        getPixelOffset: [8,0]
      });

      const arcsLayer = new deck.ArcLayer({
        id:'arcs',
        data: showArcsEl.checked ? arcs : [],
        pickable: true,
        getSourcePosition: d => d.sourcePosition,
        getTargetPosition: d => d.targetPosition,
        getSourceColor: d => {
          const alpha = Math.floor(255 * relativeness({type:'link', sourceId:d.sourceId, targetId:d.targetId}));
          return [255,255,255, Math.max(70, alpha*0.6|0)];
        },
        getTargetColor: d => {
          const alpha = Math.floor(255 * relativeness({type:'link', sourceId:d.sourceId, targetId:d.targetId}));
          return [255,200,0, Math.max(110, alpha*0.8|0)];
        },
        getWidth: d => pulsingWidth(2.0)
      });

      return [halos, scatter, rings, text, arcsLayer];
    }

    function firstSourceFor(id){
      const step = PROCESS_STEPS.find(s => s.id===id);
      return step && step.sources && step.sources[0] ? step.sources[0].url : null;
    }

    function redraw(){
      overlay.setProps({
        layers: buildLayers(),
        getTooltip: ({object, layer})=>{
          if (!object) return null;
          if (layer && layer.id==='arcs') {
            return {html:
              '<b>'+object.sName+'</b> → <b>'+object.tName+'</b><br/>'+
              '<i>'+object.desc+'</i>', style:{fontSize:'12px'}
            };
          }
          if (layer && (layer.id==='nodes' || layer.id==='labels' || layer.id==='node-rings')) {
            const src = firstSourceFor(object.id);
            const linkHtml = src ? `<br/><a href="${src}" target="_blank" rel="noopener" style="color:#93c5fd;text-decoration:underline;">source</a>` : '';
            return {html:
              '<b>'+object.name+'</b><br/>'+
              object.role+'<br/>'+
              '<i>'+object.country+' · '+object.category+'</i>'+linkHtml, style:{fontSize:'12px'}
            };
          }
          return null;
        }
      });
    }

    map.on('load', ()=>{ overlay.setProps({ layers: buildLayers() }); });

    overlay._deck && overlay._deck.setProps({
      onClick: info => {
        if (info && info.object && info.layer && (info.layer.id==='nodes' || info.layer.id==='labels')) {
          focusId = info.object.id;
          focusSel.value = focusId;
          isSeqPlaying = false; $pill.style.display = 'none';
          setNote('focus → '+nodeById[focusId].name);
          renderFlowList(focusId);
          redraw();
        }
      }
    });

    focusSel.addEventListener('change', ()=>{
      focusId = focusSel.value || "";
      isSeqPlaying = false; $pill.style.display = 'none';
      setNote(focusId ? ('focus → '+nodeById[focusId].name) : 'focus cleared');
      renderFlowList(focusId);
      redraw();
    });

    removeSel.addEventListener('change', ()=>{
      removedId = removeSel.value || "";
      setNote(removedId ? ('removed → '+nodeById[removedId].name) : 'no removal');
      redraw();
    });

    $("#reset").addEventListener('click', ()=>{
      focusId = ""; removedId = ""; focusSel.value=""; removeSel.value="";
      seqIndex = -1; if (seqTimer) { clearInterval(seqTimer); seqTimer=null; }
      isSeqPlaying = false; $pill.style.display = 'none';
      setNote('reset');
      renderFlowList();
      redraw();
    });

    function goToIndex(i){
      seqIndex = i;
      if (seqIndex < 0) seqIndex = 0;
      if (seqIndex >= sequence.length) seqIndex = sequence.length-1;
      focusId = sequence[seqIndex];
      focusSel.value = focusId;
      setNote('sequence '+(seqIndex+1)+'/'+sequence.length+' — '+nodeById[focusId].name);
      renderFlowList(focusId);
      redraw();
    }

    $("#prev-step").addEventListener('click', ()=>{
      isSeqPlaying = false; $pill.style.display = 'none';
      if (seqIndex <= 0) seqIndex = 1;
      goToIndex(seqIndex-1);
    });
    $("#next-step").addEventListener('click', ()=>{
      isSeqPlaying = false; $pill.style.display = 'none';
      if (seqIndex < 0) seqIndex = -1;
      goToIndex(seqIndex+1);
    });

    $("#play-seq").addEventListener('click', ()=>{
      if (seqTimer) { clearInterval(seqTimer); seqTimer=null; }
      seqIndex = -1;
      isSeqPlaying = true; $pill.style.display = 'inline-block';
      seqTimer = setInterval(()=>{
        seqIndex++;
        if (seqIndex >= {{ (0) }} + {{ (1) }} * 0 + sequence.length) {
          clearInterval(seqTimer); seqTimer=null;
          isSeqPlaying = false; $pill.style.display = 'none';
          setNote('sequence done');
          redraw();
          return;
        }
        goToIndex(seqIndex);
      }, 950);
    });

    $("#toggle-arcs").addEventListener('change', redraw);
    $("#dim-others").addEventListener('change', redraw);

    (function tick(){ requestAnimationFrame(tick); redraw(); })();
  </script>
</body>
</html>
""").render(
        nodes_json=nodes_json,
        links_json=links_json,
        category_colors_json=category_colors_json,
        mapbox_token=mapbox_token,
        center_lng=center_lng,
        center_lat=center_lat,
        zoom=zoom
    )

    common = fused.load("https://github.com/fusedio/udfs/tree/abf9c87/public/common/")
    return common.html_to_obj(html)
