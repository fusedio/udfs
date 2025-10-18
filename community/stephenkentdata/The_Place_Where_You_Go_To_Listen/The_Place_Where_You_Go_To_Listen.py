@fused.udf(cache_max_age="10s")
def udf(location: str = "New York", format: str = "page"):
    """
    Returns HTML page OR JSON data for continuous updates.
    Uses hybrid local + regional + global seismic approach.
    """
    import pandas as pd
    import requests
    from datetime import datetime, timezone
    import math
    import json
    locations = {
        "New York": (40.7128, -74.0060),
        "California": (36.7783, -119.4179),
        "Alaska": (64.2008, -149.4937),
        "Texas": (31.9686, -99.9018),
        "Hawaii": (19.8968, -155.5828),
        "Colorado": (39.5501, -105.7821),
        "British Columbia": (53.7267, -127.6476),
        "Mexico City": (19.4326, -99.1332),
        "Iceland": (64.9631, -19.0208),
        "Norway": (60.4720, 8.4689),
        "Scotland": (56.4907, -4.2026),
        "Italy": (41.8719, 12.5674),
        "Greece": (39.0742, 21.8243),
        "Japan": (36.2048, 138.2529),
        "New Zealand": (-40.9006, 174.8860),
        "Indonesia": (-0.7893, 113.9213),
        "Philippines": (12.8797, 121.7740),
        "Chile": (-35.6751, -71.5430),
        "Peru": (-9.1900, -75.0152),
        "Antarctica": (-77.8463, 166.6686),
    }
    
    LAT, LON = locations.get(location, (40.7128, -74.0060))
    
    data = {
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'location': location,
        'lat': LAT,
        'lon': LON,
        'seismic_local': 0.0,
        'seismic_regional': 0.0,
        'seismic_global': 0.0,
        'aurora_intensity': 0.0,
        'aurora_kp': 0.0,
        'solar_brightness': 0.0,
        'solar_position': 0.0,
        'seasonal_position': 0.0
    }
    
    # SEISMIC - Multi-scale approach
    try:
        r = requests.get("https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson", timeout=10)
        if r.status_code == 200:
            quakes = r.json()['features']
            
            local_quakes = []
            regional_quakes = []
            global_max = 0
            
            for q in quakes:
                coords = q['geometry']['coordinates']
                q_lon, q_lat = coords[0], coords[1]
                mag = q['properties']['mag']
                
                if mag and mag > global_max:
                    global_max = mag
                
                dist = math.sqrt((LAT - q_lat)**2 + (LON - q_lon)**2) * 111
                
                if dist <= 500 and mag:
                    local_quakes.append(mag)
                elif dist <= 2000 and mag:
                    regional_quakes.append(mag)
            
            if local_quakes:
                data['seismic_local'] = min(sum(local_quakes) / len(local_quakes) / 5.0, 1.0)
            
            if regional_quakes:
                data['seismic_regional'] = min(sum(regional_quakes) / len(regional_quakes) / 6.0, 1.0)
            
            data['seismic_global'] = min(global_max / 8.0, 1.0)
            
    except Exception as e:
        print(f"Seismic error: {e}")
    
    # AURORA - with latitude weighting
    try:
        r = requests.get("https://services.swpc.noaa.gov/products/noaa-planetary-k-index.json", timeout=5)
        if r.status_code == 200 and len(r.json()) > 1:
            kp = float(r.json()[-1][1])
            data['aurora_kp'] = kp
            
            aurora_potential = 0
            if abs(LAT) > 50:
                aurora_potential = min((abs(LAT) - 50) / 40, 1.0)
            
            data['aurora_intensity'] = (kp / 9.0) * max(aurora_potential, 0.2)
    except:
        pass
    
    # SOLAR/TIME
    now = datetime.now(timezone.utc)
    hour = now.hour + now.minute / 60.0
    day_of_year = now.timetuple().tm_yday
    data['solar_position'] = hour / 24.0
    data['seasonal_position'] = day_of_year / 365.0
    
    hour_angle = (hour - 12) * 15
    decl = 23.45 * math.sin(math.radians((360/365) * (day_of_year - 81)))
    try:
        elev = math.asin(
            math.sin(math.radians(LAT)) * math.sin(math.radians(decl)) +
            math.cos(math.radians(LAT)) * math.cos(math.radians(decl)) * math.cos(math.radians(hour_angle))
        )
        data['solar_brightness'] = max(0, min(math.degrees(elev) / 90.0, 1.0))
    except:
        data['solar_brightness'] = 0
    
    if format == "data":
        return pd.DataFrame([data])
    
    location_options = "\\n".join([f'<option value="{loc}" {"selected" if loc == location else ""}>{loc}</option>' for loc in sorted(locations.keys())])
    
    html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>The Place Where You Go to Listen</title>
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{ font-family: -apple-system, sans-serif; background: #000; color: rgba(255,255,255,0.7); min-height: 100vh; overflow: hidden; }}
#canvas-container {{ position: fixed; top: 0; left: 0; width: 100%; height: 100%; z-index: 1; }}
canvas {{ display: block; width: 100%; height: 100%; }}
#ui {{ position: fixed; top: 20px; left: 20px; right: 20px; z-index: 10; display: flex; justify-content: space-between; align-items: flex-start; }}
.title-block {{ text-align: left; }}
h1 {{ font-size: 1.2em; font-weight: 400; color: rgba(255,255,255,0.8); margin-bottom: 5px; letter-spacing: 0.5px; }}
.credit {{ font-size: 0.7em; color: rgba(255,255,255,0.4); font-style: italic; }}
.credit a {{ color: rgba(255,255,255,0.5); text-decoration: none; border-bottom: 1px solid transparent; transition: all 0.3s; }}
.credit a:hover {{ color: rgba(255,255,255,0.8); border-bottom-color: rgba(255,255,255,0.8); }}
.controls {{ display: flex; gap: 15px; align-items: center; }}
select {{ padding: 8px 25px 8px 12px; font-size: 0.85em; background: rgba(0,0,0,0.5); border: 1px solid rgba(255,255,255,0.2); border-radius: 3px; color: rgba(255,255,255,0.8); cursor: pointer; outline: none; appearance: none; background-image: url('data:image/svg+xml;charset=UTF-8,<svg width="10" height="6" viewBox="0 0 10 6" fill="none" xmlns="http://www.w3.org/2000/svg"><path d="M1 1L5 5L9 1" stroke="rgba(255,255,255,0.5)" stroke-width="1.5" stroke-linecap="round"/></svg>'); background-repeat: no-repeat; background-position: right 8px center; }}
select:hover {{ background: rgba(0,0,0,0.7); border-color: rgba(255,255,255,0.4); }}
select option {{ background: #000; }}
button {{ padding: 8px 20px; font-size: 0.85em; background: rgba(255,255,255,0.1); border: 1px solid rgba(255,255,255,0.3); border-radius: 3px; color: rgba(255,255,255,0.8); cursor: pointer; transition: all 0.3s; }}
button:hover {{ background: rgba(255,255,255,0.2); border-color: rgba(255,255,255,0.5); }}
#data-panel {{ position: fixed; bottom: 20px; left: 20px; background: rgba(0,0,0,0.6); border: 1px solid rgba(255,255,255,0.15); border-radius: 3px; padding: 12px 15px; z-index: 10; max-width: 280px; }}
#data-toggle {{ cursor: pointer; font-size: 0.75em; color: rgba(255,255,255,0.6); user-select: none; display: flex; align-items: center; gap: 5px; }}
#data-toggle:hover {{ color: rgba(255,255,255,0.9); }}
#data-content {{ margin-top: 10px; font-size: 0.75em; }}
#data-content.collapsed {{ display: none; }}
.data-row {{ display: flex; justify-content: space-between; margin: 6px 0; color: rgba(255,255,255,0.5); }}
.data-label {{ color: rgba(255,255,255,0.4); }}
.data-value {{ color: rgba(255,255,255,0.7); font-variant-numeric: tabular-nums; }}
#status {{ position: fixed; bottom: 20px; right: 20px; font-size: 0.7em; color: rgba(255,255,255,0.3); z-index: 10; }}
</style>
</head>
<body>
<div id="canvas-container"><canvas id="visualizer"></canvas></div>
<div id="ui">
<div class="title-block">
<h1>The Place Where You Go to Listen</h1>
<p class="credit">Inspired by <a href="https://www.johnlutheradams.net/" target="_blank">John Luther Adams</a> • <a href="https://www.uaf.edu/centennial/uaf100/ideas/the-place.php" target="_blank">Original Installation</a></p>
</div>
<div class="controls">
<select id="location-select">{location_options}</select>
<button id="startBtn">Listen</button>
</div>
</div>
<div id="data-panel">
<div id="data-toggle">▼ Data</div>
<div id="data-content">
<div class="data-row"><span class="data-label">Local Seismic</span><span class="data-value" id="local-val">0.00</span></div>
<div class="data-row"><span class="data-label">Regional Seismic</span><span class="data-value" id="regional-val">0.00</span></div>
<div class="data-row"><span class="data-label">Global Seismic</span><span class="data-value" id="global-val">0.00</span></div>
<div class="data-row"><span class="data-label">Aurora</span><span class="data-value" id="aurora-val">0.00</span></div>
<div class="data-row"><span class="data-label">Solar</span><span class="data-value" id="solar-val">0.00</span></div>
</div>
</div>
<div id="status"></div>
<script>
let data = {json.dumps(data)};
let audioCtx, playing = false;
let localOsc, regionalOsc, globalOsc, auroraOsc1, auroraOsc2, noise;
let localGain, regionalGain, globalGain, auroraGain, noiseGain, noiseFilter, master;
let updateInterval;

document.getElementById('location-select').addEventListener('change', function() {{
const newLocation = this.value;
const baseUrl = window.location.href.split('?')[0];
const playingParam = playing ? '&playing=true' : '';
window.location.href = baseUrl + '?dtype_out_vector=html&location=' + encodeURIComponent(newLocation) + playingParam;
}});

const canvas = document.getElementById('visualizer');
const ctx = canvas.getContext('2d');
let animationId, time = 0;

function resizeCanvas() {{
canvas.width = window.innerWidth;
canvas.height = window.innerHeight;
}}
resizeCanvas();
window.addEventListener('resize', resizeCanvas);

function drawColorFields() {{
const w = canvas.width;
const h = canvas.height;
const solarBright = data.solar_brightness;
const auroraIntensity = data.aurora_intensity;
const localIntensity = data.seismic_local;
const regionalIntensity = data.seismic_regional;
const globalIntensity = data.seismic_global;

const r = 50 + solarBright * 150 + localIntensity * 100 + globalIntensity * 40 + auroraIntensity * 30;
const g = 40 + solarBright * 120 + regionalIntensity * 80 + auroraIntensity * 90;
const b = 35 + solarBright * 70 + auroraIntensity * 140 + globalIntensity * 20;

ctx.fillStyle = 'rgb(' + Math.min(255, r) + ',' + Math.min(255, g) + ',' + Math.min(255, b) + ')';
ctx.fillRect(0, 0, w, h);
}}

function animate() {{
drawColorFields();
time++;
animationId = requestAnimationFrame(animate);
}}

async function fetchData() {{
try {{
const baseUrl = window.location.href.split('?')[0];
const response = await fetch(baseUrl + '?dtype_out_vector=json&format=data&location=' + encodeURIComponent(data.location));
const result = await response.json();
if (result && result.length > 0) {{
data = result[0];
updateSound(data);
updateUI(data);
const now = new Date(data.timestamp);
document.getElementById('status').textContent = data.location + ' • ' + now.toLocaleTimeString();
}}
}} catch (e) {{
console.error('Update failed:', e);
}}
}}

document.getElementById('startBtn').onclick = () => {{
if (!playing) {{
initAudio();
updateSound(data);
animate();
playing = true;
document.getElementById('startBtn').textContent = 'Stop';
updateInterval = setInterval(fetchData, 60000);
fetchData();
}} else {{
if (audioCtx) {{
audioCtx.close();
audioCtx = null;
}}
if (animationId) {{
cancelAnimationFrame(animationId);
}}
if (updateInterval) {{
clearInterval(updateInterval);
}}
playing = false;
document.getElementById('startBtn').textContent = 'Listen';
document.getElementById('status').textContent = '';
}}
}};

const urlParams = new URLSearchParams(window.location.search);
if (urlParams.get('playing') === 'true') {{
setTimeout(() => {{
document.getElementById('startBtn').click();
}}, 100);
}}

document.getElementById('data-toggle').onclick = () => {{
const content = document.getElementById('data-content');
const toggle = document.getElementById('data-toggle');
content.classList.toggle('collapsed');
toggle.textContent = content.classList.contains('collapsed') ? '▶ Data' : '▼ Data';
}};

function updateUI(d) {{
document.getElementById('local-val').textContent = d.seismic_local.toFixed(2);
document.getElementById('regional-val').textContent = d.seismic_regional.toFixed(2);
document.getElementById('global-val').textContent = d.seismic_global.toFixed(2);
document.getElementById('aurora-val').textContent = d.aurora_intensity.toFixed(2);
document.getElementById('solar-val').textContent = d.solar_brightness.toFixed(2);
}}

updateUI(data);

function initAudio() {{
audioCtx = new (window.AudioContext || window.webkitAudioContext)();
master = audioCtx.createGain();
master.gain.value = 0.3;
master.connect(audioCtx.destination);

localOsc = audioCtx.createOscillator();
localOsc.type = 'sine';
localOsc.frequency.value = 18;
localGain = audioCtx.createGain();
localGain.gain.value = 0.2;
localOsc.connect(localGain).connect(master);
localOsc.start();

regionalOsc = audioCtx.createOscillator();
regionalOsc.type = 'triangle';
regionalOsc.frequency.value = 70;
regionalGain = audioCtx.createGain();
regionalGain.gain.value = 0.12;
regionalOsc.connect(regionalGain).connect(master);
regionalOsc.start();

globalOsc = audioCtx.createOscillator();
globalOsc.type = 'sine';
globalOsc.frequency.value = 250;
globalGain = audioCtx.createGain();
globalGain.gain.value = 0.08;
globalOsc.connect(globalGain).connect(master);
globalOsc.start();

auroraOsc1 = audioCtx.createOscillator();
auroraOsc2 = audioCtx.createOscillator();
auroraOsc1.type = 'sine';
auroraOsc2.type = 'sine';
auroraOsc1.frequency.value = 130;
auroraOsc2.frequency.value = 195;
auroraGain = audioCtx.createGain();
auroraGain.gain.value = 0.12;
auroraOsc1.connect(auroraGain);
auroraOsc2.connect(auroraGain);
auroraGain.connect(master);
auroraOsc1.start();
auroraOsc2.start();

const buf = audioCtx.createBuffer(1, audioCtx.sampleRate * 2, audioCtx.sampleRate);
const d = buf.getChannelData(0);
for (let i = 0; i < d.length; i++) d[i] = Math.random() * 2 - 1;

noise = audioCtx.createBufferSource();
noise.buffer = buf;
noise.loop = true;
noiseFilter = audioCtx.createBiquadFilter();
noiseFilter.type = 'lowpass';
noiseFilter.frequency.value = 150;
noiseGain = audioCtx.createGain();
noiseGain.gain.value = 0.03;
noise.connect(noiseFilter).connect(noiseGain).connect(master);
noise.start();
}}

function updateSound(d) {{
if (!audioCtx) return;
const t = audioCtx.currentTime;

const localFreq = 15 + d.seismic_local * 25;
localOsc.frequency.linearRampToValueAtTime(localFreq, t + 2);
localGain.gain.linearRampToValueAtTime(0.15 + d.seismic_local * 0.3, t + 2);

const regionalFreq = 60 + d.seismic_regional * 60;
regionalOsc.frequency.linearRampToValueAtTime(regionalFreq, t + 2);
regionalGain.gain.linearRampToValueAtTime(d.seismic_regional * 0.2, t + 2);

const globalFreq = 200 + d.seismic_global * 300;
globalOsc.frequency.linearRampToValueAtTime(globalFreq, t + 2);
globalGain.gain.linearRampToValueAtTime(d.seismic_global * 0.15, t + 2);

const auroraBase = 120 + d.aurora_intensity * 160;
auroraOsc1.frequency.linearRampToValueAtTime(auroraBase, t + 2);
auroraOsc2.frequency.linearRampToValueAtTime(auroraBase * 1.5, t + 2);
auroraGain.gain.linearRampToValueAtTime(d.aurora_intensity * 0.25, t + 2);

const filterFreq = 100 + d.solar_brightness * 1200;
noiseFilter.frequency.linearRampToValueAtTime(filterFreq, t + 2);
noiseGain.gain.linearRampToValueAtTime(0.02 + d.solar_brightness * 0.06, t + 2);
}}
</script>
</body>
</html>"""
    
    common = fused.load("https://github.com/fusedio/udfs/tree/abf9c87/public/common/")
    return common.html_to_obj(html)