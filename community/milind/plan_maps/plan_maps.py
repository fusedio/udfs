@fused.udf
def udf():
    """
    Modern Map Drawing UDF
    - MapLibre GL JS for beautiful vector maps
    - Terra Draw for smooth drawing experience
    - URL-based sharing with UZIP compression
    """
    
    html = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
    <title>Map Draw</title>
    
    <!-- MapLibre GL JS -->
    <link href="https://unpkg.com/maplibre-gl@4.1.2/dist/maplibre-gl.css" rel="stylesheet" />
    <script src="https://unpkg.com/maplibre-gl@4.1.2/dist/maplibre-gl.js"></script>
    
    <!-- Terra Draw -->
    <script src="https://unpkg.com/terra-draw@1.0.0-beta.1/dist/terra-draw.umd.js"></script>
    
    <!-- UZIP for compression -->
    <script src="https://cdn.jsdelivr.net/npm/uzip@0.20201231.0/UZIP.min.js"></script>
    
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        
        :root {
            --bg-glass: rgba(15, 15, 20, 0.85);
            --bg-glass-hover: rgba(25, 25, 35, 0.9);
            --border-glass: rgba(255, 255, 255, 0.08);
            --text-primary: #f0f0f5;
            --text-muted: #8888a0;
            --accent: #6366f1;
            --accent-glow: rgba(99, 102, 241, 0.4);
            --success: #22c55e;
            --danger: #ef4444;
        }
        
        body {
            font-family: 'SF Pro Display', -apple-system, BlinkMacSystemFont, 'Inter', sans-serif;
            background: #0a0a0f;
            overflow: hidden;
        }
        
        #map {
            width: 100vw;
            height: 100vh;
        }
        
        /* SVG Drawing Overlay for freehand */
        #drawingOverlay {
            position: absolute;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            pointer-events: none;
            z-index: 1;
        }
        
        #drawingOverlay.freehand-active {
            pointer-events: all;
            cursor: crosshair;
        }
        
        #drawingOverlay path {
            fill: none;
            stroke-linecap: round;
            stroke-linejoin: round;
        }
        
        /* Toolbar */
        .toolbar {
            position: fixed;
            top: 16px;
            left: 50%;
            transform: translateX(-50%);
            z-index: 1000;
            display: flex;
            gap: 4px;
            padding: 6px;
            background: var(--bg-glass);
            backdrop-filter: blur(20px);
            -webkit-backdrop-filter: blur(20px);
            border: 1px solid var(--border-glass);
            border-radius: 16px;
            box-shadow: 0 8px 32px rgba(0, 0, 0, 0.4);
        }
        
        .toolbar-group {
            display: flex;
            gap: 2px;
        }
        
        .toolbar-group + .toolbar-group {
            margin-left: 4px;
            padding-left: 8px;
            border-left: 1px solid var(--border-glass);
        }
        
        .tool-btn {
            display: flex;
            align-items: center;
            justify-content: center;
            width: 40px;
            height: 40px;
            border: none;
            border-radius: 10px;
            background: transparent;
            color: var(--text-muted);
            cursor: pointer;
            transition: all 0.15s ease;
            position: relative;
        }
        
        .tool-btn:hover {
            background: var(--bg-glass-hover);
            color: var(--text-primary);
        }
        
        .tool-btn.active {
            background: var(--accent);
            color: white;
            box-shadow: 0 0 20px var(--accent-glow);
        }
        
        .tool-btn svg {
            width: 20px;
            height: 20px;
        }
        
        .tool-btn:disabled {
            opacity: 0.3;
            cursor: not-allowed;
        }
        
        /* Color picker */
        .color-btn {
            width: 40px;
            height: 40px;
            border: 2px solid transparent;
            border-radius: 10px;
            background: var(--current-color, #6366f1);
            cursor: pointer;
            transition: all 0.15s ease;
        }
        
        .color-btn:hover {
            transform: scale(1.05);
        }
        
        .color-btn.active {
            border-color: white;
            box-shadow: 0 0 12px var(--current-color);
        }
        
        .color-popover {
            position: absolute;
            top: 100%;
            left: 50%;
            transform: translateX(-50%);
            margin-top: 8px;
            padding: 8px;
            background: var(--bg-glass);
            backdrop-filter: blur(20px);
            border: 1px solid var(--border-glass);
            border-radius: 12px;
            display: none;
            grid-template-columns: repeat(4, 1fr);
            gap: 6px;
            box-shadow: 0 8px 32px rgba(0, 0, 0, 0.4);
        }
        
        .color-popover.show {
            display: grid;
        }
        
        .color-option {
            width: 28px;
            height: 28px;
            border: 2px solid transparent;
            border-radius: 50%;
            cursor: pointer;
            transition: all 0.15s ease;
        }
        
        .color-option:hover {
            transform: scale(1.15);
        }
        
        .color-option.selected {
            border-color: white;
        }
        
        /* Stroke width picker */
        .stroke-popover {
            position: absolute;
            top: 100%;
            left: 50%;
            transform: translateX(-50%);
            margin-top: 8px;
            padding: 8px;
            background: var(--bg-glass);
            backdrop-filter: blur(20px);
            border: 1px solid var(--border-glass);
            border-radius: 12px;
            display: none;
            flex-direction: column;
            gap: 4px;
            box-shadow: 0 8px 32px rgba(0, 0, 0, 0.4);
        }
        
        .stroke-popover.show {
            display: flex;
        }
        
        .stroke-option {
            display: flex;
            align-items: center;
            gap: 10px;
            padding: 6px 12px;
            border: none;
            border-radius: 8px;
            background: transparent;
            color: var(--text-muted);
            cursor: pointer;
            font-size: 12px;
            white-space: nowrap;
        }
        
        .stroke-option:hover {
            background: var(--bg-glass-hover);
            color: var(--text-primary);
        }
        
        .stroke-option.selected {
            color: var(--accent);
        }
        
        .stroke-line {
            width: 40px;
            height: var(--stroke-height, 2px);
            background: currentColor;
            border-radius: 2px;
        }
        
        /* Toast notification */
        .toast {
            position: fixed;
            bottom: 24px;
            left: 50%;
            transform: translateX(-50%) translateY(100px);
            padding: 12px 20px;
            background: var(--bg-glass);
            backdrop-filter: blur(20px);
            border: 1px solid var(--border-glass);
            border-radius: 12px;
            color: var(--text-primary);
            font-size: 14px;
            opacity: 0;
            transition: all 0.3s ease;
            z-index: 1001;
        }
        
        .toast.show {
            transform: translateX(-50%) translateY(0);
            opacity: 1;
        }
        
        .toast.success { border-color: var(--success); }
        .toast.error { border-color: var(--danger); }
        
        /* Share modal */
        .modal-overlay {
            position: fixed;
            inset: 0;
            background: rgba(0, 0, 0, 0.6);
            backdrop-filter: blur(4px);
            display: none;
            align-items: center;
            justify-content: center;
            z-index: 1002;
        }
        
        .modal-overlay.show {
            display: flex;
        }
        
        .modal {
            background: var(--bg-glass);
            backdrop-filter: blur(20px);
            border: 1px solid var(--border-glass);
            border-radius: 16px;
            padding: 24px;
            max-width: 480px;
            width: 90%;
        }
        
        .modal h3 {
            color: var(--text-primary);
            margin-bottom: 16px;
            font-size: 18px;
            font-weight: 600;
        }
        
        .modal-input {
            width: 100%;
            padding: 12px;
            background: rgba(0, 0, 0, 0.3);
            border: 1px solid var(--border-glass);
            border-radius: 8px;
            color: var(--text-primary);
            font-family: monospace;
            font-size: 12px;
            margin-bottom: 16px;
        }
        
        .modal-actions {
            display: flex;
            gap: 8px;
            justify-content: flex-end;
        }
        
        .modal-btn {
            padding: 10px 20px;
            border: none;
            border-radius: 8px;
            font-size: 14px;
            font-weight: 500;
            cursor: pointer;
            transition: all 0.15s ease;
        }
        
        .modal-btn.primary {
            background: var(--accent);
            color: white;
        }
        
        .modal-btn.primary:hover {
            box-shadow: 0 0 20px var(--accent-glow);
        }
        
        .modal-btn.secondary {
            background: rgba(255, 255, 255, 0.1);
            color: var(--text-primary);
        }
        
        /* Mobile adjustments */
        @media (max-width: 640px) {
            .toolbar {
                top: auto;
                bottom: 16px;
                padding: 4px;
            }
            
            .tool-btn {
                width: 36px;
                height: 36px;
            }
            
            .tool-btn svg {
                width: 18px;
                height: 18px;
            }
        }
    </style>
</head>
<body>
    <div id="map"></div>
    <svg id="drawingOverlay">
        <defs id="arrowMarkers"></defs>
        <g id="pathGroup"></g>
    </svg>
    
    <div class="toolbar">
        <!-- Drawing tools -->
        <div class="toolbar-group">
            <button class="tool-btn" id="selectBtn" title="Select">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <path d="M3 3l7.07 16.97 2.51-7.39 7.39-2.51L3 3z"/>
                </svg>
            </button>
            <button class="tool-btn active" id="freehandBtn" title="Freehand">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <path d="M12 19l7-7 3 3-7 7-3-3z"/>
                    <path d="M18 13l-1.5-7.5L2 2l3.5 14.5L13 18l5-5z"/>
                </svg>
            </button>
            <button class="tool-btn" id="lineBtn" title="Line">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <line x1="5" y1="19" x2="19" y2="5"/>
                </svg>
            </button>
            <button class="tool-btn" id="polygonBtn" title="Polygon">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <polygon points="12 2 22 8.5 22 15.5 12 22 2 15.5 2 8.5"/>
                </svg>
            </button>
            <button class="tool-btn" id="rectangleBtn" title="Rectangle">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <rect x="3" y="3" width="18" height="18" rx="2"/>
                </svg>
            </button>
            <button class="tool-btn" id="circleBtn" title="Circle">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <circle cx="12" cy="12" r="10"/>
                </svg>
            </button>
            <button class="tool-btn" id="arrowBtn" title="Arrow">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <line x1="5" y1="19" x2="19" y2="5"/>
                    <polyline points="10 5 19 5 19 14"/>
                </svg>
            </button>
        </div>
        
        <!-- Actions -->
        <div class="toolbar-group">
            <button class="tool-btn" id="undoBtn" title="Undo" disabled>
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <path d="M3 7v6h6"/>
                    <path d="M21 17a9 9 0 00-9-9 9 9 0 00-6 2.3L3 13"/>
                </svg>
            </button>
            <button class="tool-btn" id="redoBtn" title="Redo" disabled>
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <path d="M21 7v6h-6"/>
                    <path d="M3 17a9 9 0 019-9 9 9 0 016 2.3l3 2.7"/>
                </svg>
            </button>
            <button class="tool-btn" id="clearBtn" title="Clear All">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <polyline points="3 6 5 6 21 6"/>
                    <path d="M19 6v14a2 2 0 01-2 2H7a2 2 0 01-2-2V6m3 0V4a2 2 0 012-2h4a2 2 0 012 2v2"/>
                </svg>
            </button>
        </div>
        
        <!-- Style -->
        <div class="toolbar-group">
            <div style="position: relative;">
                <button class="color-btn" id="colorBtn" title="Color"></button>
                <div class="color-popover" id="colorPopover"></div>
            </div>
            <div style="position: relative;">
                <button class="tool-btn" id="strokeBtn" title="Stroke Width">
                    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                        <line x1="4" y1="6" x2="20" y2="6" stroke-width="1"/>
                        <line x1="4" y1="12" x2="20" y2="12" stroke-width="2"/>
                        <line x1="4" y1="18" x2="20" y2="18" stroke-width="4"/>
                    </svg>
                </button>
                <div class="stroke-popover" id="strokePopover"></div>
            </div>
        </div>
        
        <!-- Share -->
        <div class="toolbar-group">
            <button class="tool-btn" id="shareBtn" title="Share">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <path d="M4 12v8a2 2 0 002 2h12a2 2 0 002-2v-8"/>
                    <polyline points="16 6 12 2 8 6"/>
                    <line x1="12" y1="2" x2="12" y2="15"/>
                </svg>
            </button>
        </div>
    </div>
    
    <div class="toast" id="toast"></div>
    
    <div class="modal-overlay" id="shareModal">
        <div class="modal">
            <h3>Share Drawing</h3>
            <input type="text" class="modal-input" id="shareUrl" readonly>
            <div class="modal-actions">
                <button class="modal-btn secondary" id="closeModalBtn">Close</button>
                <button class="modal-btn primary" id="copyUrlBtn">Copy URL</button>
            </div>
        </div>
    </div>

    <script>
        // ============= CONFIG =============
        const COLORS = [
            '#ef4444', '#f97316', '#eab308', '#22c55e',
            '#06b6d4', '#3b82f6', '#8b5cf6', '#ec4899',
            '#f43f5e', '#6366f1', '#14b8a6', '#84cc16',
            '#a855f7', '#0ea5e9', '#64748b', '#ffffff'
        ];
        
        const STROKES = [
            { width: 2, label: 'Fine' },
            { width: 4, label: 'Medium' },
            { width: 6, label: 'Bold' },
            { width: 10, label: 'Heavy' }
        ];
        
        // ============= STATE =============
        let map, draw;
        let currentColor = COLORS[5]; // Blue
        let currentStroke = STROKES[1].width; // Medium
        let currentMode = 'freehand';
        let undoStack = [];
        let redoStack = [];
        
        // Freehand drawing state
        let freehandPaths = []; // Array of {points: [{lat, lng}], color, stroke}
        let isDrawingFreehand = false;
        let currentFreehandPath = null;
        
        // ============= INIT =============
        function init() {
            initMap();
            initDraw();
            initUI();
            loadFromUrl();
            
            // Set freehand mode as default
            setMode('freehand');
        }
        
        function initMap() {
            map = new maplibregl.Map({
                container: 'map',
                style: {
                    version: 8,
                    sources: {
                        'carto-dark': {
                            type: 'raster',
                            tiles: [
                                'https://a.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}@2x.png',
                                'https://b.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}@2x.png',
                                'https://c.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}@2x.png'
                            ],
                            tileSize: 256,
                            attribution: '© CARTO © OpenStreetMap contributors'
                        }
                    },
                    layers: [{
                        id: 'carto-dark-layer',
                        type: 'raster',
                        source: 'carto-dark',
                        minzoom: 0,
                        maxzoom: 22
                    }]
                },
                center: [-122.4194, 37.7749],
                zoom: 12
            });
            
            map.addControl(new maplibregl.NavigationControl(), 'bottom-right');
            
            // Auto-save URL on map move
            map.on('moveend', () => {
                updateUrl();
            });
        }
        
        function initDraw() {
            const { TerraDraw, TerraDrawMapLibreGLAdapter,
                    TerraDrawLineStringMode, TerraDrawPolygonMode, TerraDrawRectangleMode,
                    TerraDrawCircleMode, TerraDrawSelectMode } = window.terraDraw;
            
            // Store color at draw time for each feature
            let drawingColor = currentColor;
            let drawingStroke = currentStroke;
            
            draw = new TerraDraw({
                adapter: new TerraDrawMapLibreGLAdapter({ map, coordinatePrecision: 9 }),
                modes: [
                    new TerraDrawSelectMode({
                        flags: {
                            linestring: { feature: { draggable: true, deletable: true } },
                            polygon: { feature: { draggable: true, deletable: true } },
                            rectangle: { feature: { draggable: true, deletable: true } },
                            circle: { feature: { draggable: true, deletable: true } }
                        }
                    }),
                    new TerraDrawLineStringMode(),
                    new TerraDrawPolygonMode(),
                    new TerraDrawRectangleMode(),
                    new TerraDrawCircleMode()
                ]
            });
            
            draw.start();
            
            // Track changes for undo
            draw.on('finish', () => {
                saveUndoState();
                updateUrl();
            });
            
            draw.on('change', () => {
                updateUndoRedoButtons();
            });
            
            // Initialize freehand drawing
            initFreehandDrawing();
            
            // Redraw freehand paths on map move/zoom
            map.on('move', redrawFreehandPaths);
            map.on('zoom', redrawFreehandPaths);
        }
        
        // ============= FREEHAND DRAWING =============
        function initFreehandDrawing() {
            const overlay = document.getElementById('drawingOverlay');
            
            // Mouse events
            overlay.addEventListener('mousedown', handleFreehandStart);
            overlay.addEventListener('mousemove', handleFreehandMove);
            overlay.addEventListener('mouseup', handleFreehandEnd);
            overlay.addEventListener('mouseleave', handleFreehandEnd);
            
            // Touch events
            overlay.addEventListener('touchstart', (e) => {
                e.preventDefault();
                handleFreehandStart(e);
            }, { passive: false });
            overlay.addEventListener('touchmove', (e) => {
                e.preventDefault();
                handleFreehandMove(e);
            }, { passive: false });
            overlay.addEventListener('touchend', (e) => {
                e.preventDefault();
                handleFreehandEnd(e);
            }, { passive: false });
        }
        
        function handleFreehandStart(e) {
            // Handle both freehand and arrow modes
            if (currentMode !== 'freehand' && currentMode !== 'arrow') return;
            
            isDrawingFreehand = true;
            const point = getEventPoint(e);
            const lngLat = map.unproject([point.x, point.y]);
            
            currentFreehandPath = {
                points: [{ lng: lngLat.lng, lat: lngLat.lat }],
                color: currentColor,
                stroke: currentStroke,
                type: currentMode // 'freehand' or 'arrow'
            };
            
            // Create SVG path element
            const pathEl = document.createElementNS('http://www.w3.org/2000/svg', 'path');
            pathEl.setAttribute('stroke', currentColor);
            pathEl.setAttribute('stroke-width', currentStroke);
            pathEl.id = 'currentPath';
            
            // Add arrowhead marker for arrow mode
            if (currentMode === 'arrow') {
                ensureArrowMarker(currentColor);
                pathEl.setAttribute('marker-end', `url(#arrow-${currentColor.replace('#', '')})`);
            }
            
            document.getElementById('pathGroup').appendChild(pathEl);
            
            updateCurrentPathElement();
        }
        
        function handleFreehandMove(e) {
            if (!isDrawingFreehand || !currentFreehandPath) return;
            
            const point = getEventPoint(e);
            const lngLat = map.unproject([point.x, point.y]);
            
            // For arrow mode, just keep start and end points (straight line)
            if (currentFreehandPath.type === 'arrow') {
                if (currentFreehandPath.points.length === 1) {
                    currentFreehandPath.points.push({ lng: lngLat.lng, lat: lngLat.lat });
                } else {
                    currentFreehandPath.points[1] = { lng: lngLat.lng, lat: lngLat.lat };
                }
            } else {
                currentFreehandPath.points.push({ lng: lngLat.lng, lat: lngLat.lat });
            }
            updateCurrentPathElement();
        }
        
        function handleFreehandEnd(e) {
            if (!isDrawingFreehand || !currentFreehandPath) return;
            
            isDrawingFreehand = false;
            
            // Only save if we have more than 1 point
            if (currentFreehandPath.points.length > 1) {
                freehandPaths.push(currentFreehandPath);
                saveUndoState();
                updateUrl();
            }
            
            currentFreehandPath = null;
            
            // Remove temp path and redraw all
            const tempPath = document.getElementById('currentPath');
            if (tempPath) tempPath.remove();
            
            redrawFreehandPaths();
        }
        
        // Create arrow marker for a specific color
        function ensureArrowMarker(color) {
            const markerId = `arrow-${color.replace('#', '')}`;
            if (document.getElementById(markerId)) return;
            
            const defs = document.getElementById('arrowMarkers');
            const marker = document.createElementNS('http://www.w3.org/2000/svg', 'marker');
            marker.setAttribute('id', markerId);
            marker.setAttribute('markerWidth', '10');
            marker.setAttribute('markerHeight', '7');
            marker.setAttribute('refX', '9');
            marker.setAttribute('refY', '3.5');
            marker.setAttribute('orient', 'auto');
            marker.setAttribute('markerUnits', 'strokeWidth');
            
            const polygon = document.createElementNS('http://www.w3.org/2000/svg', 'polygon');
            polygon.setAttribute('points', '0 0, 10 3.5, 0 7');
            polygon.setAttribute('fill', color);
            
            marker.appendChild(polygon);
            defs.appendChild(marker);
        }
        
        function getEventPoint(e) {
            const rect = map.getContainer().getBoundingClientRect();
            if (e.touches && e.touches.length > 0) {
                return {
                    x: e.touches[0].clientX - rect.left,
                    y: e.touches[0].clientY - rect.top
                };
            }
            return {
                x: e.clientX - rect.left,
                y: e.clientY - rect.top
            };
        }
        
        function updateCurrentPathElement() {
            if (!currentFreehandPath) return;
            
            const pathEl = document.getElementById('currentPath');
            if (!pathEl) return;
            
            const d = pointsToPathData(currentFreehandPath.points);
            pathEl.setAttribute('d', d);
        }
        
        function pointsToPathData(points) {
            if (!points || points.length === 0) return '';
            
            const screenPoints = points.map(p => map.project([p.lng, p.lat]));
            
            let d = `M ${screenPoints[0].x} ${screenPoints[0].y}`;
            for (let i = 1; i < screenPoints.length; i++) {
                d += ` L ${screenPoints[i].x} ${screenPoints[i].y}`;
            }
            return d;
        }
        
        function redrawFreehandPaths() {
            const group = document.getElementById('pathGroup');
            group.innerHTML = '';
            
            freehandPaths.forEach((path, index) => {
                const pathEl = document.createElementNS('http://www.w3.org/2000/svg', 'path');
                pathEl.setAttribute('stroke', path.color);
                pathEl.setAttribute('stroke-width', path.stroke);
                pathEl.setAttribute('d', pointsToPathData(path.points));
                pathEl.dataset.index = index;
                
                // Add arrowhead for arrow type
                if (path.type === 'arrow') {
                    ensureArrowMarker(path.color);
                    pathEl.setAttribute('marker-end', `url(#arrow-${path.color.replace('#', '')})`);
                }
                
                group.appendChild(pathEl);
            });
        }
        
        function initUI() {
            // Tool buttons
            const tools = {
                selectBtn: 'select',
                freehandBtn: 'freehand',
                lineBtn: 'linestring',
                polygonBtn: 'polygon',
                rectangleBtn: 'rectangle',
                circleBtn: 'circle',
                arrowBtn: 'arrow'
            };
            
            Object.entries(tools).forEach(([id, mode]) => {
                document.getElementById(id).addEventListener('click', () => setMode(mode));
            });
            
            // Action buttons
            document.getElementById('undoBtn').addEventListener('click', undo);
            document.getElementById('redoBtn').addEventListener('click', redo);
            document.getElementById('clearBtn').addEventListener('click', clearAll);
            document.getElementById('shareBtn').addEventListener('click', showShareModal);
            
            // Color picker
            initColorPicker();
            
            // Stroke picker
            initStrokePicker();
            
            // Share modal
            document.getElementById('closeModalBtn').addEventListener('click', hideShareModal);
            document.getElementById('copyUrlBtn').addEventListener('click', copyUrl);
            document.getElementById('shareModal').addEventListener('click', (e) => {
                if (e.target.id === 'shareModal') hideShareModal();
            });
            
            // Close popovers on outside click (with delay to prevent immediate close)
            document.addEventListener('click', (e) => {
                setTimeout(() => {
                    if (!e.target.closest('#colorBtn') && !e.target.closest('#colorPopover')) {
                        document.getElementById('colorPopover').classList.remove('show');
                    }
                    if (!e.target.closest('#strokeBtn') && !e.target.closest('#strokePopover')) {
                        document.getElementById('strokePopover').classList.remove('show');
                    }
                }, 0);
            });
        }
        
        function initColorPicker() {
            const popover = document.getElementById('colorPopover');
            const btn = document.getElementById('colorBtn');
            
            COLORS.forEach((color, i) => {
                const option = document.createElement('div');
                option.className = 'color-option' + (color === currentColor ? ' selected' : '');
                option.style.background = color;
                option.addEventListener('click', (e) => {
                    e.stopPropagation();
                    selectColor(color);
                });
                popover.appendChild(option);
            });
            
            btn.style.setProperty('--current-color', currentColor);
            btn.style.background = currentColor;
            btn.addEventListener('click', (e) => {
                e.stopPropagation();
                document.getElementById('strokePopover').classList.remove('show');
                popover.classList.toggle('show');
            });
        }
        
        function initStrokePicker() {
            const popover = document.getElementById('strokePopover');
            const btn = document.getElementById('strokeBtn');
            
            STROKES.forEach((stroke, i) => {
                const option = document.createElement('button');
                option.className = 'stroke-option' + (stroke.width === currentStroke ? ' selected' : '');
                option.innerHTML = `
                    <span class="stroke-line" style="--stroke-height: ${stroke.width}px"></span>
                    <span>${stroke.label}</span>
                `;
                option.addEventListener('click', () => selectStroke(stroke.width));
                popover.appendChild(option);
            });
            
            btn.addEventListener('click', () => {
                document.getElementById('colorPopover').classList.remove('show');
                popover.classList.toggle('show');
            });
        }
        
        // ============= DRAWING =============
        function setMode(mode) {
            currentMode = mode;
            
            const overlay = document.getElementById('drawingOverlay');
            
            // SVG overlay modes: freehand and arrow
            if (mode === 'freehand' || mode === 'arrow') {
                // Enable SVG overlay, disable Terra Draw
                overlay.classList.add('freehand-active');
                draw.setMode('static'); // Put Terra Draw in static mode
            } else {
                // Disable SVG overlay, enable Terra Draw
                overlay.classList.remove('freehand-active');
                draw.setMode(mode);
            }
            
            document.querySelectorAll('.toolbar-group:first-child .tool-btn').forEach(btn => {
                btn.classList.remove('active');
            });
            
            const btnMap = {
                select: 'selectBtn',
                freehand: 'freehandBtn',
                linestring: 'lineBtn',
                polygon: 'polygonBtn',
                rectangle: 'rectangleBtn',
                circle: 'circleBtn',
                arrow: 'arrowBtn'
            };
            
            document.getElementById(btnMap[mode])?.classList.add('active');
        }
        
        function selectColor(color) {
            currentColor = color;
            
            const btn = document.getElementById('colorBtn');
            btn.style.setProperty('--current-color', color);
            btn.style.background = color;
            
            document.querySelectorAll('.color-option').forEach((opt, i) => {
                opt.classList.toggle('selected', COLORS[i] === color);
            });
            
            document.getElementById('colorPopover').classList.remove('show');
        }
        
        function selectStroke(width) {
            currentStroke = width;
            
            document.querySelectorAll('.stroke-option').forEach((opt, i) => {
                opt.classList.toggle('selected', STROKES[i].width === width);
            });
            
            document.getElementById('strokePopover').classList.remove('show');
        }
        
        // ============= UNDO/REDO =============
        function saveUndoState() {
            const state = {
                terraDrawFeatures: draw.getSnapshot(),
                freehandPaths: JSON.parse(JSON.stringify(freehandPaths))
            };
            undoStack.push(JSON.stringify(state));
            redoStack = [];
            
            // Limit stack size
            if (undoStack.length > 50) undoStack.shift();
            
            updateUndoRedoButtons();
        }
        
        function undo() {
            if (undoStack.length === 0) return;
            
            // Save current state to redo stack
            const currentState = {
                terraDrawFeatures: draw.getSnapshot(),
                freehandPaths: JSON.parse(JSON.stringify(freehandPaths))
            };
            redoStack.push(JSON.stringify(currentState));
            
            // Restore previous state
            const previous = JSON.parse(undoStack.pop());
            
            draw.clear();
            if (previous.terraDrawFeatures) {
                previous.terraDrawFeatures.forEach(feature => draw.addFeatures([feature]));
            }
            
            freehandPaths = previous.freehandPaths || [];
            redrawFreehandPaths();
            
            updateUndoRedoButtons();
            updateUrl();
        }
        
        function redo() {
            if (redoStack.length === 0) return;
            
            // Save current state to undo stack
            const currentState = {
                terraDrawFeatures: draw.getSnapshot(),
                freehandPaths: JSON.parse(JSON.stringify(freehandPaths))
            };
            undoStack.push(JSON.stringify(currentState));
            
            // Restore next state
            const next = JSON.parse(redoStack.pop());
            
            draw.clear();
            if (next.terraDrawFeatures) {
                next.terraDrawFeatures.forEach(feature => draw.addFeatures([feature]));
            }
            
            freehandPaths = next.freehandPaths || [];
            redrawFreehandPaths();
            
            updateUndoRedoButtons();
            updateUrl();
        }
        
        function updateUndoRedoButtons() {
            document.getElementById('undoBtn').disabled = undoStack.length === 0;
            document.getElementById('redoBtn').disabled = redoStack.length === 0;
        }
        
        function clearAll() {
            if (draw.getSnapshot().length === 0 && freehandPaths.length === 0) return;
            
            saveUndoState();
            draw.clear();
            freehandPaths = [];
            redrawFreehandPaths();
            updateUrl();
            showToast('Drawing cleared', 'success');
        }
        
        // ============= URL SHARING =============
        function compressData(data) {
            const json = JSON.stringify(data);
            const bytes = new TextEncoder().encode(json);
            const compressed = UZIP.deflateRaw(bytes);
            return btoa(String.fromCharCode(...compressed))
                .replace(/\\+/g, '-')
                .replace(/\\//g, '_')
                .replace(/=+$/, '');
        }
        
        function decompressData(encoded) {
            try {
                const base64 = encoded.replace(/-/g, '+').replace(/_/g, '/');
                const padded = base64 + '='.repeat((4 - base64.length % 4) % 4);
                const binary = atob(padded);
                const bytes = new Uint8Array(binary.length);
                for (let i = 0; i < binary.length; i++) {
                    bytes[i] = binary.charCodeAt(i);
                }
                const inflated = UZIP.inflateRaw(bytes);
                const json = new TextDecoder().decode(inflated);
                return JSON.parse(json);
            } catch (e) {
                console.error('Failed to decompress:', e);
                return null;
            }
        }
        
        function updateUrl() {
            const center = map.getCenter();
            const zoom = map.getZoom();
            const features = draw.getSnapshot();
            
            const params = new URLSearchParams();
            params.set('c', `${center.lng.toFixed(4)},${center.lat.toFixed(4)}`);
            params.set('z', zoom.toFixed(1));
            
            if (features.length > 0 || freehandPaths.length > 0) {
                const data = {
                    f: features,
                    p: freehandPaths, // freehand paths
                    s: { color: currentColor, stroke: currentStroke }
                };
                params.set('d', compressData(data));
            }
            
            const url = `${location.origin}${location.pathname}#${params.toString()}`;
            history.replaceState(null, '', url);
        }
        
        function loadFromUrl() {
            const hash = location.hash.slice(1);
            if (!hash) return;
            
            try {
                const params = new URLSearchParams(hash);
                
                // Load view
                const center = params.get('c')?.split(',').map(Number);
                const zoom = parseFloat(params.get('z'));
                
                if (center && center.length === 2 && !isNaN(zoom)) {
                    map.setCenter([center[0], center[1]]);
                    map.setZoom(zoom);
                }
                
                // Load drawing
                const drawingData = params.get('d');
                if (drawingData) {
                    const data = decompressData(drawingData);
                    if (data) {
                        // Load Terra Draw features
                        if (data.f) {
                            data.f.forEach(feature => draw.addFeatures([feature]));
                        }
                        
                        // Load freehand paths
                        if (data.p) {
                            freehandPaths = data.p;
                            redrawFreehandPaths();
                        }
                        
                        // Load style settings
                        if (data.s) {
                            if (data.s.color) selectColor(data.s.color);
                            if (data.s.stroke) selectStroke(data.s.stroke);
                        }
                        
                        showToast('Drawing loaded!', 'success');
                    }
                }
            } catch (e) {
                console.error('Failed to load from URL:', e);
            }
        }
        
        function showShareModal() {
            updateUrl();
            document.getElementById('shareUrl').value = location.href;
            document.getElementById('shareModal').classList.add('show');
        }
        
        function hideShareModal() {
            document.getElementById('shareModal').classList.remove('show');
        }
        
        function copyUrl() {
            const input = document.getElementById('shareUrl');
            input.select();
            document.execCommand('copy');
            showToast('URL copied!', 'success');
            hideShareModal();
        }
        
        // ============= TOAST =============
        function showToast(message, type = '') {
            const toast = document.getElementById('toast');
            toast.textContent = message;
            toast.className = 'toast show' + (type ? ` ${type}` : '');
            
            setTimeout(() => {
                toast.classList.remove('show');
            }, 2500);
        }
        
        // ============= START =============
        document.addEventListener('DOMContentLoaded', init);
    </script>
</body>
</html>"""
    
    return html

