import fused

@fused.udf
def udf(
    path: str = "s3://fused-users/fused/sina/overture_overview/2024-09-18-0/hex4.parquet",
):

    data_url = fused.api.sign_url(path)
    print(data_url)
    import json
    
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8" />
        <title>SQL Query Viewer - Optimized</title>
        <meta name="viewport" content="initial-scale=1,maximum-scale=1,user-scalable=no" />
        
        <style>
            body {{
                margin: 0;
                padding: 0;
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                background: #1a1a1a;
                color: white;
                height: 100vh;
                overflow: hidden;
                position: relative;
            }}
            
            .results-container {{
                width: 100%;
                height: 100vh;
                background: #1a1a1a;
                overflow: hidden;
                position: relative;
            }}
            
            .table-container {{
                height: calc(100vh - 100px);
                overflow: auto;
                position: relative;
            }}
            
            .virtual-table {{
                position: relative;
                width: 100%;
            }}
            
            .table-header {{
                position: sticky;
                top: 0;
                z-index: 10;
                background: #333;
                border-bottom: 2px solid #E8FF59;
                display: flex;
                min-width: 100%;
            }}
            
            .table-cell {{
                padding: 8px 12px;
                border-right: 1px solid #444;
                font-family: 'Courier New', monospace;
                font-size: 12px;
                white-space: nowrap;
                overflow: hidden;
                text-overflow: ellipsis;
                min-width: 120px;
                max-width: 300px;
            }}
            
            .table-header .table-cell {{
                background: #333;
                color: #E8FF59;
                font-weight: 600;
                cursor: pointer;
                position: relative;
            }}
            
            .table-header .table-cell:hover {{
                background: #444;
            }}
            
            .table-row {{
                display: flex;
                min-width: 100%;
                border-bottom: 1px solid #333;
            }}
            
            .table-row:nth-child(even) {{
                background: rgba(255,255,255,0.02);
            }}
            
            .table-row:hover {{
                background: rgba(232,255,89,0.1);
            }}
            
            .row-number {{
                background: #2a2a2a;
                color: #666;
                min-width: 60px;
                max-width: 60px;
                text-align: center;
                font-size: 10px;
                position: sticky;
                left: 0;
                z-index: 5;
            }}
            
            .column-type {{
                font-size: 9px;
                color: #888;
                margin-left: 8px;
                opacity: 0.7;
            }}
            
            .query-container {{
                position: absolute;
                bottom: 0px;
                left: 50%;
                transform: translateX(-50%);
                background: rgba(0,0,0,0.5);
                padding: 4px 0px 0px 0px;
                border-radius: 6px 6px 0 0;
                z-index: 1000;
                backdrop-filter: blur(5px);
                display: block;
                width: 100%;
                border: 1px solid #444;
                transition: transform 0.3s ease;
            }}
            
            .query-container.minimized {{
                transform: translateX(-50%) translateY(100%);
                height: 0;
                overflow: hidden;
            }}
            
            .query-toggle {{
                position: absolute;
                bottom: 0px;
                right: 10px;
                background: rgba(0,0,0,0.3);
                color: rgba(255,255,255,0.6);
                border: 1px solid rgba(255,255,255,0.2);
                border-bottom: none;
                border-radius: 6px 6px 0 0;
                padding: 2px 6px;
                cursor: pointer;
                font-size: 8px;
                font-weight: 400;
                z-index: 1001;
                backdrop-filter: blur(5px);
            }}
            
            .query-toggle:hover {{
                background: rgba(0,0,0,0.95);
            }}
            
            .query-container textarea {{
                width: 100%;
                background: transparent;
                color: white;
                border: none;
                border-radius: 0;
                padding: 4px 0px 0px 0px;
                font-family: 'Courier New', monospace;
                font-size: 13px;
                resize: none;
                min-height: 60px;
                max-height: 300px;
                overflow-y: auto;
                line-height: 1.4;
                box-sizing: border-box;
            }}
            
            .query-container textarea:focus {{
                border-color: #E8FF59;
                outline: none;
            }}
            
            .status-info {{
                position: absolute;
                bottom: 70px;
                left: 10px;
                background: rgba(0,0,0,0.8);
                color: white;
                padding: 8px 12px;
                border-radius: 4px;
                font-size: 13px;
                z-index: 1000;
                backdrop-filter: blur(5px);
                display: block;
                transition: bottom 0.3s ease;
            }}
            
            .loading {{
                display: flex;
                align-items: center;
                justify-content: center;
                padding: 40px;
                color: #E8FF59;
                font-size: 16px;
            }}
            
            .spinner {{
                display: inline-block;
                width: 20px;
                height: 20px;
                border: 2px solid rgba(232,255,89,0.3);
                border-radius: 50%;
                border-top-color: #E8FF59;
                animation: spin 1s linear infinite;
                margin-right: 10px;
            }}
            
            @keyframes spin {{
                to {{ transform: rotate(360deg); }}
            }}
            
            .no-results {{
                text-align: center;
                padding: 40px;
                color: #666;
                font-style: italic;
            }}
            
            .error-details {{
                background: rgba(255,107,107,0.1);
                border: 1px solid #ff6b6b;
                border-radius: 4px;
                padding: 12px;
                margin-top: 12px;
                font-family: 'Courier New', monospace;
                font-size: 12px;
                color: #ff6b6b;
                white-space: pre-wrap;
            }}
            
            .table-stats {{
                position: absolute;
                bottom: 0;
                left: 0;
                right: 0;
                background: rgba(0,0,0,0.8);
                padding: 8px 16px;
                font-size: 12px;
                color: #ccc;
                border-top: 1px solid #444;
                z-index: 1000;
            }}
            
            .progress-bar {{
                position: absolute;
                top: 0;
                left: 0;
                height: 2px;
                background: #E8FF59;
                transition: width 0.3s ease;
                z-index: 2000;
            }}
        </style>
    </head>
    <body>
        <div class="loading" id="loadingText">
            <div class="spinner"></div>
            Initializing DuckDB...
        </div>
        
        <div class="progress-bar" id="progressBar" style="width: 0%"></div>
        <div class="status-info" id="statusInfo">Initializing...</div>
        <button class="query-toggle" id="queryToggle" onclick="toggleQueryContainer()">▼</button>
        <div class="query-container" id="queryContainer">
            <textarea 
                id="queryInput" 
                placeholder="SELECT *
FROM @{path}
LIMIT 100;">SELECT *
FROM @{path}
LIMIT 100;</textarea>
        </div>
        
        <div class="results-container" id="resultsContainer">
            <div class="no-results">
                <h3>Initializing DuckDB...</h3>
                <p>Please wait while we set up the database</p>
            </div>
        </div>

        <script type="module">
            // ========================================
            // ENHANCED STATE MANAGEMENT
            // ========================================
            
            // Query state management
            let currentQuery = `SELECT *
FROM @{path}
LIMIT 100;`;
            let queryTimeout = null;
            let currentAbortController = null;
            
            // DuckDB state
            let duckDB = null;
            let connection = null;
            let isDuckDBReady = false;
            
            // Data state
            let currentArrowTable = null;
            let currentColumns = [];
            let virtualizer = null;
            
            // Cache state
            const queryCache = new Map();
            const urlCache = new Map();
            let urlTableCounter = 0;
            
            // UI elements
            const statusInfo = document.getElementById('statusInfo');
            const loadingText = document.getElementById('loadingText');
            const resultsContainer = document.getElementById('resultsContainer');
            const queryInput = document.getElementById('queryInput');
            const progressBar = document.getElementById('progressBar');

            // ========================================
            // ENHANCED DUCKDB INITIALIZATION
            // ========================================
            
            async function initDuckDB() {{
                try {{
                    updateProgress(10);
                    statusInfo.textContent = 'Loading DuckDB WASM...';
                    
                    const initStartTime = performance.now();
                    
                    const duckdbModule = await import('https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.1-dev249.0/+esm');
                    updateProgress(30);
                    
                    const bundles = duckdbModule.getJsDelivrBundles();
                    const bundle = await duckdbModule.selectBundle(bundles);
                    
                    if (!bundle.mainWorker) {{
                        throw new Error('No worker bundle found for DuckDB');
                    }}
                    if (!bundle.mainModule) {{
                        throw new Error('No main module bundle found for DuckDB');
                    }}
                    
                    updateProgress(50);
                    
                    // Create optimized worker with proper URL handling
                    const workerUrl = bundle.mainWorker.startsWith('http') 
                        ? bundle.mainWorker 
                        : new URL(bundle.mainWorker, globalThis.location.origin).href;
                    
                    const workerBlob = new Blob([`importScripts("${{workerUrl}}");`], {{
                        type: 'text/javascript'
                    }});
                    const workerBlobUrl = URL.createObjectURL(workerBlob);
                    const worker = new Worker(workerBlobUrl);
                    updateProgress(70);
                    
                    const logger = new duckdbModule.ConsoleLogger();
                    duckDB = new duckdbModule.AsyncDuckDB(logger, worker);
                    
                    const mainModuleUrl = bundle.mainModule.startsWith('http')
                        ? bundle.mainModule
                        : new URL(bundle.mainModule, globalThis.location.origin).href;
                    
                    await duckDB.instantiate(mainModuleUrl);
                    URL.revokeObjectURL(workerBlobUrl);
                    updateProgress(85);
                    
                    await duckDB.open({{
                        path: ':memory:',
                        config: {{
                            'memory_limit': '2GB',
                            'threads': '4'
                        }}
                    }});
                    
                    connection = await duckDB.connect();
                    updateProgress(100);
                    
                    const initTime = performance.now() - initStartTime;
                    isDuckDBReady = true;
                    
                    statusInfo.textContent = `DuckDB ready (${{initTime.toFixed(0)}}ms)`;
                    console.log(`[DUCKDB] ✅ Enhanced initialization completed in ${{initTime.toFixed(0)}}ms`);
                    
                    onDuckDBReady();
                    
                }} catch (error) {{
                    console.error('[DUCKDB] Enhanced initialization failed:', error);
                    console.log('[DUCKDB] Falling back to simple initialization...');
                    
                    // Fallback to simple initialization
                    try {{
                        await initDuckDBSimple();
                    }} catch (fallbackError) {{
                        console.error('[DUCKDB] Fallback initialization also failed:', fallbackError);
                        statusInfo.textContent = `DuckDB error: ${{fallbackError.message}}`;
                        showError('DuckDB Initialization Failed', fallbackError.message, fallbackError.stack);
                    }}
                }} finally {{
                    setTimeout(() => updateProgress(0), 1000);
                }}
            }}
            
            function updateProgress(percent) {{
                progressBar.style.width = percent + '%';
            }}
            
            async function initDuckDBSimple() {{
                try {{
                    statusInfo.textContent = 'Loading DuckDB (simple mode)...';
                    updateProgress(20);
                    
                    const duckdbModule = await import('https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.1-dev249.0/+esm');
                    updateProgress(40);
                    
                    const bundles = duckdbModule.getJsDelivrBundles();
                    const bundle = await duckdbModule.selectBundle(bundles);
                    updateProgress(60);
                    
                    const worker = new Worker(bundle.mainWorker);
                    updateProgress(70);
                    
                    const logger = new duckdbModule.ConsoleLogger();
                    duckDB = new duckdbModule.AsyncDuckDB(logger, worker);
                    
                    await duckDB.instantiate(bundle.mainModule);
                    await duckDB.open({{ path: ':memory:' }});
                    updateProgress(85);
                    
                    connection = await duckDB.connect();
                    updateProgress(100);
                    
                    isDuckDBReady = true;
                    statusInfo.textContent = 'DuckDB ready (simple mode)';
                    console.log('[DUCKDB] ✅ Simple initialization completed');
                    
                    onDuckDBReady();
                    
                }} catch (error) {{
                    console.error('[DUCKDB] Simple initialization failed:', error);
                    throw error;
                }}
            }}
            
            function onDuckDBReady() {{
                loadingText.style.display = 'none';
                statusInfo.style.display = 'block';
                document.getElementById('queryContainer').style.display = 'block';
                
                setupQueryInput();
                showEmptyState();
                
                setTimeout(() => {{
                    const textarea = document.getElementById('queryInput');
                    if (textarea) {{
                        updateStatusInfoPosition(textarea.offsetHeight);
                    }}
                }}, 200);
            }}

            // ========================================
            // ENHANCED @URL PROCESSING WITH ARROW
            // ========================================
            
            // ========================================
            // URL MAPPING AND S3 PATH HANDLING
            // ========================================
            
            // Mapping from clean S3 paths to signed URLs
            const s3PathToSignedUrl = new Map();
            s3PathToSignedUrl.set('{path}', '{data_url}');
            
            function extractURLsFromQuery(query) {{
                // Extract both HTTP URLs and S3 paths
                const httpUrlRegex = /@(https?:\\/\\/[^\\s]+)/gi;
                const s3PathRegex = /@(s3:\\/\\/[^\\s]+)/gi;
                const pathVariableRegex = /@\\{{([^}}]+)\\}}/gi;
                
                const urls = [];
                let match;
                
                // Extract HTTP URLs
                while ((match = httpUrlRegex.exec(query)) !== null) {{
                    urls.push({{ type: 'http', value: match[1] }});
                }}
                
                // Extract S3 paths
                query = query.replace(httpUrlRegex, ''); // Reset regex
                while ((match = s3PathRegex.exec(query)) !== null) {{
                    urls.push({{ type: 's3', value: match[1] }});
                }}
                
                // Extract path variables like {{path}}
                query = query.replace(s3PathRegex, ''); // Reset regex
                while ((match = pathVariableRegex.exec(query)) !== null) {{
                    const varName = match[1];
                    if (s3PathToSignedUrl.has(varName)) {{
                        const signedUrl = s3PathToSignedUrl.get(varName);
                        urls.push({{ type: 'http', value: signedUrl, displayAs: `${{varName}}` }});
                    }}
                }}
                
                return urls;
            }}
            
            function getSignedUrlForS3Path(s3Path) {{
                // In a real implementation, this would call your signing service
                // For now, we'll use the existing signed URL if it matches the path pattern
                if (s3Path.includes('{path}') || s3PathToSignedUrl.has(s3Path)) {{
                    return s3PathToSignedUrl.get('{path}') || s3PathToSignedUrl.get(s3Path);
                }}
                
                // If no mapping found, return the S3 path as-is (will likely fail but shows the issue)
                console.warn(`No signed URL mapping found for S3 path: ${{s3Path}}`);
                return s3Path;
            }}
            
            function detectFormatFromURL(url) {{
                try {{
                    const urlObj = new URL(url);
                    
                    // First check the URL path for file extensions (more reliable)
                    const pathname = urlObj.pathname.toLowerCase();
                    
                    // Text formats
                    if (pathname.endsWith('.json')) return 'json';
                    if (pathname.endsWith('.geojson')) return 'geojson';
                    if (pathname.endsWith('.csv')) return 'csv';
                    if (pathname.endsWith('.tsv')) return 'tsv';
                    if (pathname.endsWith('.txt')) return 'txt';
                    
                    // Binary formats supported by DuckDB
                    if (pathname.endsWith('.parquet')) return 'parquet';
                    if (pathname.endsWith('.arrow')) return 'arrow';
                    if (pathname.endsWith('.feather')) return 'feather';
                    if (pathname.endsWith('.ipc')) return 'arrow'; // Arrow IPC format
                    
                    // Excel formats (binary)
                    if (pathname.endsWith('.xlsx')) return 'xlsx';
                    if (pathname.endsWith('.xls')) return 'xls';
                    
                    // Fallback to query parameters if no extension found
                    const params = urlObj.searchParams;
                    if (params.has('dtype_out_vector')) {{
                        const format = params.get('dtype_out_vector').toLowerCase();
                        return format === 'json' ? 'json' : 
                               format === 'csv' ? 'csv' : 
                               format === 'parquet' ? 'parquet' : 
                               format === 'geojson' ? 'geojson' : 'auto';
                    }}
                    
                    return 'auto';
                }} catch (error) {{
                    return 'auto';
                }}
            }}
            
            // Check if a format is binary (has ArrayBuffer issues)
            function isBinaryFormat(format) {{
                const binaryFormats = ['parquet', 'arrow', 'feather', 'xlsx', 'xls'];
                return binaryFormats.includes(format);
            }}
            
            async function loadDataFromURL(url, signal) {{
                console.log('[URL] Loading data from:', url);
                
                const detectedFormat = detectFormatFromURL(url);
                
                // For binary files, don't use cache to avoid ArrayBuffer detachment issues
                if (!isBinaryFormat(detectedFormat) && urlCache.has(url)) {{
                    console.log('[URL] Using cached data for:', url);
                    return urlCache.get(url);
                }}
                
                try {{
                    const response = await fetch(url, {{ signal }});
                    if (!response.ok) {{
                        throw new Error(`HTTP ${{response.status}}: ${{response.statusText}}`);
                    }}
                    
                    let data;
                    if (isBinaryFormat(detectedFormat)) {{
                        // Load binary files as ArrayBuffer - don't cache due to detachment issues
                        const arrayBuffer = await response.arrayBuffer();
                        data = {{ _isBinary: true, buffer: new Uint8Array(arrayBuffer), format: detectedFormat }};
                        console.log(`[URL] Successfully loaded ${{detectedFormat}} data (no cache)`);
                        return {{ data, format: detectedFormat }};
                    }} else if (detectedFormat === 'json' || detectedFormat === 'geojson') {{
                        data = await response.json();
                    }} else if (detectedFormat === 'csv') {{
                        const text = await response.text();
                        data = parseCSV(text);
                    }} else {{
                        // Default to JSON
                        data = await response.json();
                    }}
                    
                    console.log(`[URL] Successfully loaded data as ${{detectedFormat}}`);
                    urlCache.set(url, {{ data, format: detectedFormat }});
                    return {{ data, format: detectedFormat }};
                    
                }} catch (error) {{
                    if (error.name === 'AbortError') {{
                        throw new Error('Data loading was cancelled');
                    }}
                    console.error(`[URL] Failed to load ${{url}}:`, error);
                    throw error;
                }}
            }}
            
            function parseCSV(text) {{
                const lines = text.trim().split('\\n');
                if (lines.length === 0) return [];
                
                const headers = lines[0].split(',').map(h => h.trim().replace(/"/g, ''));
                const rows = [];
                
                for (let i = 1; i < lines.length; i++) {{
                    const values = lines[i].split(',');
                    const row = {{}};
                    headers.forEach((header, index) => {{
                        let value = values[index] ? values[index].trim().replace(/"/g, '') : '';
                        if (!isNaN(value) && value !== '') {{
                            value = parseFloat(value);
                        }}
                        row[header] = value;
                    }});
                    rows.push(row);
                }}
                
                return rows;
            }}
            
            async function registerURLDataInDuckDB(url, tableName, signal) {{
                const urlData = await loadDataFromURL(url, signal);
                const {{ data, format }} = urlData;
                
                console.log(`[URL] Registering ${{tableName}} from ${{url}} (format: ${{format}})`);
                
                if (isBinaryFormat(format)) {{
                    // Use efficient binary loading for binary formats
                    try {{
                        const fileExtension = format === 'arrow' ? 'arrow' : 
                                             format === 'feather' ? 'feather' : 
                                             format === 'xlsx' ? 'xlsx' : 
                                             format === 'xls' ? 'xls' : 'parquet';
                        
                        await duckDB.registerFileBuffer(`${{tableName}}.${{fileExtension}}`, data.buffer);
                        
                        // Use appropriate DuckDB reader function for each format
                        let readFunction;
                        switch (format) {{
                            case 'parquet':
                                readFunction = `read_parquet('${{tableName}}.parquet')`;
                                break;
                            case 'arrow':
                                readFunction = `read_parquet('${{tableName}}.arrow')`;
                                break;
                            case 'feather':
                                readFunction = `read_parquet('${{tableName}}.feather')`;
                                break;
                            case 'xlsx':
                                readFunction = `read_csv('${{tableName}}.xlsx', auto_detect=true)`;
                                break;
                            case 'xls':
                                readFunction = `read_csv('${{tableName}}.xls', auto_detect=true)`;
                                break;
                            default:
                                readFunction = `read_parquet('${{tableName}}.parquet')`;
                        }}
                        
                        await connection.query(`CREATE TABLE ${{tableName}} AS SELECT * FROM ${{readFunction}}`);
                    }} catch (error) {{
                        console.error(`[URL] Failed to register ${{format}} buffer for ${{tableName}}:`, error);
                        throw error;
                    }}
                }} else {{
                    let jsonData = data;
                    
                    // Convert GeoJSON features to flat objects
                    if (data.features && Array.isArray(data.features)) {{
                        jsonData = data.features.map(feature => ({{
                            ...feature.properties,
                            geometry_type: feature.geometry?.type,
                            longitude: feature.geometry?.coordinates?.[0],
                            latitude: feature.geometry?.coordinates?.[1],
                            lng: feature.geometry?.coordinates?.[0],
                            lat: feature.geometry?.coordinates?.[1]
                        }}));
                    }}
                    
                    if (!Array.isArray(jsonData)) {{
                        jsonData = [jsonData];
                    }}
                    
                    if (jsonData.length === 0) {{
                        throw new Error('No data found in URL');
                    }}
                    
                    // Use Arrow-optimized loading when possible
                    await duckDB.registerFileText(`${{tableName}}.json`, JSON.stringify(jsonData));
                    await connection.query(`CREATE TABLE ${{tableName}} AS SELECT * FROM read_json_auto('${{tableName}}.json')`);
                }}
                
                // Get row count efficiently
                const countResult = await connection.query(`SELECT COUNT(*) as count FROM ${{tableName}}`);
                const rowCount = countResult.getChild('count').get(0);
                
                console.log(`[URL] Registered ${{tableName}} with ${{rowCount}} rows`);
                return {{ tableName, rowCount, format }};
            }}
            
            async function processQueryWithURLs(query, signal) {{
                const urlEntries = extractURLsFromQuery(query);
                
                if (urlEntries.length === 0) {{
                    return {{ processedQuery: query, tableInfo: [] }};
                }}
                
                console.log('[URL] Found URL entries in query:', urlEntries);
                
                let processedQuery = query;
                const tableInfo = [];
                
                for (const urlEntry of urlEntries) {{
                    try {{
                        urlTableCounter++;
                        const tableName = `url_table_${{urlTableCounter}}`;
                        
                        let actualUrl = urlEntry.value;
                        let displayUrl = urlEntry.displayAs || urlEntry.value;
                        
                        // Handle S3 paths by getting signed URLs
                        if (urlEntry.type === 's3') {{
                            actualUrl = getSignedUrlForS3Path(urlEntry.value);
                            displayUrl = urlEntry.value; // Keep showing the clean S3 path
                        }}
                        
                        console.log(`[URL] Processing: Display="${{displayUrl}}", Actual="${{actualUrl.substring(0, 50)}}..."`);
                        
                        const info = await registerURLDataInDuckDB(actualUrl, tableName, signal);
                        tableInfo.push({{ 
                            url: actualUrl, 
                            displayUrl: displayUrl,
                            tableName,
                            ...info 
                        }});
                        
                        // Replace the original reference with table name
                        let searchPattern;
                        if (urlEntry.displayAs) {{
                            // Handle variables like @{{path}}
                            searchPattern = `@\\{{${{urlEntry.displayAs}}\\}}`;
                        }} else if (urlEntry.type === 's3') {{
                            // Handle S3 paths like @s3://bucket/file
                            searchPattern = `@${{urlEntry.value.replace(/[.*+?^${{}}()|[\\]\\\\]/g, '\\\\$&')}}`;
                        }} else {{
                            // Handle HTTP URLs
                            searchPattern = `@${{urlEntry.value.replace(/[.*+?^${{}}()|[\\]\\\\]/g, '\\\\$&')}}`;
                        }}
                        
                        const regex = new RegExp(searchPattern, 'gi');
                        processedQuery = processedQuery.replace(regex, tableName);
                        
                        console.log(`[URL] ${{tableName}} ready for ${{displayUrl}}`);
                        
                    }} catch (error) {{
                        console.error(`[URL] Failed to load ${{urlEntry.value}}:`, error);
                        throw error;
                    }}
                }}
                
                console.log('[URL] Processed query:', processedQuery);
                return {{ processedQuery, tableInfo }};
            }}

            // ========================================
            // ENHANCED QUERY EXECUTION WITH ARROW
            // ========================================
            
            async function executeQuery(query) {{
                if (!isDuckDBReady || !connection) {{
                    statusInfo.textContent = 'DuckDB not ready for queries';
                    showError('DuckDB not ready', 'Please wait for DuckDB to initialize');
                    return;
                }}
                
                if (!query || query.trim() === '') {{
                    showEmptyState();
                    return;
                }}
                
                // Cancel previous query
                if (currentAbortController) {{
                    currentAbortController.abort();
                }}
                
                currentAbortController = new AbortController();
                const signal = currentAbortController.signal;
                
                // Clean up tables from previous queries
                try {{
                    const tables = await connection.query("SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'url_table_%'");
                    for (let i = 0; i < tables.numRows; i++) {{
                        const tableName = tables.getChild('table_name').get(i);
                        await connection.query(`DROP TABLE IF EXISTS ${{tableName}}`);
                    }}
                }} catch (error) {{
                    console.warn('[CLEANUP] Failed to clean up previous tables:', error);
                }}
                
                try {{
                    statusInfo.textContent = 'Processing query...';
                    showLoading('Executing query...');
                    updateProgress(20);
                    
                    console.log('[QUERY] Original query:', query);
                    
                    // Process @URLs in the query
                    const {{ processedQuery, tableInfo }} = await processQueryWithURLs(query, signal);
                    updateProgress(50);
                    
                    if (tableInfo.length > 0) {{
                        const displaySources = tableInfo.map(info => info.displayUrl || info.url).join(', ');
                        const truncatedSources = displaySources.length > 50 
                            ? displaySources.substring(0, 50) + '...' 
                            : displaySources;
                        statusInfo.textContent = `Loading data from: ${{truncatedSources}}`;
                    }}
                    
                    console.log('[QUERY] Processed query:', processedQuery);
                    
                    // Execute with Arrow streaming for better performance
                    const startTime = performance.now();
                    

                    const queryConnection = await duckDB.connect();
                    
                    try {{
                        // Use DuckDB's built-in query method for stability
                        const arrowTable = await queryConnection.query(processedQuery);
                        updateProgress(100);
                        
                        const queryTime = performance.now() - startTime;
                        const rowCount = arrowTable.numRows;
                        console.log(`[QUERY] Query executed in ${{queryTime.toFixed(2)}}ms, returned ${{rowCount}} rows`);
                        
                        // Display results with virtualization
                        displayArrowResults(arrowTable);
                        
                        statusInfo.textContent = `Query: ${{rowCount}} rows (${{queryTime.toFixed(1)}}ms)`;
                        
                    }} finally {{
                        await queryConnection.close();
                        setTimeout(() => updateProgress(0), 1000);
                    }}
                    
                }} catch (error) {{
                    console.error('[QUERY] Query execution error:', error);
                    
                    if (error.name === 'AbortError' || error.message.includes('cancelled')) {{
                        statusInfo.textContent = 'Query cancelled';
                        showEmptyState();
                    }} else {{
                        statusInfo.textContent = `Query error: ${{error.message}}`;
                        showError('Query Error', error.message, error.stack);
                    }}
                }} finally {{
                    currentAbortController = null;
                    updateProgress(0);
                }}
            }}

            // ========================================
            // VIRTUALIZED ARROW TABLE DISPLAY
            // ========================================
            
            function displayArrowResults(arrowTable) {{
                if (!arrowTable || arrowTable.numRows === 0) {{
                    showEmptyResults();
                    return;
                }}
                
                currentArrowTable = arrowTable;
                
                // Extract column information safely
                const columns = arrowTable.schema.fields.map(field => ({{
                    name: field.name,
                    type: field.type.toString(),
                    accessor: (i) => {{
                        try {{
                            const column = arrowTable.getChild(field.name);
                            return column ? column.get(i) : null;
                        }} catch (error) {{
                            console.warn(`Error accessing column ${{field.name}} at row ${{i}}:`, error);
                            return 'Error';
                        }}
                    }}
                }}));
                
                currentColumns = columns;
                
                // Create virtualized table
                createVirtualizedTable(arrowTable, columns);
            }}
            
            function createVirtualizedTable(arrowTable, columns) {{
                const numRows = arrowTable.numRows;
                const ROW_HEIGHT = 32;
                const VISIBLE_ROWS = Math.ceil(window.innerHeight / ROW_HEIGHT);
                const OVERSCAN = 20;
                
                let scrollTop = 0;
                let startIndex = 0;
                let endIndex = Math.min(VISIBLE_ROWS + OVERSCAN, numRows);
                
                const containerHTML = `
                    <div class="table-container" id="tableContainer">
                        <div class="virtual-table" id="virtualTable">
                            <div class="table-header" id="tableHeader">
                                <div class="table-cell row-number">#</div>
                                ${{columns.map(col => `
                                    <div class="table-cell" title="${{col.name}}">
                                        ${{col.name}}
                                        <span class="column-type">${{col.type}}</span>
                                    </div>
                                `).join('')}}
                            </div>
                            <div id="tableBody" style="height: ${{numRows * ROW_HEIGHT}}px; position: relative;">
                                <!-- Rows will be rendered here -->
                            </div>
                        </div>
                    </div>

                `;
                
                resultsContainer.innerHTML = containerHTML;
                
                const tableContainer = document.getElementById('tableContainer');
                const tableBody = document.getElementById('tableBody');
                
                function renderVisibleRows() {{
                    const fragment = document.createDocumentFragment();
                    
                    for (let i = startIndex; i < endIndex; i++) {{
                        const rowDiv = document.createElement('div');
                        rowDiv.className = 'table-row';
                        rowDiv.style.position = 'absolute';
                        rowDiv.style.top = (i * ROW_HEIGHT) + 'px';
                        rowDiv.style.height = ROW_HEIGHT + 'px';
                        
                        // Row number
                        const rowNumDiv = document.createElement('div');
                        rowNumDiv.className = 'table-cell row-number';
                        rowNumDiv.textContent = (i + 1).toString();
                        rowDiv.appendChild(rowNumDiv);
                        
                        // Data cells
                        columns.forEach(col => {{
                            const cellDiv = document.createElement('div');
                            cellDiv.className = 'table-cell';
                            
                            try {{
                                const value = col.accessor(i);
                                if (value === null || value === undefined) {{
                                    cellDiv.textContent = 'NULL';
                                    cellDiv.style.color = '#666';
                                    cellDiv.style.fontStyle = 'italic';
                                }} else if (value === 'Error') {{
                                    cellDiv.textContent = 'Error';
                                    cellDiv.style.color = '#ff6b6b';
                                }} else {{
                                    cellDiv.textContent = String(value);
                                    cellDiv.style.color = '';
                                    cellDiv.style.fontStyle = '';
                                }}
                            }} catch (error) {{
                                cellDiv.textContent = 'Error';
                                cellDiv.style.color = '#ff6b6b';
                                console.warn(`Error rendering cell for column ${{col.name}} at row ${{i}}:`, error);
                            }}
                            
                            rowDiv.appendChild(cellDiv);
                        }});
                        
                        fragment.appendChild(rowDiv);
                    }}
                    
                    tableBody.innerHTML = '';
                    tableBody.appendChild(fragment);
                }}
                
                function handleScroll() {{
                    const newScrollTop = tableContainer.scrollTop;
                    const newStartIndex = Math.floor(newScrollTop / ROW_HEIGHT);
                    const newEndIndex = Math.min(
                        newStartIndex + VISIBLE_ROWS + OVERSCAN * 2,
                        numRows
                    );
                    
                    if (newStartIndex !== startIndex || newEndIndex !== endIndex) {{
                        startIndex = Math.max(0, newStartIndex - OVERSCAN);
                        endIndex = newEndIndex;
                        renderVisibleRows();
                    }}
                }}
                
                tableContainer.addEventListener('scroll', handleScroll);
                renderVisibleRows();
                
                console.log(`[TABLE] Virtualized table created for ${{numRows}} rows`);
            }}
            
            function showEmptyState() {{
                resultsContainer.innerHTML = '';
            }}
            
            function showEmptyResults() {{
                resultsContainer.innerHTML = `
                    <div class="no-results">
                        <h3>No results</h3>
                        <p>The query returned no rows</p>
                    </div>
                `;
            }}
            
            function showLoading(message) {{
                resultsContainer.innerHTML = `
                    <div class="loading">
                        <div class="spinner"></div>
                        ${{message || 'Loading...'}}
                    </div>
                `;
            }}
            
            function showError(title, message, stack) {{
                resultsContainer.innerHTML = `
                    <div class="no-results">
                        <h3 style="color: #ff6b6b;">${{title}}</h3>
                        <p style="color: #ff6b6b;">${{message}}</p>
                        ${{stack ? `<div class="error-details">${{stack}}</div>` : ''}}
                    </div>
                `;
            }}

            // ========================================
            // UI INTERACTIONS (Enhanced)
            // ========================================
            
            function toggleQueryContainer() {{
                const container = document.getElementById('queryContainer');
                const toggle = document.getElementById('queryToggle');
                const statusInfo = document.getElementById('statusInfo');
                const textarea = document.getElementById('queryInput');
                const isMinimized = container.classList.contains('minimized');
                
                if (isMinimized) {{
                    container.classList.remove('minimized');
                    toggle.textContent = '▼';
                    const currentHeight = textarea.offsetHeight;
                    updateStatusInfoPosition(currentHeight);
                }} else {{
                    container.classList.add('minimized');
                    toggle.textContent = '▲';
                    statusInfo.style.bottom = '10px';
                    statusInfo.style.left = '10px';
                }}
            }}
            
            function autoResizeTextarea(textarea) {{
                textarea.style.height = 'auto';
                const lineHeight = parseInt(window.getComputedStyle(textarea).lineHeight);
                const lines = textarea.value.split('\\n').length;
                const newHeight = Math.max(60, Math.min(300, lines * lineHeight + 8));
                textarea.style.height = newHeight + 'px';
                updateStatusInfoPosition(newHeight);
            }}
            
            function updateStatusInfoPosition(queryHeight) {{
                const statusInfo = document.getElementById('statusInfo');
                const container = document.getElementById('queryContainer');
                
                if (container.classList.contains('minimized')) {{
                    statusInfo.style.bottom = '10px';
                    statusInfo.style.left = '10px';
                }} else {{
                    statusInfo.style.bottom = (queryHeight + 10) + 'px';
                    statusInfo.style.left = '10px';
                }}
            }}
            
            function setupQueryInput() {{
                const queryInput = document.getElementById('queryInput');
                
                queryInput.addEventListener('input', function(e) {{
                    autoResizeTextarea(this);
                    
                    clearTimeout(queryTimeout);
                    currentQuery = e.target.value.trim();
                    
                    if (!currentQuery) {{
                        showEmptyState();
                        return;
                    }}
                    
                    // Debounced execution - 800ms after user stops typing
                    queryTimeout = setTimeout(() => {{
                        executeQuery(currentQuery);
                    }}, 100);
                }});
                
                queryInput.addEventListener('keydown', function(e) {{
                    if (e.key === 'Enter') {{
                        setTimeout(() => autoResizeTextarea(this), 0);
                    }}
                    
                    if (e.ctrlKey && e.key === 'Enter') {{
                        e.preventDefault();
                        clearTimeout(queryTimeout);
                        executeQuery(currentQuery);
                    }}
                    
                    // Cancel query with Escape
                    if (e.key === 'Escape' && currentAbortController) {{
                        currentAbortController.abort();
                        statusInfo.textContent = 'Query cancelled';
                    }}
                }});
                
                setTimeout(() => {{
                    autoResizeTextarea(queryInput);
                }}, 100);
                
                // Execute initial query after UI is ready
                setTimeout(() => {{
                    executeQuery(currentQuery);
                }}, 1000);
            }}

            // ========================================
            // INITIALIZATION
            // ========================================
            
            window.toggleQueryContainer = toggleQueryContainer;
            
            // Handle window resize for virtualization
            window.addEventListener('resize', () => {{
                if (currentArrowTable && currentColumns.length > 0) {{
                    displayArrowResults(currentArrowTable);
                }}
            }});
            
            initDuckDB();
        </script>
    </body>
    </html>
    """
    
    return html_content 
