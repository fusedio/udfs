@fused.udf(cache_max_age='30m')
def udf(path: str= ''):
    import html
    import urllib.parse

    if path and (path.startswith('/mount/') or path.startswith('gdrive://')):
        safe_path = html.escape(path, quote=True)
        return f"""
    <html style="background-color: #1a1a1a">
    <body style="margin:0; padding:24px; font-family: system-ui, sans-serif; color:#ccc;">
        <h2 style="color:#ff6b6b;">Parquet preview not available</h2>
        <p>Signed URLs are not supported for <code>/mount/</code> or <code>gdrive://</code> paths, so the parquet viewer cannot load this file.</p>
        <p><strong>Path:</strong> <code>{safe_path}</code></p>
    </body>
    </html>
    """
    
    base_url = fused.options.base_web_url
    signed_url = fused.api.sign_url(path) if path else ''
    encoded_url = urllib.parse.quote(signed_url, safe='')
    url = f'{base_url}/parquet-viewer?key={encoded_url}'
    
    return f"""
    <html style="background-color: white">
    <head>
    <script>
    setTimeout(()=>{{window.location.reload()}}, 40*60*1000)
    </script>
    </head>
    <body style="margin:0">
    <iframe src="{url}" style="height: 100%; width: 100%" />
    </body>
    </html>
    """