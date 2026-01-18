@fused.udf(cache_max_age='30m')
def udf(path: str= ''):
    import pandas as pd
    import urllib.parse
    
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