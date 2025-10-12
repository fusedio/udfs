import fused, json

@fused.udf
def udf(channel: str = "bounds-demo"):
    common = fused.load("https://github.com/fusedio/udfs/tree/351515e/public/common/")
    html = f"""<!doctype html>
    <meta charset="utf-8">
    <pre id="out" style="padding:1em;font-family:ui-monospace,monospace;">[waiting]</pre>
    
    <script src="https://cdn.jsdelivr.net/gh/milind-soni/fused-channel@48b6d9a/channel.js"></script>
    
    <script>
    document.addEventListener('DOMContentLoaded', () => {{
      enableMsgListener('bounds-demo');
    }});
    </script>
    """
    return common.html_to_obj(html)
