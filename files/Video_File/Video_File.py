import html
import mimetypes
import fused
from urllib.parse import urlsplit


@fused.udf(cache_max_age="30m")
def udf(path: str):
    if path.startswith("/mount/") or path.startswith("gdrive://"):
        safe_path = html.escape(path, quote=True)
        return f"""
    <html>
    <body style="margin:0; padding:24px; font-family: system-ui, sans-serif; background:#1a1a1a; color:#ccc;">
        <h2 style="color:#ff6b6b;">Video preview not available</h2>
        <p>Signed URLs are not supported for <code>/mount/</code> or <code>gdrive://</code> paths, so this viewer cannot load the video from the browser.</p>
        <p><strong>Path:</strong> <code>{safe_path}</code></p>
    </body>
    </html>
    """
    signed_url = fused.api.sign_url(path)
    source_type = mimetypes.guess_type(urlsplit(signed_url).path)[0] or "video/mp4"
    return f"""
    <html>
    <body style="margin:0; display:flex; justify-content:center; align-items:center; height:100vh;">
        <video controls autoplay muted style="max-width:100%; max-height:100%;">
            <source src="{signed_url}" type="{source_type}">
            Your browser does not support the video tag.
        </video>
    </body>
    </html>
    """
