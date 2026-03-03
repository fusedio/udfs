import mimetypes
import fused
from urllib.parse import urlsplit


@fused.udf(cache_max_age="30m")
def udf(path: str):
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
