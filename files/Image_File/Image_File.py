import html

import fused


@fused.udf(cache_max_age="30m")
def udf(path: str, preview: bool = False):
    """Show an image the way a browser does — every format it decodes natively.

    ImageIO_File decodes to an array for map work; this one is for looking at
    the picture, so it covers SVG, ICO and AVIF too, which imageio cannot read.
    """
    if path.startswith("/mount/") or path.startswith("gdrive://"):
        return _no_signed_url(path, "Image")

    url = html.escape(fused.api.sign_url(path), quote=True)
    name = html.escape(path.rsplit("/", 1)[-1], quote=True)
    # SVG in an <img> is inert, but it is still untrusted markup; keep the
    # whole document out of the way of scripts by declining to inline it.
    return f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>{name}</title>
<style>
  html, body {{ margin: 0; height: 100%; background: #1a1a1a; color: #cccccc;
                font-family: system-ui, -apple-system, "Segoe UI", sans-serif; }}
  body {{ display: flex; align-items: center; justify-content: center; padding: 16px;
          box-sizing: border-box; }}
  img {{ max-width: 100%; max-height: 100%; display: block;
         /* Checkerboard so transparent PNGs read as transparent, not black. */
         background-image:
           linear-gradient(45deg, #262626 25%, transparent 25%, transparent 75%, #262626 75%),
           linear-gradient(45deg, #262626 25%, transparent 25%, transparent 75%, #262626 75%);
         background-size: 16px 16px;
         background-position: 0 0, 8px 8px; }}
  .err {{ color: #ff6b6b; line-height: 1.6; }}
  a {{ color: #D1E550; }}
</style>
</head>
<body>
  <img src="{url}" alt="{name}" onerror="document.body.innerHTML = '<p class=&quot;err&quot;>This browser could not decode {name}.</p>'">
</body>
</html>"""


def _no_signed_url(path, label):
    """Signed URLs are the only way the browser can reach the bytes."""
    safe = html.escape(path, quote=True)
    return f"""<!DOCTYPE html>
<html>
<body style="margin:0; padding:24px; background:#1a1a1a; color:#cccccc;
             font-family: system-ui, -apple-system, sans-serif; line-height:1.6;">
  <h2 style="color:#ff6b6b;">{label} preview not available</h2>
  <p>Signed URLs are not supported for <code>/mount/</code> or <code>gdrive://</code>
     paths, so this viewer cannot load the file from the browser.</p>
  <p><strong>Path:</strong> <code>{safe}</code></p>
</body>
</html>"""
