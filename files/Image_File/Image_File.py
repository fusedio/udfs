import base64
import html
import os

import fused

# Past this the base64 of the file is a bigger page than is worth returning, so
# the picture is left to the browser to fetch from a signed URL instead.
MAX_INLINE_BYTES = 12_000_000

MIMES = {
    "png": "image/png", "jpg": "image/jpeg", "jpeg": "image/jpeg",
    "gif": "image/gif", "webp": "image/webp", "avif": "image/avif",
    "bmp": "image/bmp", "ico": "image/x-icon", "svg": "image/svg+xml",
    "apng": "image/apng", "jfif": "image/jpeg", "tif": "image/tiff",
    "tiff": "image/tiff",
}


@fused.udf(cache_max_age="30m")
def udf(path: str, preview: bool = False):
    """Show an image the way a browser does — every format it decodes natively.

    ImageIO_File decodes to an array for map work; this one is for looking at
    the picture, so it covers SVG, ICO and AVIF too, which imageio cannot read.

    The bytes are read here and inlined as a data URI rather than fetched by
    the browser from a signed URL. A requester-pays bucket signs a URL that is
    only honoured alongside an `x-amz-request-payer` header, which the browser
    cannot add, so those images came back 403 and the tab stayed empty.
    Reading server-side also means `/mount/` and `gdrive://` paths, which
    cannot be signed at all, display like any other.
    """
    ext = os.path.splitext(path)[1].lstrip(".").lower()
    mime = MIMES.get(ext, "application/octet-stream")
    name = html.escape(path.rsplit("/", 1)[-1], quote=True)

    try:
        raw = fused.api.get(path)
    except Exception as e:  # noqa: BLE001 - surfaced to the viewer, not swallowed
        return _error(path, f"{type(e).__name__}: {e}")

    if len(raw) > MAX_INLINE_BYTES:
        # Too large to inline. A signed URL still works for any bucket that is
        # not requester-pays, which is the only case that got this far anyway.
        try:
            src = html.escape(fused.api.sign_url(path), quote=True)
        except Exception as e:  # noqa: BLE001 - no link left to offer
            return _error(path, f"{len(raw):,} bytes is too large to inline, and "
                                f"the path cannot be signed: {type(e).__name__}: {e}")
    else:
        # An <img> src, not inline markup: an <img> cannot run an SVG's scripts.
        src = f"data:{mime};base64,{base64.b64encode(raw).decode('ascii')}"

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
  <img src="{src}" alt="{name}" onerror="document.body.innerHTML = '<p class=&quot;err&quot;>This browser could not decode {name}.</p>'">
</body>
</html>"""


def _error(path, message):
    """The read failed, so there is nothing to show — say which path and why."""
    safe = html.escape(path, quote=True)
    return f"""<!DOCTYPE html>
<html>
<body style="margin:0; padding:24px; background:#1a1a1a; color:#cccccc;
             font-family: system-ui, -apple-system, sans-serif; line-height:1.6;">
  <h2 style="color:#ff6b6b;">Could not read this image</h2>
  <p><code>{html.escape(message, quote=True)}</code></p>
  <p><strong>Path:</strong> <code>{safe}</code></p>
</body>
</html>"""
